"""Create, verify and restore a self-contained SQLite backup without replacing live data."""
from __future__ import annotations

import argparse
import array
import ctypes
import fcntl
import hashlib
import io
import json
import os
import shutil
import sqlite3
import stat
import subprocess
import sys
import tarfile
import tempfile
import threading
import time
from pathlib import Path


class BackupError(RuntimeError):
    pass


def _open_regular(path: Path):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    if not stat.S_ISREG(os.fstat(fd).st_mode):
        os.close(fd)
        raise BackupError(f"not a regular file: {path}")
    return os.fdopen(fd, "rb")


def _publish_directory(source: Path, destination: Path) -> None:
    libc = ctypes.CDLL(None, use_errno=True)
    rename = getattr(libc, "renameat2", None)
    if rename is None:
        raise BackupError("atomic no-replace directory publication requires renameat2")
    rename.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
    rename.restype = ctypes.c_int
    if rename(-100, os.fsencode(source), -100, os.fsencode(destination), 1):
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error), str(destination))
    _sync_directory(destination.parent)


def _sync_directory(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _guard(deadline: float, path: Path, reserve: int) -> None:
    if time.monotonic() >= deadline:
        raise BackupError("backup operation exceeded its deadline")
    if shutil.disk_usage(path).free < reserve:
        raise BackupError("backup operation reached its free-space reserve")


def _hash(path: Path, deadline: float, reserve: int = 0) -> str:
    digest = hashlib.sha256()
    with _open_regular(path) as handle:
        while chunk := handle.read(4 * 1024 * 1024):
            _guard(deadline, path.parent, reserve)
            digest.update(chunk)
    return digest.hexdigest()


def _inspect(path: Path, deadline: float, reserve: int = 0) -> dict:
    connection = sqlite3.connect(path.as_uri() + "?mode=ro&immutable=1", uri=True)
    try:
        def progress():
            try:
                _guard(deadline, path.parent, reserve)
            except BackupError:
                return 1
            return 0
        connection.set_progress_handler(progress, 1000)
        result = connection.execute("PRAGMA quick_check").fetchall()
        if result != [("ok",)]:
            raise BackupError(f"SQLite quick_check failed: {result[:5]}")
        metadata = {}
        if connection.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='metadata'").fetchone():
            metadata = dict(connection.execute(
                "SELECT key,value FROM metadata WHERE key IN ('epoch','cursor:live','cursor:history')"
            ))
        return {
            "schema_version": connection.execute("PRAGMA user_version").fetchone()[0],
            "page_count": connection.execute("PRAGMA page_count").fetchone()[0],
            "page_size": connection.execute("PRAGMA page_size").fetchone()[0],
            "metadata": metadata,
        }
    finally:
        connection.close()


def _absent(path: Path) -> None:
    for candidate in (path, *(Path(str(path) + suffix) for suffix in ("-wal", "-shm", "-journal"))):
        if os.path.lexists(candidate):
            raise BackupError(f"refusing to replace existing path: {candidate}")


def _load_manifest(backup: Path, *, archive: bool = False) -> dict:
    database = backup / ("database.tar.zst" if archive else "database.sqlite")
    manifest_path = backup / "manifest.json"
    if database.is_symlink() or manifest_path.is_symlink():
        raise BackupError("backup members must not be symbolic links")
    with _open_regular(manifest_path) as handle:
        raw = handle.read(1024 * 1024 + 1)
    if len(raw) > 1024 * 1024:
        raise BackupError("backup manifest exceeds its size limit")
    manifest = json.loads(raw)
    if not isinstance(manifest, dict) or type(manifest.get("format_version")) is not int or manifest["format_version"] != 1:
        raise BackupError("unsupported backup manifest")
    for name in ("bytes", "schema_version", "page_count", "page_size"):
        if type(manifest.get(name)) is not int or manifest[name] < 0:
            raise BackupError(f"invalid manifest field: {name}")
    digest = manifest.get("sha256")
    if not isinstance(digest, str) or len(digest) != 64 or any(ch not in "0123456789abcdef" for ch in digest):
        raise BackupError("invalid manifest checksum")
    if manifest.get("verification") != "quick_check" or not isinstance(manifest.get("metadata"), dict):
        raise BackupError("invalid manifest verification or metadata")
    for suffix in ("-wal", "-shm", "-journal"):
        if os.path.lexists(str(database) + suffix):
            raise BackupError("backup is not self-contained")
    if not archive and database.stat().st_size != manifest["bytes"]:
        raise BackupError("backup byte count does not match its manifest")
    return manifest


def verify(backup: Path, *, timeout: float = 600) -> dict:
    if timeout <= 0:
        raise ValueError("timeout must be positive")
    backup = backup.resolve(strict=True)
    deadline = time.monotonic() + timeout
    manifest = _load_manifest(backup)
    database = backup / "database.sqlite"
    if _hash(database, deadline) != manifest["sha256"]:
        raise BackupError("backup checksum does not match its manifest")
    state = _inspect(database, deadline)
    if any(manifest.get(key) != value for key, value in state.items()):
        raise BackupError("backup state does not match its manifest")
    return {"verified": True, "backup": str(backup), "manifest": manifest}


def create(database: Path, destination: Path, *, timeout: float = 600, reserve_bytes: int = 160 * 1024**3, snapshot: bool = False) -> dict:
    if timeout <= 0 or reserve_bytes < 0:
        raise ValueError("timeout must be positive and reserve must be nonnegative")
    database = database.resolve(strict=True)
    destination = destination.absolute()
    _absent(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination = destination.parent.resolve() / destination.name
    required = reserve_bytes + (0 if snapshot else database.stat().st_size)
    if shutil.disk_usage(destination.parent).free < required:
        raise BackupError(f"backup needs at least {required} free bytes including its reserve")
    deadline = time.monotonic() + timeout
    with tempfile.TemporaryDirectory(prefix=".rhpools-backup-", dir=destination.parent) as temporary:
        staging = Path(temporary)
        output = staging / "database.sqlite"
        if snapshot:
            for suffix in ("-wal", "-shm", "-journal"):
                if os.path.lexists(str(database) + suffix):
                    raise BackupError("offline snapshot must have no journal sidecars")
            with _open_regular(database) as source, output.open("xb") as target:
                before = os.fstat(source.fileno())
                flags = array.array("I", [0])
                fcntl.ioctl(source.fileno(), 0x80086601, flags, True)
                source_nocow = flags[0] & 0x00800000
                fcntl.ioctl(target.fileno(), 0x80086601, flags, True)
                flags[0] = (flags[0] & ~0x00800000) | source_nocow
                fcntl.ioctl(target.fileno(), 0x40086602, flags, True)
                fcntl.ioctl(target.fileno(), 0x40049409, source.fileno())
                after = os.fstat(source.fileno())
                if (before.st_size, before.st_mtime_ns, before.st_ctime_ns) != (
                    after.st_size, after.st_mtime_ns, after.st_ctime_ns
                ):
                    raise BackupError("offline source changed during snapshot")
                os.fchmod(target.fileno(), 0o600)
                os.fsync(target.fileno())
        else:
            _guard(deadline, database.parent, reserve_bytes)
            source = sqlite3.connect(database.as_uri() + "?mode=ro", uri=True, timeout=1)
            target = sqlite3.connect(output)
            output.chmod(0o600)
            try:
                source.execute("PRAGMA query_only=ON")
                source.execute("BEGIN")
                source.execute("SELECT rootpage FROM sqlite_master LIMIT 1").fetchone()
                wal = Path(str(database) + "-wal")
                initial_wal = wal.stat().st_size if wal.exists() else 0

                def progress(*_):
                    _guard(deadline, staging, reserve_bytes)
                    _guard(deadline, database.parent, reserve_bytes)
                    if wal.exists() and wal.stat().st_size - initial_wal > 512 * 1024**2:
                        raise BackupError("source WAL grew by more than 512 MiB; use a quiescent snapshot")
                source.backup(target, pages=4096, sleep=0.05, progress=progress)
                if target.execute("PRAGMA journal_mode=DELETE").fetchone()[0].lower() != "delete":
                    raise BackupError("could not make backup independent of a WAL")
            finally:
                target.close()
                source.close()
        _guard(deadline, staging, reserve_bytes)
        manifest = {
            "format_version": 1,
            "created_at": time.time(),
            "verification": "quick_check",
            "bytes": output.stat().st_size,
            "sha256": _hash(output, deadline, reserve_bytes),
            **_inspect(output, deadline, reserve_bytes),
        }
        with output.open("rb") as handle:
            os.fsync(handle.fileno())
        manifest_path = staging / "manifest.json"
        with manifest_path.open("x") as handle:
            json.dump(manifest, handle, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        _sync_directory(staging)
        _guard(deadline, staging, reserve_bytes)
        _publish_directory(staging, destination)
        return {"created": True, "backup": str(destination), "manifest": manifest}


def archive(backup: Path, destination: Path, *, timeout: float = 600, reserve_bytes: int = 160 * 1024**3) -> dict:
    if timeout <= 0 or reserve_bytes < 0:
        raise ValueError("timeout must be positive and reserve must be nonnegative")
    deadline = time.monotonic() + timeout
    backup = backup.resolve(strict=True)
    manifest = _load_manifest(backup)
    destination = destination.absolute()
    _absent(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination = destination.parent.resolve() / destination.name
    _guard(deadline, destination.parent, reserve_bytes)
    with tempfile.TemporaryDirectory(prefix=".rhpools-archive-", dir=destination.parent) as temporary:
        staging = Path(temporary)
        compressed = staging / "database.tar.zst"
        checksum = hashlib.sha256()
        with _open_regular(backup / "database.sqlite") as source, compressed.open("xb") as target:
            os.fchmod(target.fileno(), 0o600)
            before = os.fstat(source.fileno())

            class CheckedReader:
                def read(self, size):
                    _guard(deadline, staging, reserve_bytes)
                    chunk = source.read(size)
                    checksum.update(chunk)
                    return chunk

            process = subprocess.Popen(
                ["zstd", "-q", "-T2", "-3", "-c"], stdin=subprocess.PIPE,
                stdout=target, stderr=subprocess.PIPE,
            )
            watchdog = threading.Timer(max(0.01, deadline - time.monotonic()), process.kill)
            watchdog.daemon = True
            watchdog.start()
            try:
                with tarfile.open(fileobj=process.stdin, mode="w|", bufsize=1024**2, copybufsize=1024**2) as packed:
                    member = tarfile.TarInfo("database.sqlite")
                    member.size = manifest["bytes"]
                    member.mode = 0o600
                    packed.addfile(member, CheckedReader())
                    after = os.fstat(source.fileno())
                    if checksum.hexdigest() != manifest["sha256"] or (
                        before.st_size, before.st_mtime_ns, before.st_ctime_ns
                    ) != (after.st_size, after.st_mtime_ns, after.st_ctime_ns):
                        raise BackupError("backup changed or failed its checksum during archival")
                    encoded = (json.dumps(manifest, sort_keys=True) + "\n").encode()
                    member = tarfile.TarInfo("manifest.json")
                    member.size = len(encoded)
                    member.mode = 0o600
                    packed.addfile(member, io.BytesIO(encoded))
                process.stdin.close()
                if process.wait(timeout=max(0.01, deadline - time.monotonic())):
                    raise BackupError("backup compression failed: " + process.stderr.read(4096).decode("utf-8", "replace"))
            finally:
                watchdog.cancel()
                if process.poll() is None:
                    process.kill()
                process.wait()
                watchdog.join()
                process.stdin.close()
                process.stderr.close()
            target.flush()
            os.fsync(target.fileno())
        proof = {
            "format_version": 1, "archive": compressed.name, "bytes": compressed.stat().st_size,
            "sha256": _hash(compressed, deadline, reserve_bytes),
            "database_sha256": manifest["sha256"], "captured_bytes": manifest["bytes"],
            "verification": manifest["verification"],
        }
        for name, contents in (("manifest.json", manifest), ("archive.json", proof)):
            with (staging / name).open("x") as output:
                json.dump(contents, output, sort_keys=True)
                output.write("\n")
                output.flush()
                os.fsync(output.fileno())
        _sync_directory(staging)
        _guard(deadline, staging, reserve_bytes)
        _publish_directory(staging, destination)
    return {"archived": True, "backup": str(destination), **proof}


def _copy_bytes(source, target, size: int, deadline: float, directory: Path, reserve: int) -> None:
    remaining = size
    while remaining:
        _guard(deadline, directory, reserve)
        chunk = source.read(min(4 * 1024**2, remaining))
        if not chunk:
            raise BackupError("backup payload ended before its declared size")
        target.write(chunk)
        remaining -= len(chunk)


def _archive_restore(backup: Path, target, manifest: dict, deadline: float, directory: Path, reserve: int) -> None:
    with _open_regular(backup / "database.tar.zst") as compressed:
        process = subprocess.Popen(
            ["zstd", "-q", "-d", "-c"], stdin=compressed,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
        watchdog = threading.Timer(max(0.01, deadline - time.monotonic()), process.kill)
        watchdog.daemon = True
        watchdog.start()
        stream = process.stdout
        seen = set()
        extended = None

        def take(size):
            _guard(deadline, directory, reserve)
            data = stream.read(size)
            if len(data) != size:
                raise BackupError("truncated backup archive")
            return data

        try:
            while True:
                header = take(512)
                if header == bytes(512):
                    if take(512) != bytes(512) or seen != {"database.sqlite", "manifest.json"} or extended is not None:
                        raise BackupError("incomplete backup archive")
                    trailer = stream.read(10241)
                    if len(trailer) > 10240 or any(trailer):
                        raise BackupError("unexpected data after the backup archive")
                    break
                try:
                    member = tarfile.TarInfo.frombuf(header, "utf-8", "strict")
                except tarfile.HeaderError as error:
                    raise BackupError("invalid backup archive header") from error
                if member.type == tarfile.XHDTYPE:
                    if extended is not None or not 0 < member.size <= 4096:
                        raise BackupError("unsupported archive extension")
                    data = take((member.size + 511) // 512 * 512)[:member.size]
                    extended = {}
                    offset = 0
                    while offset < len(data):
                        separator = data.find(b" ", offset)
                        length = data[offset:separator]
                        if separator < offset or not length.isdigit():
                            raise BackupError("invalid archive extension length")
                        end = offset + int(length)
                        if end > len(data) or end <= separator + 1 or data[end - 1:end] != b"\n":
                            raise BackupError("invalid archive extension record")
                        key, equals, value = data[separator + 1:end - 1].partition(b"=")
                        if not equals or key not in (b"size", b"mtime", b"atime", b"ctime") or key in extended:
                            raise BackupError("unsupported archive extension field")
                        extended[key] = value
                        offset = end
                    continue
                if member.type not in (tarfile.REGTYPE, tarfile.AREGTYPE) or member.name not in ("database.sqlite", "manifest.json") or member.name in seen:
                    raise BackupError("unexpected backup archive member")
                size = member.size
                if extended is not None:
                    if b"size" in extended:
                        if not extended[b"size"].isdigit():
                            raise BackupError("invalid archive member size")
                        size = int(extended[b"size"])
                    extended = None
                if member.name == "database.sqlite":
                    if size != manifest["bytes"]:
                        raise BackupError("archived database size does not match its manifest")
                    _copy_bytes(stream, target, size, deadline, directory, reserve)
                else:
                    if not 0 < size <= 1024**2 or json.loads(take(size)) != manifest:
                        raise BackupError("archived manifest does not match its external copy")
                if any(take((-size) % 512)):
                    raise BackupError("invalid backup archive padding")
                seen.add(member.name)
            if process.wait(timeout=max(0.01, deadline - time.monotonic())):
                raise BackupError("backup archive decompression failed: " + process.stderr.read(4096).decode("utf-8", "replace"))
        finally:
            watchdog.cancel()
            if process.poll() is None:
                process.kill()
            process.wait()
            watchdog.join()
            process.stdout.close()
            process.stderr.close()


def restore(backup: Path, destination: Path, *, timeout: float = 600, reserve_bytes: int = 160 * 1024**3, archive: bool = False) -> dict:
    if timeout <= 0 or reserve_bytes < 0:
        raise ValueError("timeout must be positive and reserve must be nonnegative")
    deadline = time.monotonic() + timeout
    backup = backup.resolve(strict=True)
    manifest = _load_manifest(backup, archive=archive)
    destination = destination.absolute()
    _absent(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination = destination.parent.resolve() / destination.name
    if shutil.disk_usage(destination.parent).free < manifest["bytes"] + reserve_bytes:
        raise BackupError("restore would violate its free-space reserve")
    with tempfile.TemporaryDirectory(prefix=".rhpools-restore-", dir=destination.parent) as temporary:
        output = Path(temporary) / "database.sqlite"
        with output.open("xb") as target:
            if archive:
                _archive_restore(backup, target, manifest, deadline, destination.parent, reserve_bytes)
            else:
                with _open_regular(backup / "database.sqlite") as source:
                    _copy_bytes(source, target, manifest["bytes"], deadline, destination.parent, reserve_bytes)
                    if source.read(1):
                        raise BackupError("backup payload exceeds its declared size")
            target.flush()
            os.fchmod(target.fileno(), 0o600)
            os.fsync(target.fileno())
        if _hash(output, deadline, reserve_bytes) != manifest["sha256"]:
            raise BackupError("restored file checksum mismatch")
        state = _inspect(output, deadline, reserve_bytes)
        if any(manifest.get(key) != value for key, value in state.items()):
            raise BackupError("restored state mismatch")
        _absent(destination)
        _guard(deadline, destination.parent, reserve_bytes)
        os.link(output, destination)
        _sync_directory(destination.parent)
    return {"restored": True, "database": str(destination), "manifest": manifest}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    create_parser = commands.add_parser("create")
    create_parser.add_argument("--database", required=True, type=Path)
    create_parser.add_argument("--destination", required=True, type=Path)
    create_parser.add_argument("--snapshot", action="store_true", help="reflink an offline, journal-free source on a supporting filesystem")
    verify_parser = commands.add_parser("verify")
    verify_parser.add_argument("--backup", required=True, type=Path)
    restore_parser = commands.add_parser("restore")
    restore_parser.add_argument("--backup", required=True, type=Path)
    restore_parser.add_argument("--destination", required=True, type=Path)
    restore_parser.add_argument("--archive", action="store_true", help="restore database.tar.zst directly without a second uncompressed copy")
    archive_parser = commands.add_parser("archive")
    archive_parser.add_argument("--backup", required=True, type=Path)
    archive_parser.add_argument("--destination", required=True, type=Path)
    for command in (create_parser, verify_parser, restore_parser, archive_parser):
        command.add_argument("--timeout", type=float, default=600)
    for command in (create_parser, restore_parser, archive_parser):
        command.add_argument("--reserve-bytes", type=int, default=160 * 1024**3)
    args = vars(parser.parse_args())
    command = args.pop("command")
    try:
        result = {"create": create, "verify": verify, "restore": restore, "archive": archive}[command](**args)
    except (OSError, ValueError, sqlite3.Error, BackupError, subprocess.TimeoutExpired) as exc:
        print(json.dumps({"ok": False, "error": str(exc)}))
        return 1
    print(json.dumps({"ok": True, **result}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
