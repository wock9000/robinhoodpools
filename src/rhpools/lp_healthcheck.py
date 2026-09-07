"""Bounded one-shot availability checks and conservative local recovery."""

from __future__ import annotations

import argparse
import contextlib
import fcntl
import json
import math
import os
from pathlib import Path
import re
import signal
import socket
import ssl
import subprocess
import tempfile
import time
from typing import Any, Iterator
from urllib import error as urlerror
from urllib import parse as urlparse
from urllib import request as urlrequest

CHAIN_ID = 4663
STATUS_PATH = "/api/lp/status"
TERMINAL_MARKER = b"<title>Robinhood Pools / Chain 4663</title>"
DEFAULT_ORIGIN_URL = "http://127.0.0.1:8196"
DEFAULT_PUBLIC_URL = "https://rhpools.lol"
DEFAULT_STATE_PATH = Path.home() / ".local/state/rhpools/lp-healthcheck.json"
DEFAULT_APP_UNIT = "robinhoodpools.service"
DEFAULT_TUNNEL_UNIT = "robinhoodpools-tunnel.service"
MAX_HTML_BYTES = 512 * 1024
MAX_JSON_BYTES = 128 * 1024
FAILURE_THRESHOLD = 3
STARTUP_GRACE_SECONDS = 120
RECOVERY_COOLDOWN_SECONDS = 600
RECOVERY_WINDOW_SECONDS = 3600
MAX_RECOVERIES_PER_WINDOW = 3
STATE_VERSION = 1
_UNIT_RE = re.compile(r"[A-Za-z0-9_.@:-]+\.service\Z")


class _NoRedirect(urlrequest.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: ANN001
        return None


class _ProbeDeadline(TimeoutError):
    pass


_OPENER = urlrequest.build_opener(_NoRedirect)


def _compact(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True)


def _emit(value: dict[str, Any]) -> None:
    print(_compact(value), flush=True)


def _normalize_base_url(value: str) -> str:
    try:
        parsed = urlparse.urlsplit(value)
        _ = parsed.port
    except ValueError as exc:
        raise ValueError("invalid_url") from exc
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise ValueError("url_must_be_http_or_https")
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("url_userinfo_not_allowed")
    if parsed.query or parsed.fragment:
        raise ValueError("url_query_or_fragment_not_allowed")
    path = parsed.path.rstrip("/")
    return urlparse.urlunsplit((parsed.scheme, parsed.netloc, path, "", ""))


def _validate_unit(value: str) -> str:
    if not _UNIT_RE.fullmatch(value) or value.startswith("-"):
        raise ValueError("invalid_service_unit")
    return value


def _validate_state_path(value: str) -> Path:
    path = Path(value).expanduser()
    if not path.is_absolute():
        raise ValueError("state_path_must_be_absolute")
    resolved = path.resolve(strict=False)
    checkout = Path(__file__).resolve().parents[2]
    if resolved == checkout or resolved.is_relative_to(checkout):
        raise ValueError("state_path_must_be_outside_checkout")
    return resolved


@contextlib.contextmanager
def _request_deadline(seconds: float) -> Iterator[None]:
    """Apply an end-to-end wall deadline, including DNS and slow response reads."""
    previous_handler = signal.getsignal(signal.SIGALRM)

    def expired(_signum: int, _frame: Any) -> None:
        raise _ProbeDeadline()

    signal.signal(signal.SIGALRM, expired)
    previous_timer = signal.setitimer(signal.ITIMER_REAL, seconds)
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous_handler)
        if previous_timer != (0.0, 0.0):
            signal.setitimer(signal.ITIMER_REAL, *previous_timer)


def _error_code(exc: BaseException) -> str:
    reason = exc.reason if isinstance(exc, urlerror.URLError) else exc
    if isinstance(reason, (TimeoutError, socket.timeout)):
        return "timeout"
    if isinstance(reason, socket.gaierror):
        return "dns"
    if isinstance(reason, ssl.SSLError):
        return "tls"
    if isinstance(reason, (ConnectionError, OSError)):
        return "connection"
    return "request_failed"


def _fetch(base_url: str, path: str, max_bytes: int, timeout: float) -> tuple[dict[str, Any], bytes | None]:
    started = time.monotonic()
    result: dict[str, Any] = {"path": path, "ok": False, "status": None}
    body: bytes | None = None
    request = urlrequest.Request(
        f"{base_url}{path}",
        headers={
            "Accept": "application/json" if path == STATUS_PATH else "text/html",
            "Accept-Encoding": "identity",
            "User-Agent": "rhpools-healthcheck/1",
        },
        method="GET",
    )
    try:
        with _request_deadline(timeout):
            with _OPENER.open(request, timeout=timeout) as response:
                result["status"] = int(response.getcode())
                if result["status"] != 200:
                    result["error"] = "http_status"
                else:
                    content_length = response.headers.get("Content-Length")
                    if content_length is not None:
                        try:
                            length = int(content_length)
                            if length < 0:
                                result["error"] = "invalid_content_length"
                            elif length > max_bytes:
                                result["error"] = "body_too_large"
                        except ValueError:
                            result["error"] = "invalid_content_length"
                    if "error" not in result:
                        body = response.read(max_bytes + 1)
                        if len(body) > max_bytes:
                            body = None
                            result["error"] = "body_too_large"
                        else:
                            result["ok"] = True
    except urlerror.HTTPError as exc:
        result["status"] = int(exc.code)
        result["error"] = "http_status"
    except BaseException as exc:
        if isinstance(exc, (KeyboardInterrupt, SystemExit)):
            raise
        result["error"] = _error_code(exc)
    result["latency_ms"] = max(0, int((time.monotonic() - started) * 1000))
    return result, body


def _valid_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def _validate_status(body: bytes) -> tuple[dict[str, Any] | None, str | None]:
    try:
        payload = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None, "invalid_json"
    if not isinstance(payload, dict):
        return None, "invalid_schema"
    if payload.get("chain_id") != CHAIN_ID or isinstance(payload.get("chain_id"), bool):
        return None, "wrong_chain"
    if not all(_valid_int(payload.get(key)) for key in ("head", "indexed_head", "lag_blocks")):
        return None, "invalid_schema"
    state = payload.get("state")
    if (
        not isinstance(state, str)
        or not re.fullmatch(r"[a-z][a-z0-9_-]{0,63}", state)
    ):
        return None, "invalid_schema"
    return payload, None


def _probe_target(base_url: str, timeout: float) -> tuple[dict[str, Any], dict[str, Any] | None]:
    root, root_body = _fetch(base_url, "/", MAX_HTML_BYTES, timeout)
    if root["ok"] and (root_body is None or TERMINAL_MARKER not in root_body):
        root["ok"] = False
        root["error"] = "terminal_marker_missing"

    status_check, status_body = _fetch(base_url, STATUS_PATH, MAX_JSON_BYTES, timeout)
    payload: dict[str, Any] | None = None
    if status_check["ok"]:
        payload, schema_error = _validate_status(status_body or b"")
        if schema_error is not None:
            status_check["ok"] = False
            status_check["error"] = schema_error

    checks = [root, status_check]
    return {"ok": all(check["ok"] for check in checks), "checks": checks}, payload


def _empty_state() -> dict[str, Any]:
    return {
        "version": STATE_VERSION,
        "origin_failures": 0,
        "public_failures": 0,
        "app_attempts": [],
        "tunnel_attempts": [],
    }


def _load_state(path: Path) -> tuple[dict[str, Any], bool]:
    if not path.exists():
        return _empty_state(), True
    try:
        with path.open("rb") as handle:
            raw = handle.read(MAX_JSON_BYTES + 1)
        if len(raw) > MAX_JSON_BYTES:
            return _empty_state(), False
        state = json.loads(raw)
    except (OSError, json.JSONDecodeError, UnicodeDecodeError):
        return _empty_state(), False
    if not isinstance(state, dict) or state.get("version") != STATE_VERSION:
        return _empty_state(), False
    for key in ("origin_failures", "public_failures"):
        if not _valid_int(state.get(key)):
            return _empty_state(), False
    for key in ("app_attempts", "tunnel_attempts"):
        attempts = state.get(key)
        if not isinstance(attempts, list) or any(
            not isinstance(item, (int, float))
            or isinstance(item, bool)
            or not math.isfinite(item)
            for item in attempts
        ):
            return _empty_state(), False
    return state, True


def _save_state(path: Path, state: dict[str, Any]) -> bool:
    temporary: str | None = None
    try:
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(_compact(state))
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        temporary = None
        directory_fd = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
        return True
    except OSError:
        return False
    finally:
        if temporary is not None:
            try:
                os.unlink(temporary)
            except OSError:
                pass


@contextlib.contextmanager
def _invocation_lock(state_path: Path) -> Iterator[bool]:
    fd: int | None = None
    try:
        state_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        fd = os.open(
            f"{state_path}.lock",
            os.O_CREAT | os.O_RDWR | getattr(os, "O_CLOEXEC", 0),
            0o600,
        )
        os.fchmod(fd, 0o600)
    except OSError:
        if fd is not None:
            os.close(fd)
        yield False
        return
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            yield False
            return
        yield True
    finally:
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        except OSError:
            pass
        os.close(fd)


def _status_diagnostic(
    payload: dict[str, Any] | None,
    state: dict[str, Any],
    checked_at: int,
    *,
    update: bool,
) -> dict[str, Any] | None:
    if payload is None:
        return None
    diagnostic: dict[str, Any] = {
        "head": payload["head"],
        "indexed_head": payload["indexed_head"],
        "lag_blocks": payload["lag_blocks"],
        "state": payload["state"],
    }
    as_of = payload.get("as_of")
    if (
        isinstance(as_of, (int, float))
        and not isinstance(as_of, bool)
        and math.isfinite(as_of)
    ):
        age = checked_at - float(as_of)
        diagnostic["fresh"] = -30 <= age <= 120
        diagnostic["age_s"] = max(0, int(age))
    else:
        diagnostic["fresh"] = None

    for field, state_key, progress_key in (
        ("head", "last_head", "last_head_progress_at"),
        ("indexed_head", "last_indexed_head", "last_index_progress_at"),
    ):
        current = payload[field]
        previous = state.get(state_key)
        progressing = None if not _valid_int(previous) else current > previous
        diagnostic[f"{field}_progressing"] = progressing
        progress_at = state.get(progress_key)
        if (
            progressing is True
            or not isinstance(progress_at, (int, float))
            or isinstance(progress_at, bool)
            or not math.isfinite(progress_at)
        ):
            progress_at = checked_at
        diagnostic[f"{field}_stalled_s"] = max(0, int(checked_at - progress_at))
        if update:
            state[state_key] = current
            state[progress_key] = progress_at
    return diagnostic


def _prune_attempts(state: dict[str, Any], key: str, now: int) -> list[float]:
    attempts = [
        float(value)
        for value in state[key]
        if now - RECOVERY_WINDOW_SECONDS < float(value)
    ]
    state[key] = attempts
    return attempts


def _systemd_status(unit: str, timeout: float) -> dict[str, Any] | None:
    command = [
        "systemctl",
        "--user",
        "show",
        unit,
        "--property=LoadState",
        "--property=ActiveState",
        "--property=SubState",
        "--property=ActiveEnterTimestampMonotonic",
        "--no-pager",
    ]
    try:
        completed = subprocess.run(
            command,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            timeout=timeout,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if completed.returncode != 0 or len(completed.stdout) > 8192:
        return None
    properties: dict[str, str] = {}
    for line in completed.stdout.splitlines():
        key, separator, value = line.partition("=")
        if separator:
            properties[key] = value
    if properties.get("LoadState") != "loaded":
        return None
    active = properties.get("ActiveState")
    if active not in {"active", "inactive", "failed", "activating", "deactivating", "reloading"}:
        return None
    output: dict[str, Any] = {"active_state": active}
    sub = properties.get("SubState")
    if sub and len(sub) <= 64 and re.fullmatch(r"[A-Za-z0-9_-]+", sub):
        output["sub_state"] = sub
    raw_entered = properties.get("ActiveEnterTimestampMonotonic")
    if raw_entered and raw_entered.isdigit() and int(raw_entered) > 0:
        output["active_age_s"] = max(0, int(time.monotonic() - int(raw_entered) / 1_000_000))
    return output


def _restart_unit(unit: str, timeout: float) -> tuple[bool, str]:
    try:
        completed = subprocess.run(
            ["systemctl", "--user", "restart", unit],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=timeout,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return False, "restart_timeout"
    except OSError:
        return False, "restart_unavailable"
    return (True, "restarted") if completed.returncode == 0 else (False, "restart_failed")


def _rate_limit_reason(attempts: list[float], now: int) -> str | None:
    if attempts and now - max(attempts) < RECOVERY_COOLDOWN_SECONDS:
        return "cooldown"
    if len(attempts) >= MAX_RECOVERIES_PER_WINDOW:
        return "hourly_limit"
    return None


def _app_recovery(
    args: argparse.Namespace,
    state: dict[str, Any],
    now: int,
    state_path: Path,
) -> dict[str, Any]:
    report: dict[str, Any] = {
        "attempted": False,
        "consecutive_failures": state["origin_failures"],
    }
    if state["origin_failures"] < FAILURE_THRESHOLD:
        report["reason"] = "below_threshold"
        return report
    attempts = _prune_attempts(state, "app_attempts", now)
    limited = _rate_limit_reason(attempts, now)
    if limited is not None:
        report["reason"] = limited
        return report
    unit_status = _systemd_status(args.app_unit, args.timeout)
    if unit_status is None:
        report["reason"] = "unit_status_unavailable"
        return report
    report["unit"] = unit_status
    if unit_status["active_state"] == "active":
        age = unit_status.get("active_age_s")
        if age is None:
            report["reason"] = "startup_age_unknown"
            return report
        if age < STARTUP_GRACE_SECONDS:
            report["reason"] = "startup_grace"
            return report
    elif unit_status["active_state"] not in {"inactive", "failed"}:
        report["reason"] = "unit_transitioning"
        return report

    attempts.append(float(now))
    state["app_attempts"] = attempts
    if not _save_state(state_path, state):
        report["reason"] = "state_write_failed"
        return report
    ok, reason = _restart_unit(args.app_unit, args.timeout)
    report.update({"attempted": True, "ok": ok, "reason": reason})
    return report


def _tunnel_recovery(
    args: argparse.Namespace,
    state: dict[str, Any],
    now: int,
    state_path: Path,
    *,
    origin_ok: bool,
) -> dict[str, Any]:
    report: dict[str, Any] = {
        "attempted": False,
        "consecutive_failures": state["public_failures"],
    }
    if not origin_ok:
        report["reason"] = "origin_unhealthy"
        return report
    if state["public_failures"] < FAILURE_THRESHOLD:
        report["reason"] = "below_threshold"
        return report
    attempts = _prune_attempts(state, "tunnel_attempts", now)
    limited = _rate_limit_reason(attempts, now)
    if limited is not None:
        report["reason"] = limited
        return report
    unit_status = _systemd_status(args.tunnel_unit, args.timeout)
    if unit_status is None:
        report["reason"] = "unit_status_unavailable"
        return report
    report["unit"] = unit_status
    if unit_status["active_state"] not in {"inactive", "failed"}:
        report["reason"] = (
            "active_no_restart" if unit_status["active_state"] == "active" else "unit_transitioning"
        )
        return report

    attempts.append(float(now))
    state["tunnel_attempts"] = attempts
    if not _save_state(state_path, state):
        report["reason"] = "state_write_failed"
        return report
    ok, reason = _restart_unit(args.tunnel_unit, args.timeout)
    report.update({"attempted": True, "ok": ok, "reason": reason})
    return report


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check-only", action="store_true", help="probe and report without changing recovery state")
    parser.add_argument("--origin-url", default=DEFAULT_ORIGIN_URL, help="local HTTP(S) base URL")
    parser.add_argument("--public-url", default=DEFAULT_PUBLIC_URL, help="public HTTP(S) base URL")
    parser.add_argument("--state-path", default=str(DEFAULT_STATE_PATH), help="absolute recovery state path outside the checkout")
    parser.add_argument("--timeout", type=float, default=3.0, help="per-operation timeout in seconds (0.1 to 30)")
    parser.add_argument("--app-unit", default=DEFAULT_APP_UNIT, help="exact systemd user app service")
    parser.add_argument("--tunnel-unit", default=DEFAULT_TUNNEL_UNIT, help="exact systemd user tunnel service")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    checked_at = int(time.time())
    try:
        if not 0.1 <= args.timeout <= 30:
            raise ValueError("timeout_out_of_range")
        args.origin_url = _normalize_base_url(args.origin_url)
        args.public_url = _normalize_base_url(args.public_url)
        args.app_unit = _validate_unit(args.app_unit)
        args.tunnel_unit = _validate_unit(args.tunnel_unit)
        state_path = _validate_state_path(args.state_path)
    except ValueError as exc:
        _emit({"checked_at": checked_at, "ok": False, "error": str(exc)})
        return 2

    with _invocation_lock(state_path) as acquired:
        if not acquired:
            _emit({"checked_at": checked_at, "ok": False, "error": "invocation_locked"})
            return 2

        origin, origin_payload = _probe_target(args.origin_url, args.timeout)
        public, public_payload = _probe_target(args.public_url, args.timeout)
        state, state_ok = _load_state(state_path)
        status_diagnostic = _status_diagnostic(
            origin_payload, state, checked_at, update=not args.check_only and state_ok
        )
        public_status = _status_diagnostic(public_payload, {}, checked_at, update=False)
        if status_diagnostic is not None:
            origin["status"] = status_diagnostic
        if public_status is not None:
            public["status"] = public_status

        output: dict[str, Any] = {
            "checked_at": checked_at,
            "origin": origin,
            "public": public,
            "check_only": args.check_only,
        }
        if args.check_only:
            output["recovery"] = {"app": {"attempted": False}, "tunnel": {"attempted": False}}
            _emit(output)
            return 0 if origin["ok"] and public["ok"] else 1

        if not state_ok:
            output["recovery"] = {
                "app": {"attempted": False, "reason": "state_unavailable"},
                "tunnel": {"attempted": False, "reason": "state_unavailable"},
            }
            _emit(output)
            return 2

        state["origin_failures"] = 0 if origin["ok"] else min(state["origin_failures"] + 1, 1_000_000)
        state["public_failures"] = 0 if public["ok"] else min(state["public_failures"] + 1, 1_000_000)
        state["last_checked_at"] = checked_at
        _prune_attempts(state, "app_attempts", checked_at)
        _prune_attempts(state, "tunnel_attempts", checked_at)
        if not _save_state(state_path, state):
            output["recovery"] = {
                "app": {"attempted": False, "reason": "state_write_failed"},
                "tunnel": {"attempted": False, "reason": "state_write_failed"},
            }
            _emit(output)
            return 2

        if origin["ok"]:
            app_report = {
                "attempted": False,
                "consecutive_failures": state["origin_failures"],
                "reason": "origin_healthy",
            }
            tunnel_report = _tunnel_recovery(
                args, state, checked_at, state_path, origin_ok=True
            )
        else:
            app_report = _app_recovery(args, state, checked_at, state_path)
            tunnel_report = {
                "attempted": False,
                "consecutive_failures": state["public_failures"],
                "reason": "origin_unhealthy",
            }
        output["recovery"] = {"app": app_report, "tunnel": tunnel_report}
        _emit(output)
        if (app_report.get("attempted") and not app_report.get("ok")) or (
            tunnel_report.get("attempted") and not tunnel_report.get("ok")
        ):
            return 2
        return 0 if origin["ok"] and public["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
