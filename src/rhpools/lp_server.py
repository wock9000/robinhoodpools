"""Standalone LP-only HTTP server and lifecycle owner."""
from __future__ import annotations

import argparse
from contextlib import ExitStack
import hashlib
import json
import os
import re
import signal
import sys
import threading
import time
import zlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

STATIC = Path(__file__).resolve().parent / "static"
MAX_BODY = 16_384
WORKBENCH_RESOLUTION_WAIT_S = 2.0
_BYTES32_RE = re.compile(r"0x[0-9a-f]{64}")

_ASSETS = {
    "/": ("lp_terminal.html", "text/html; charset=utf-8"),
    "/pools": ("lp_terminal.html", "text/html; charset=utf-8"),
    "/lp": ("lp_terminal.html", "text/html; charset=utf-8"),
    "/pool": ("workbench.html", "text/html; charset=utf-8"),
    "/guide": ("lp_guide.html", "text/html; charset=utf-8"),
    "/research": ("lp_research.html", "text/html; charset=utf-8"),
    "/flow": ("lp_flow.html", "text/html; charset=utf-8"),
    "/api/v1/openapi.json": ("openapi.json", "application/json; charset=utf-8"),
}
for _name in (
    "lp_theme.css", "lp_terminal.css", "lp_terminal.js", "lp_panes_boot.js",
    "workbench.css", "workbench.js", "lp_guide.css",
    "lp_research.css", "lp_research.js", "lp_flow.css", "lp_flow.js",
):
    _ASSETS["/static/" + _name] = (
        _name,
        "text/css; charset=utf-8" if _name.endswith(".css") else "application/javascript; charset=utf-8",
    )

_GET_METHODS = {
    "/api/lp/" + name: ("lp", name)
    for name in ("status", "search", "overview", "pools", "tape", "owners", "closed", "owner")
}
_GET_METHODS.update({
    "/api/v1/pools": ("public", "pools"),
    "/api/v1/assets": ("public", "assets"),
    "/api/v1/research/owner": ("research", "owner"),
    "/api/v1/fomo/flow": ("flow", "flow"),
    "/api/workbench/pools": ("market", "catalog"),
})


_body_cache = [None] * 16
_body_locks = tuple(threading.Lock() for _ in range(4))


def _memoized_body(key, build):
    slot = hash(key) % len(_body_cache)
    cached = _body_cache[slot]
    if cached is not None and cached[0] == key:
        return cached[1]
    with _body_locks[slot % len(_body_locks)]:
        cached = _body_cache[slot]
        if cached is not None and cached[0] == key:
            return cached[1]
        body = build()
        if len(body) <= 1024 * 1024:
            _body_cache[slot] = (key, body)
        return body


def _json_bytes(payload, key=None):
    def build():
        return json.dumps(payload, allow_nan=False, separators=(",", ":")).encode()
    return build() if key is None else _memoized_body(("json", *key), build)


def _publication_key(path, query, payload):
    status = payload.get("status") or {}
    revision = payload.get("revision", status.get("revision"))
    if revision is None or path == "/api/lp/status":
        return None
    activity = payload.get("current_activity") or {}
    return (
        path, tuple(sorted(query.items())), revision,
        payload.get("epoch", status.get("epoch")),
        payload.get("financial_revision"), payload.get("accounting_as_of"),
        payload.get("as_of", status.get("as_of")), payload.get("block_hash"),
        tuple(activity.get(k) for k in ("revision", "epoch", "head", "observed_from")),
    )


def _load_assets():
    raw = {name: (STATIC / name).read_bytes() for name in {name for name, _ in _ASSETS.values()}}
    theme = raw["lp_theme.css"]
    for name in raw:
        if name.endswith(".css") and name != "lp_theme.css":
            raw[name] = theme + b"\n" + raw[name]
    versions = {name: hashlib.sha256(body).hexdigest()[:16] for name, body in raw.items()}
    assets = {}
    for path, (name, content_type) in _ASSETS.items():
        body = raw[name]
        if name.endswith(".html"):
            css_name = name[:-5] + ".css"
            if css_name in raw:
                body = body.replace(
                    f'<link rel="stylesheet" href="/static/{css_name}">'.encode(),
                    b"<style>" + raw[css_name] + b"</style>",
                )
            for dependency, version in versions.items():
                body = body.replace(
                    f'"/static/{dependency}"'.encode(),
                    f'"/static/{dependency}?v={version}"'.encode(),
                )
        assets[path] = (
            body, content_type, '"' + hashlib.sha256(body).hexdigest()[:16] + '"',
            zlib.compress(body, level=1, wbits=31),
        )
    return assets


class Runtime:
    def __init__(self, args: argparse.Namespace) -> None:
        from .lp_market_service import LPMarketService
        from .lp_public_api import PublicMarketAPI
        from .lp_research import LPResearchService
        from .lp_fomo_flow import FomoFlowService
        from .workbench_actions import ActionService
        from .workbench_market import MarketService

        self.stopping = threading.Event()
        self.origins = frozenset(args.public_origin)
        self.enable_prepare = args.enable_transaction_prepare
        self.assets = _load_assets()
        self._resources = ExitStack()
        self._close_lock = threading.Lock()
        try:
            self.market = MarketService(
                args.rpc_url, data_dir=args.data_dir, external_index=True,
            )
            self._resources.callback(self.market.close)
            self.lp = LPMarketService(
                self.market, args.rpc_url, args.database,
                history_days=args.history_days,
                history_disk_reserve_bytes=int(args.disk_reserve_gib * 1024**3),
            )
            self._resources.callback(self.lp.close)
            self.actions = ActionService(self.market.rpc)
            self._resources.callback(self.actions.close)
            self.public = PublicMarketAPI(self.lp)
            self._resources.callback(self.public.close)
            self.research = LPResearchService(self.lp)
            self.flow = FomoFlowService()
            self._resources.callback(self.flow.close)
        except BaseException:
            self._resources.close()
            raise

    def close(self) -> None:
        self.stopping.set()
        with self._close_lock:
            self._resources.close()


class LPHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True


class Handler(BaseHTTPRequestHandler):
    runtime: Runtime
    api_slots = threading.BoundedSemaphore(64)
    lp_streams = threading.BoundedSemaphore(512)
    workbench_streams = threading.BoundedSemaphore(128)

    def setup(self) -> None:
        self.request.settimeout(20)
        self._event_write = None
        super().setup()

    def log_message(self, fmt: str, *args: object) -> None:
        # Query strings and arbitrary exception text do not belong in access logs.
        if args and isinstance(args[0], str):
            words = args[0].split()
            if len(words) >= 2:
                super().log_message("%s %s", words[0], urlsplit(words[1]).path)

    def _gzip_ok(self) -> bool:
        match = re.search(r"(?:^|,)\s*gzip(?:\s*;\s*q=([01](?:\.\d+)?))?\s*(?:,|$)", str(self.headers.get("Accept-Encoding") or "").lower())
        return match is not None and float(match.group(1) or "1") > 0

    def _json(self, status: int, payload: object, *, retry: int | None = None, key=None) -> None:
        body = _json_bytes(payload, key)
        compressed = len(body) >= 1024 and self._gzip_ok()
        if compressed:
            build = lambda: zlib.compress(body, level=1, wbits=31)
            body = build() if key is None else _memoized_body(("gzip", *key), build)
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Vary", "Accept-Encoding")
        self.send_header("Access-Control-Allow-Origin", "*")
        if compressed:
            self.send_header("Content-Encoding", "gzip")
        if retry is not None:
            self.send_header("Retry-After", str(retry))
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def _asset(self, path: str, head: bool = False) -> bool:
        item = self.runtime.assets.get(path)
        if item is None:
            return False
        raw, content_type, etag, zipped = item
        same = self.headers.get("If-None-Match") == etag
        compressed = self._gzip_ok() and len(raw) >= 1024
        body = zipped if compressed else raw
        self.send_response(304 if same else 200)
        self.send_header("Content-Type", content_type)
        self.send_header("ETag", etag)
        self.send_header("Cache-Control", "public,max-age=60" if path.startswith("/static/") else "no-store")
        self.send_header("Vary", "Accept-Encoding")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("X-Frame-Options", "SAMEORIGIN" if path == "/pool" else "DENY")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header(
            "Content-Security-Policy",
            "default-src 'self';script-src 'self';style-src 'self' 'unsafe-inline';"
            "img-src 'self' data:;connect-src 'self';object-src 'none';base-uri 'none';"
            "frame-ancestors " + ("'self'" if path == "/pool" else "'none'"),
        )
        if path.startswith("/api/"):
            self.send_header("Access-Control-Allow-Origin", "*")
        if compressed:
            self.send_header("Content-Encoding", "gzip")
        if not same:
            self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if not head and self.command != "HEAD" and not same:
            self.wfile.write(body)
        return True

    def _query(self) -> tuple[str, dict[str, str]]:
        if len(self.path) > 8192:
            raise ValueError("Request URI is too long")
        parsed = urlsplit(self.path)
        return parsed.path, {
            key: values[-1]
            for key, values in parse_qs(parsed.query, max_num_fields=64).items()
        }

    def _bounded(self, call, path="", query=None) -> None:
        if not self.api_slots.acquire(False):
            self._json(503, {"error": "API request capacity reached"}, retry=1)
            return
        try:
            try:
                payload = call()
            finally:
                self.runtime.lp.store.close_reader()
            key = _publication_key(path, query or {}, payload) if isinstance(payload, dict) else None
            self._json(200, payload, key=key)
        finally:
            self.api_slots.release()

    def _start_event_stream(self):
        compressor = zlib.compressobj(level=1, wbits=31) if self._gzip_ok() else None
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache,no-transform")
        self.send_header("X-Accel-Buffering", "no")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Vary", "Accept-Encoding")
        if compressor is not None:
            self.send_header("Content-Encoding", "gzip")
        self.end_headers()

        def write_event(data):
            if compressor is not None:
                data = compressor.compress(data) + compressor.flush(zlib.Z_SYNC_FLUSH)
            self.wfile.write(data)
            self.wfile.flush()
        self._event_write = write_event
        return write_event

    def _request_workbench_pool(
            self, query: dict[str, str], timeout: float = WORKBENCH_RESOLUTION_WAIT_S,
    ):
        pool_id = str(query.get("id") or "").strip().lower()
        self.runtime.lp.indexer.request_pool_resolution(
            pool_id, query.get("tx", ""),
        )
        # Stored identities publish synchronously. A genuinely cold V4 lookup
        # uses the indexer's bounded executor, so give that publication a
        # bounded chance to arrive before declaring the identifier unknown.
        if _BYTES32_RE.fullmatch(pool_id):
            return self.runtime.market.wait_pool(pool_id, timeout)
        return self.runtime.market._pool_by_id(pool_id)

    def _workbench_detail(self, query: dict[str, str]):
        self._request_workbench_pool(query)
        return self.runtime.market.detail(
            query.get("id", ""), query.get("owner"),
        )

    def _workbench_stream(self, query):
        revision = None
        write = self._start_event_stream()
        while not self.runtime.stopping.is_set():
            if self._request_workbench_pool(query, 10.0) is None:
                write(b": heartbeat\n\n")
                self.runtime.stopping.wait(0.05)
                continue
            detail = self.runtime.market.wait_detail(
                query.get("id", ""), query.get("owner"), revision, 10.0,
            )
            self.runtime.lp.store.close_reader()
            if detail is None:
                write(b": heartbeat\n\n")
                self.runtime.stopping.wait(0.05)
                continue
            revision = detail.get("revision")
            key = _publication_key("workbench-stream", query, detail)
            write(b"data: " + _json_bytes({"type": "pool", "data": detail}, key) + b"\n\n")

    def _lp_stream(self, query):
        lp = self.runtime.lp
        after = max(0, int(query.get("after") or 0))
        block_after = max(0, int(query.get("block_after") or 0))
        feed_epoch = str(query.get("feed_epoch") or "") or None
        cursor = str(self.headers.get("Last-Event-ID") or "")
        if cursor:
            parts = cursor.split(":", 2)
            try:
                after = max(0, int(parts[0]))
                if len(parts) == 3:
                    feed_epoch, block_after = parts[1] or None, max(0, int(parts[2]))
            except ValueError:
                pass
        epoch = int(query.get("epoch") or -1)
        current_only = str(query.get("current_only") or "").lower() in {"1", "true", "yes"}
        terminal = query.get("view") == "terminal"
        owners_enabled = terminal and str(query.get("owners", "1")).lower() not in {"0", "false", "no", "off"}
        channel = query.get("channel") or "both"
        if channel not in {"both", "heads", "activity"}:
            raise ValueError("Unknown current feed channel")
        view_key = tuple(sorted(
            (key, value) for key, value in query.items()
            if key not in {"after", "epoch", "block_after", "feed_epoch"}
        ))
        try:
            feed = lp.stream_updates(query, block_after, feed_epoch)
        finally:
            lp.store.close_reader()
        write = self._start_event_stream()
        frame = None
        last_write = time.monotonic()
        next_durable = float("inf") if current_only or terminal else 0.0
        next_owner = 0.0 if owners_enabled else float("inf")
        owner_revision = None
        seen_rows = {}
        while not self.runtime.stopping.is_set():
            for item in feed["events"]:
                block_after = max(block_after, int(item["sequence"]))
                feed_epoch = feed["feed_epoch"]
                if channel == "heads" and item["event"] != "block" or channel == "activity" and item["event"] != "activity":
                    continue
                key = ("lp-feed", feed_epoch, int(item["sequence"]), item["event"], () if item["event"] == "block" else view_key)
                body = _json_bytes(item["data"], key)
                write(f"id: {after}:{feed_epoch}:{block_after}\nevent: {item['event']}\n".encode() + b"data: " + body + b"\n\n")
                last_write = time.monotonic()
            if frame is not None:
                current_rows = {str(row["id"]): row for row in frame["rows"]}
                frame["removed"] = [key for key in seen_rows if key not in current_rows]
                frame["snapshot"] = not seen_rows or frame["reset"]
                frame["rows"] = [row for key, row in current_rows.items() if row != seen_rows.get(key)]
                seen_rows = current_rows
                after, epoch = frame["revision"], frame["epoch"]
                write(f"id: {after}:{feed_epoch or feed['feed_epoch']}:{block_after}\n".encode() + b"data: " + _json_bytes(frame) + b"\n\n")
                frame = None
                last_write = time.monotonic()
            now = time.monotonic()
            if owners_enabled and now >= next_owner:
                try:
                    owners = lp.poll_owners(query, owner_revision)
                finally:
                    lp.store.close_reader()
                next_owner = now + 1.0
                if owners is not None:
                    owner_revision = (int(owners["revision"]), int((owners.get("current_activity") or {}).get("epoch") or 0))
                    body = _json_bytes(owners, ("lp-owners", view_key, *owner_revision))
                    write(f"id: {after}:{feed_epoch or feed['feed_epoch']}:{block_after}\nevent: owners\n".encode() + b"data: " + body + b"\n\n")
                    last_write = time.monotonic()
            block_after = max(block_after, int(feed["sequence"]))
            feed_epoch = feed["feed_epoch"]
            feed = {**feed, "events": []}
            now = time.monotonic()
            if not current_only and now >= next_durable:
                try:
                    frame = lp.poll_frame(query, after, epoch)
                finally:
                    lp.store.close_reader()
                next_durable = now + 0.5
                if frame is not None:
                    continue
            if now - last_write >= 10:
                write(b": heartbeat\n\n")
                last_write = now
            lp.wait_stream(block_after, min(0.5, max(0.01, min(next_durable, next_owner) - now)))
            try:
                feed = lp.stream_updates(query, block_after, feed_epoch)
            finally:
                lp.store.close_reader()

    def _sse(self, workbench: bool, query: dict[str, str]) -> None:
        if self.command == "HEAD":
            return self._json(405, {"error": "Use GET for event streams"})
        slots = self.workbench_streams if workbench else self.lp_streams
        if not slots.acquire(False):
            return self._json(503, {"error": "Live stream capacity reached"}, retry=2)
        try:
            if workbench:
                self._workbench_stream(query)
            else:
                self._lp_stream(query)
        except (BrokenPipeError, ConnectionResetError, TimeoutError, OSError):
            pass
        except Exception:
            if self._event_write is None:
                raise
            try:
                self._event_write(b'event: error\ndata: {"error":"Live data is temporarily unavailable"}\n\n')
            except (BrokenPipeError, ConnectionResetError, TimeoutError, OSError):
                pass
        finally:
            self.runtime.lp.store.close_reader()
            slots.release()

    def do_GET(self) -> None:
        try:
            path, query = self._query()
        except ValueError as exc:
            return self._json(400, {"error": str(exc)})
        if self._asset(path):
            return
        try:
            target = _GET_METHODS.get(path)
            if target is not None:
                resource, method_name = target
                method = getattr(getattr(self.runtime, resource), method_name)
                call = method if path == "/api/lp/status" else lambda: method(query)
                return self._bounded(call, path, query)
            if path == "/api/workbench/capabilities":
                local = self._loopback()
                return self._json(200, {"allocation_preview": True, "simulate": local, "prepare": local and self.runtime.enable_prepare, "broadcast": False, "server_signing": False})
            if path == "/api/workbench/pool":
                return self._bounded(
                    lambda: self._workbench_detail(query), path, query,
                )
            if path == "/api/lp/stream":
                return self._sse(False, query)
            if path == "/api/workbench/stream":
                return self._sse(True, query)
            self._json(404, {"error": "Not found"})
        except ValueError as exc:
            self._json(400, {"error": str(exc)})
        except (BrokenPipeError, ConnectionResetError, TimeoutError):
            return
        except Exception:
            self._json(503, {"error": "Upstream data is temporarily unavailable"}, retry=2)

    def do_OPTIONS(self) -> None:
        try:
            path, _query = self._query()
        except ValueError as exc:
            return self._json(400, {"error": str(exc)})
        if path not in _GET_METHODS and path not in {
            "/api/v1/openapi.json", "/api/workbench/pool", "/api/workbench/capabilities",
        }:
            return self._json(404, {"error": "Not found"})
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, HEAD, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Accept, Content-Type")
        self.send_header("Content-Length", "0")
        self.end_headers()

    def _loopback(self) -> bool:
        try:
            return urlsplit("http://" + str(self.headers.get("Host") or "")).hostname in {"localhost", "127.0.0.1", "::1"}
        except ValueError:
            return False

    def _same_origin(self) -> bool:
        origin = str(self.headers.get("Origin") or "")
        host = str(self.headers.get("Host") or "")
        if self._loopback():
            return origin == "http://" + host
        return origin in self.runtime.origins and urlsplit(origin).netloc == host

    def do_POST(self) -> None:
        try:
            path, _query = self._query()
        except ValueError as exc:
            return self._json(400, {"error": str(exc)})
        if path not in {"/api/lp/allocation", "/api/workbench/simulate", "/api/workbench/prepare"}:
            return self._json(404, {"error": "Not found"})
        if not self._same_origin():
            return self._json(403, {"error": "Configured same-origin request required"})
        if path != "/api/lp/allocation" and not self._loopback():
            return self._json(403, {"error": "Simulation and preparation require loopback access"})
        if path.endswith("/prepare") and not self.runtime.enable_prepare:
            return self._json(403, {"error": "Transaction preparation is disabled"})
        if self.headers.get("Content-Type", "").split(";", 1)[0].strip() != "application/json":
            return self._json(415, {"error": "Expected application/json"})
        if not self.api_slots.acquire(False):
            return self._json(503, {"error": "API request capacity reached"}, retry=1)
        try:
            size = int(self.headers.get("Content-Length", "0"))
            if not 0 < size <= MAX_BODY:
                return self._json(413, {"error": "Request must be between 1 and 16384 bytes"})
            payload = json.loads(self.rfile.read(size))
            if not isinstance(payload, dict):
                raise ValueError("Expected a JSON object")
            if path == "/api/lp/allocation":
                from .lp_allocation import preview
                result = preview(payload, self.runtime.market, self.runtime.lp.store)
            elif path.endswith("/simulate"):
                result = self.runtime.actions.simulate(payload)
            else:
                result = self.runtime.actions.prepare(payload)
            self.runtime.lp.store.close_reader()
            self._json(200, result)
        except (ValueError, KeyError, TypeError) as exc:
            self._json(400, {"error": str(exc)})
        except (BrokenPipeError, ConnectionResetError, TimeoutError):
            return
        except Exception:
            self._json(503, {"error": "Upstream data is temporarily unavailable"}, retry=2)
        finally:
            self.runtime.lp.store.close_reader()
            self.api_slots.release()

    def do_HEAD(self) -> None:
        self.do_GET()

    def finish(self) -> None:
        try:
            super().finish()
        finally:
            if hasattr(self, "runtime"):
                self.runtime.lp.store.close_reader()


def parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="RobinhoodPools public LP explorer")
    ap.add_argument("--host", default=os.environ.get("RHP_HTTP_HOST", "127.0.0.1"))
    ap.add_argument("--port", type=int, default=int(os.environ.get("RHP_HTTP_PORT", "8196")))
    ap.add_argument("--rpc-url", default=os.environ.get("RHP_RPC_URL", "https://rpc.mainnet.chain.robinhood.com"))
    ap.add_argument("--data-dir", default=os.environ.get("RHP_DATA_DIR", str(Path.home() / ".local/share/rhpools")))
    ap.add_argument("--database", default=os.environ.get("RHP_DATABASE"))
    ap.add_argument("--history-days", type=int, default=int(os.environ.get("LP_HISTORY_DAYS", "30")))
    ap.add_argument("--disk-reserve-gib", type=float, default=float(os.environ.get("LP_DISK_RESERVE_GIB", "4")))
    ap.add_argument("--public-origin", action="append", default=[])
    ap.add_argument("--enable-transaction-prepare", action="store_true")
    return ap


def main() -> None:
    args = parser().parse_args()
    # Reduce SQLite I/O handoff delays behind CPU-heavy valuation threads.
    sys.setswitchinterval(min(sys.getswitchinterval(), 0.001))
    data_dir = Path(args.data_dir).expanduser()
    data_dir.mkdir(parents=True, exist_ok=True)
    args.data_dir = data_dir
    args.database = Path(args.database).expanduser() if args.database else data_dir / "lp_market.sqlite"
    server = LPHTTPServer((args.host, args.port), Handler)
    try:
        runtime = Runtime(args)
    except BaseException:
        server.server_close()
        raise
    Handler.runtime = runtime
    def halt(_signum=None, _frame=None):
        runtime.stopping.set()
        threading.Thread(target=server.shutdown, daemon=True).start()
    signal.signal(signal.SIGINT, halt)
    signal.signal(signal.SIGTERM, halt)
    print(f"RobinhoodPools listening on http://{args.host}:{args.port}", flush=True)
    try:
        server.serve_forever()
    finally:
        server.server_close()
        runtime.close()


if __name__ == "__main__":
    main()
