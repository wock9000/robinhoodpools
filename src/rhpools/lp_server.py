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
from dataclasses import replace
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import NamedTuple
from urllib.parse import parse_qs, urlsplit

from .lp_gate import COOKIE_NAME, Entitlement, Gate, GatePolicy, GateRefusal, Principal
from .lp_gate_ws import CLOSE_NOT_ENTITLED, WebSocketPush
from .lp_flow_tags import FlowTagger, PostgresListener, TagStore
from .tx_core import JsonRpc, TxCore
from .tx_trade_store import TradeStore
from .tx_plan import LpIntent, LpOp, Signatures, SwapIntent, TxError, TxPolicy, TxRefusal
from .tx_routes import RouteBook, Side

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
    "lp_theme.css", "lp_terminal.css", "lp_terminal.js", "lp_panes_boot.js", "lp_gate.css", "lp_gate.js", "lp_trade.css", "lp_trade.js",
    "workbench.css", "workbench.js", "lp_guide.css",
    "lp_research.css", "lp_research.js", "lp_flow.css", "lp_flow.js",
):
    _ASSETS["/static/" + _name] = (
        _name,
        "text/css; charset=utf-8" if _name.endswith(".css") else "application/javascript; charset=utf-8",
    )


class Route(NamedTuple):
    resource: str | None
    method: str
    lane: str
    freshness: tuple[int, int] | dict[str, tuple[int, int]]


# The service coalesces summary refreshes. Downstream caches must revalidate,
# rather than adding another freshness window to an already retained snapshot.
_WINDOW_FRESHNESS = dict.fromkeys(("1h", "24h", "7d", "30d", "all"), (0, 0))
_ROUTES = {
    "/api/lp/status": Route("lp", "status", "fast", (1, 2)),
    "/api/lp/search": Route("lp", "search", "fast", (5, 30)),
    "/api/lp/overview": Route("lp", "overview", "fast", _WINDOW_FRESHNESS),
    "/api/lp/pools": Route("lp", "pools", "fast", _WINDOW_FRESHNESS),
    "/api/lp/tape": Route("lp", "tape", "fast", (2, 10)),
    "/api/lp/dislocations": Route("lp", "dislocations", "fast", (2, 10)),
    "/api/lp/owners": Route("lp", "owners", "slow", (5, 60)),
    "/api/lp/closed": Route("lp", "closed", "slow", (15, 60)),
    "/api/lp/owner": Route("lp", "owner", "slow", (5, 60)),
    "/api/v1/pools": Route("public", "pools", "slow", (2, 10)),
    "/api/v1/assets": Route("public", "assets", "slow", (2, 10)),
    "/api/v1/research/owner": Route("research", "owner", "slow", (5, 60)),
    "/api/v1/fomo/flow": Route("flow", "flow", "slow", (10, 60)),
    "/api/workbench/pools": Route("market", "catalog", "fast", (5, 30)),
    "/api/workbench/pool": Route(None, "_workbench_detail", "slow", (2, 10)),
}
_LANE_SHARE = {"fast": 1.0, "slow": 0.5, "keyed": 0.5}
_GATE_GET = {"/api/gate/nonce", "/api/gate/me", "/api/gate/keys", "/api/gate/policy"}
_GATE_POST = {"/api/gate/session", "/api/gate/keys", "/api/gate/logout", "/api/gate/policy"}
KEYED_STREAM = "/api/v1/stream"
_TX_GET = {"/api/tx/status", "/api/tx/receipt", "/api/tx/balances", "/api/tx/pool", "/api/tx/history"}
_TX_POST = {"/api/tx/quote", "/api/tx/prepare"}
TAGS_PATH = "/api/v1/tags"
TAGS_MAX_TX = 100
_MINT_TTL_DEFAULT_S = 90 * 86400
DEFAULT_API_SLOTS = 4 * (getattr(os, "process_cpu_count", os.cpu_count)() or 1)


def lane_slots(total: int) -> dict[str, threading.BoundedSemaphore]:
    return {
        lane: threading.BoundedSemaphore(max(1, int(total * share)))
        for lane, share in _LANE_SHARE.items()
    }


def freshness(route: Route, query: dict[str, str]) -> tuple[int, int]:
    policy = route.freshness
    if isinstance(policy, dict):
        return policy[str(query.get("window") or "24h")]
    return policy


def _etag(key, body: bytes) -> str:
    identity = repr(key).encode() if key is not None else body
    return 'W/"' + hashlib.blake2b(identity, digest_size=8).hexdigest() + '"'


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
    # status.as_of is the wall clock at publication, not part of the identity.
    return (
        path, tuple(sorted(query.items())), revision,
        payload.get("epoch", status.get("epoch")),
        payload.get("financial_revision"), payload.get("accounting_as_of"),
        payload.get("block_hash"),
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


def _startup_activity(tid: int) -> dict[str, int] | None:
    """Work counters that grow only while initialization really advances.

    CPU is the initializing thread's own, so serving probes never masquerades
    as progress; storage bytes are process-wide because SQLite performs the
    reads of a long WAL recovery from whichever thread the store chooses.
    """
    try:
        with open(f"/proc/self/task/{tid}/stat", "rb") as handle:
            stat = handle.read()
        with open("/proc/self/io", "rb") as handle:
            io = handle.read()
        fields = stat[stat.rindex(b")") + 2:].split()
        ticks = int(fields[11]) + int(fields[12])
        counters = dict(line.split(b":", 1) for line in io.splitlines() if b":" in line)
        return {
            "cpu_ms": ticks * 1000 // os.sysconf("SC_CLK_TCK"),
            "read_bytes": int(counters[b"read_bytes"]),
            "write_bytes": int(counters[b"write_bytes"]),
        }
    except (OSError, ValueError, IndexError, KeyError):
        return None


class Startup:
    """What the process can say for itself before the runtime exists.

    Opening a large store is a single blocking call that can run for hours,
    so the socket is served from the start and every request that needs
    data gets this record instead of a queued connection that times out.
    """

    def __init__(self, assets: dict) -> None:
        self.assets = assets
        self.started_at = time.time()
        self.tid = threading.get_native_id()
        self._clock = time.monotonic()
        self._phase = "starting"

    def phase(self, name: str) -> None:
        self._phase = name
        print(f"RobinhoodPools startup: {name}", flush=True)

    def snapshot(self) -> dict:
        elapsed = int(time.monotonic() - self._clock)
        return {
            "chain_id": 4663,
            "state": "starting",
            "error": f"Service is starting: {self._phase} ({elapsed}s elapsed)",
            "startup": {
                "started_at": self.started_at,
                "phase": self._phase,
                "elapsed_s": elapsed,
                "activity": _startup_activity(self.tid),
            },
            "as_of": time.time(),
        }


class Runtime:
    def __init__(self, args: argparse.Namespace, startup: Startup) -> None:
        from .lp_market_service import LPMarketService
        from .lp_public_api import PublicMarketAPI
        from .lp_research import LPResearchService
        from .lp_fomo_flow import FomoFlowService
        from .workbench_actions import ActionService
        from .workbench_market import MarketService

        self.stopping = threading.Event()
        self.origins = frozenset(args.public_origin)
        self.enable_prepare = args.enable_transaction_prepare
        self.assets = startup.assets
        self._resources = ExitStack()
        self._close_lock = threading.Lock()
        try:
            startup.phase("market")
            self.market = MarketService(
                args.rpc_url, data_dir=args.data_dir, external_index=True,
            )
            self._resources.callback(self.market.close)
            startup.phase("store")
            self.lp = LPMarketService(
                self.market, args.rpc_url, args.database,
                history_days=args.history_days,
                history_disk_reserve_bytes=int(args.disk_reserve_gib * 1024**3),
            )
            self._resources.callback(self.lp.close)
            startup.phase("services")
            self.actions = ActionService(self.market.rpc)
            self._resources.callback(self.actions.close)
            self.public = PublicMarketAPI(self.lp)
            self._resources.callback(self.public.close)
            self.research = LPResearchService(self.lp)
            self.flow = FomoFlowService()
            self._resources.callback(self.flow.close)
            startup.phase("gate")
            self.gate = Gate(
                args.gate_db, owner=args.gate_owner, rpc_url=args.gate_rpc_url,
                hosts=frozenset(urlsplit(origin).netloc for origin in self.origins),
            )
            self._resources.callback(self.gate.close)
            startup.phase("tx")
            self.tx, self.tx_unavailable = None, "fee recipient not configured"
            if args.tx_fee_recipient:
                try:
                    rpc = JsonRpc(args.gate_rpc_url)
                    self.tx = TxCore(
                        rpc, RouteBook(self.lp.store.reader_snapshot, rpc),
                        TxPolicy(100, str(args.tx_fee_recipient).lower()),
                        trade_store=TradeStore(args.gate_db.parent / "trades.sqlite"),
                    )
                    self.tx_unavailable = None
                except Exception as exc:
                    self.tx_unavailable = "transaction core failed to start: " + " ".join(str(exc).split())[:160]
                if self.tx is not None and not self.tx.enabled:
                    self.tx_unavailable = "pinned contract code changed; trading disabled"
            startup.phase("tags")
            listener = None
            if os.environ.get("RHP_LISTENER_DSN"):
                try:
                    listener = PostgresListener(os.environ["RHP_LISTENER_DSN"])
                except Exception:
                    listener = None
            self.tags = FlowTagger(
                JsonRpc(args.gate_rpc_url, timeout=15), TagStore(str(args.data_dir / "tags.sqlite")),
                _pool_identity_reader(self.lp.store), listener,
            )
        except BaseException:
            self._resources.close()
            raise

    def close(self) -> None:
        self.stopping.set()
        with self._close_lock:
            self._resources.close()


def _pool_identity_reader(store):
    def lookup(pool_id: str):
        with store.reader_snapshot() as connection:
            row = connection.execute("SELECT id,protocol,hook FROM pools WHERE id=?", (pool_id,)).fetchone()
        return None if row is None else {"id": row[0], "protocol": row[1], "hook": row[2]}
    return lookup


class LPHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True
    # socketserver's default backlog of 5 drops SYNs under any burst; each
    # drop costs the client a 1 s retransmit before the request even arrives.
    request_queue_size = 1024


def _tx_uint(payload: dict, name: str, default: str | None = None) -> int:
    value = payload.get(name, default)
    if isinstance(value, bool) or not isinstance(value, (int, str)) or not str(value).isdigit():
        raise ValueError(f"{name} must be a non-negative integer in raw units")
    return int(value)


def _tx_int(payload: dict, name: str) -> int:
    value = payload.get(name, 0)
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{name} must be an integer")
    return value


def _tx_intent(payload: dict, wallet: str) -> SwapIntent | LpIntent:
    """The wallet always comes from the credential, never from the request body."""
    slippage = _tx_uint(payload, "slippage_bps")
    if not 1 <= slippage <= 5000:
        raise ValueError("slippage_bps must be between 1 and 5000")
    if payload["kind"] == "swap":
        return SwapIntent(
            wallet.lower(), Side(str(payload.get("side"))), str(payload.get("token") or "").lower(),
            str(payload.get("quote_currency") or "").lower(), _tx_uint(payload, "amount_in"), slippage,
        )
    token_id = payload.get("token_id")
    return LpIntent(
        wallet.lower(), LpOp(str(payload.get("op"))), str(payload.get("pool_id") or "").lower(), slippage,
        tick_lower=_tx_int(payload, "tick_lower"), tick_upper=_tx_int(payload, "tick_upper"),
        amount0=_tx_uint(payload, "amount0", "0"), amount1=_tx_uint(payload, "amount1", "0"),
        liquidity=_tx_uint(payload, "liquidity", "0"),
        token_id=None if token_id is None else _tx_uint(payload, "token_id"),
    )


def _quota_headers(quota) -> tuple[tuple[str, str], ...]:
    return (
        ("X-RateLimit-Limit", str(quota.limit)), ("X-RateLimit-Remaining", str(quota.remaining)),
        ("X-RateLimit-Reset", str(int(quota.reset_at))),
    )


class _StreamClosed(Exception):
    pass


class _SseSink:
    def __init__(self, handler: "Handler") -> None:
        self.handler = handler
        self.recheck = None
        self.opened = False

    def open(self) -> None:
        self.write = self.handler._start_event_stream()
        self.opened = True

    def tick(self) -> None:
        if self.recheck is not None:
            try:
                self.recheck()
            except GateRefusal as refusal:
                self.gate(refusal.payload)
                raise _StreamClosed from None

    def event(self, id_: str, name: str | None, body: bytes) -> None:
        head = f"id: {id_}\n" + (f"event: {name}\n" if name else "")
        self.write(head.encode() + b"data: " + body + b"\n\n")

    def heartbeat(self) -> None:
        self.write(b": heartbeat\n\n")

    def gate(self, payload: dict) -> None:
        self.write(b"event: gate\ndata: " + _json_bytes(payload) + b"\n\n")

    def error(self, message: str) -> None:
        try:
            self.write(b"event: error\ndata: " + _json_bytes({"error": message}) + b"\n\n")
        except (BrokenPipeError, ConnectionResetError, TimeoutError, OSError):
            pass

    def finish(self) -> None:
        pass


class _WsSink(_SseSink):
    def open(self) -> None:
        self.push = WebSocketPush(self.handler)
        if self.push.accept() != 101:
            raise _StreamClosed
        self.opened = True

    def tick(self) -> None:
        if not self.push.poll():
            raise _StreamClosed
        super().tick()

    def event(self, id_: str, name: str | None, body: bytes) -> None:
        self.push.send_text('{"event":"%s","id":"%s","data":%s}' % (name or "message", id_, body.decode()))

    def heartbeat(self) -> None:
        self.push.ping()

    def gate(self, payload: dict) -> None:
        self.push.send_text(_json_bytes({"event": "gate", "data": payload}).decode())
        self.push.close(CLOSE_NOT_ENTITLED, "not entitled")

    def error(self, message: str) -> None:
        try:
            self.push.close(1011, message[:100])
        except (BrokenPipeError, ConnectionResetError, TimeoutError, OSError):
            pass

    def finish(self) -> None:
        if self.opened and self.push.open:
            try:
                self.push.close(1000, "bye")
            except (BrokenPipeError, ConnectionResetError, TimeoutError, OSError):
                pass


class Handler(BaseHTTPRequestHandler):
    # `runtime` stays None until initialization finishes; `startup` answers
    # for it meanwhile. Data routes report 503 "starting", never a hang.
    runtime: Runtime | None = None
    startup: Startup | None = None
    server_version = "rhpools"
    sys_version = ""
    # Keep-alive lets cloudflared reuse origin connections instead of paying a
    # handshake and a new thread per poll. Every response carries Content-Length
    # or closes the connection.
    protocol_version = "HTTP/1.1"
    api_slots = lane_slots(DEFAULT_API_SLOTS)
    lp_streams = threading.BoundedSemaphore(512)
    workbench_streams = threading.BoundedSemaphore(128)
    keyed_streams = threading.BoundedSemaphore(128)

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

    def _json(
            self, status: int, payload: object, *,
            retry: int | None = None, key=None, fresh: tuple[int, int] | None = None,
            private: bool = False, headers: tuple[tuple[str, str], ...] = (),
    ) -> None:
        body = _json_bytes(payload, key)
        etag = _etag(key, body) if status == 200 and fresh is not None else None
        if etag is not None and etag in self._client_etags():
            status, body = 304, b""
        compressed = status != 304 and len(body) >= 1024 and self._gzip_ok()
        if compressed:
            build = lambda: zlib.compress(body, level=1, wbits=31)
            body = build() if key is None else _memoized_body(("gzip", *key), build)
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        if private:
            self.send_header("Cache-Control", "private, no-store")
        elif etag is None:
            self.send_header("Cache-Control", "no-store")
        else:
            self.send_header("Cache-Control", f"public, max-age={fresh[0]}, stale-while-revalidate={fresh[1]}")
            self.send_header("ETag", etag)
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Vary", "Accept-Encoding, Authorization, Cookie" if private else "Accept-Encoding")
        for name, value in headers:
            self.send_header(name, value)
        self.send_header("Access-Control-Allow-Origin", "*")
        if compressed:
            self.send_header("Content-Encoding", "gzip")
        if retry is not None:
            self.send_header("Retry-After", str(retry))
        if status != 304:
            self.send_header("Content-Length", str(len(body)))
        if self.close_connection:
            self.send_header("Connection", "close")
        self.end_headers()
        if status != 304 and self.command != "HEAD":
            self.wfile.write(body)

    def _client_etags(self) -> set[str]:
        return {tag.strip() for tag in str(self.headers.get("If-None-Match") or "").split(",")}

    def _asset(self, path: str, head: bool = False) -> bool:
        owner = self.runtime if self.runtime is not None else self.startup
        item = owner.assets.get(path)
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

    def _starting(self) -> None:
        self._json(503, self.startup.snapshot(), retry=15)

    def _bounded(self, route: Route, path: str, query: dict[str, str]) -> None:
        keyed = self.headers.get("Authorization", "")[:11].lower() == "bearer rhp_"
        if keyed:
            try:
                principal, _ = self.runtime.gate.require(self.headers, "api")
                quota = self.runtime.gate.admit(principal)
            except GateRefusal as refusal:
                return self._refuse(refusal)
        slots = self.api_slots["keyed" if keyed else route.lane]
        if not slots.acquire(False):
            self._json(503, {"error": "API request capacity reached"}, retry=1)
            return
        try:
            owner = self if route.resource is None else getattr(self.runtime, route.resource)
            method = getattr(owner, route.method)
            try:
                payload = method() if path == "/api/lp/status" else method(query)
            finally:
                self.runtime.lp.store.close_reader()
            if keyed:
                return self._json(200, payload, private=True, headers=_quota_headers(quota))
            key = _publication_key(path, query, payload) if isinstance(payload, dict) else None
            self._json(200, payload, key=key, fresh=freshness(route, query))
        finally:
            slots.release()

    def _start_event_stream(self):
        compressor = zlib.compressobj(level=1, wbits=31) if self._gzip_ok() else None
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Connection", "close")
        self.send_header("Cache-Control", "no-store,no-transform")
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

    def _lp_stream(self, query, sink):
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
        sink.open()
        frame = None
        last_write = time.monotonic()
        next_durable = float("inf") if current_only or terminal else 0.0
        next_owner = 0.0 if owners_enabled else float("inf")
        owner_revision = None
        seen_rows = {}
        while not self.runtime.stopping.is_set():
            sink.tick()
            if feed["reset"]:
                # Sequence numbers belong to one feed epoch. Keeping a retired
                # high cursor would force snapshot-only replay after a restart.
                block_after = 0
            for item in feed["events"]:
                block_after = max(block_after, int(item["sequence"]))
                feed_epoch = feed["feed_epoch"]
                if channel == "heads" and item["event"] != "block" or channel == "activity" and item["event"] != "activity":
                    continue
                key = ("lp-feed", feed_epoch, int(item["sequence"]), item["event"], () if item["event"] == "block" else view_key)
                body = _json_bytes(item["data"], key)
                sink.event(f"{after}:{feed_epoch}:{block_after}", item["event"], body)
                last_write = time.monotonic()
            if frame is not None:
                current_rows = {str(row["id"]): row for row in frame["rows"]}
                frame["removed"] = [key for key in seen_rows if key not in current_rows]
                frame["snapshot"] = not seen_rows or frame["reset"]
                frame["rows"] = [row for key, row in current_rows.items() if row != seen_rows.get(key)]
                seen_rows = current_rows
                after, epoch = frame["revision"], frame["epoch"]
                sink.event(f"{after}:{feed_epoch or feed['feed_epoch']}:{block_after}", None, _json_bytes(frame))
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
                    sink.event(f"{after}:{feed_epoch or feed['feed_epoch']}:{block_after}", "owners", body)
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
                sink.heartbeat()
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
                self._lp_stream(query, _SseSink(self))
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

    def _refuse(self, refusal: GateRefusal) -> None:
        self._json(refusal.status, refusal.payload, retry=refusal.retry_after, private=True)

    def _client_ip(self) -> str:
        address = self.client_address[0]
        if address in {"127.0.0.1", "::1"}:
            return str(self.headers.get("CF-Connecting-IP") or address)
        return address

    def _me(self, principal: Principal | None) -> dict:
        gate = self.runtime.gate
        policy = gate.policy()
        if principal is None:
            return {"signed_in": False, "policy": policy.public()}
        ent = gate.entitlement(principal.wallet)
        return {
            "signed_in": True, "wallet": principal.wallet, "key_id": principal.key_id, "kind": principal.kind,
            "via": principal.via, "expires_at": principal.expires_at, "state": ent.state(policy),
            **ent.public(), "policy": policy.public(), "owner": gate.is_owner(principal.wallet),
        }

    def _gate_get(self, path: str, query: dict[str, str]) -> None:
        gate = self.runtime.gate
        try:
            if path == "/api/gate/nonce":
                return self._json(200, gate.nonce(query.get("wallet"), client_ip=self._client_ip()), private=True)
            if path == "/api/gate/policy":
                return self._json(200, gate.status(), private=True)
            principal = gate.resolve(self.headers)
            if path == "/api/gate/me":
                return self._json(200, self._me(principal), private=True)
            if principal is None:
                raise GateRefusal(401, "credential required", state="anonymous")
            return self._json(200, {"wallet": principal.wallet, "keys": [record.public() for record in gate.keys(principal.wallet)]}, private=True)
        except GateRefusal as refusal:
            self._refuse(refusal)

    def _keyed_stream(self, query: dict[str, str]) -> None:
        if self.command == "HEAD":
            return self._json(405, {"error": "Use GET for event streams"})
        gate = self.runtime.gate
        try:
            principal, _ = gate.require(self.headers, "api")
        except GateRefusal as refusal:
            return self._refuse(refusal)
        if not self.keyed_streams.acquire(False):
            return self._json(503, {"error": "Live stream capacity reached"}, retry=2)
        websocket = str(self.headers.get("Upgrade") or "").lower() == "websocket"
        sink = None
        try:
            with gate.stream_slot(principal):
                sink = _WsSink(self) if websocket else _SseSink(self)
                sink.recheck = lambda: gate.recheck(principal, "api")
                self._lp_stream(query, sink)
        except GateRefusal as refusal:
            self._refuse(refusal)
        except _StreamClosed:
            pass
        except (BrokenPipeError, ConnectionResetError, TimeoutError, OSError):
            pass
        except ValueError as exc:
            if sink is None or not sink.opened:
                raise
            sink.error(str(exc))
        except Exception:
            if sink is None or not sink.opened:
                raise
            sink.error("Live data is temporarily unavailable")
        finally:
            if sink is not None:
                sink.finish()
            self.runtime.lp.store.close_reader()
            self.keyed_streams.release()
            self.close_connection = True

    def do_GET(self) -> None:
        try:
            path, query = self._query()
        except ValueError as exc:
            return self._json(400, {"error": str(exc)})
        if self._asset(path):
            return
        try:
            route = _ROUTES.get(path)
            if route is None and path not in {"/api/workbench/capabilities", "/api/lp/stream", "/api/workbench/stream"} and path not in _GATE_GET and path != KEYED_STREAM and path not in _TX_GET and path != TAGS_PATH:
                return self._json(404, {"error": "Not found"})
            if self.runtime is None:
                return self._starting()
            if route is not None:
                return self._bounded(route, path, query)
            if path == "/api/workbench/capabilities":
                local = self._loopback()
                return self._json(200, {"allocation_preview": True, "simulate": local, "prepare": local and self.runtime.enable_prepare, "broadcast": False, "server_signing": False})
            if path == "/api/lp/stream":
                return self._sse(False, query)
            if path in _GATE_GET:
                return self._gate_get(path, query)
            if path in _TX_GET:
                return self._tx_get(path, query)
            if path == TAGS_PATH:
                return self._tags(query)
            if path == KEYED_STREAM:
                return self._keyed_stream(query)
            return self._sse(True, query)
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
        if path in _GATE_GET or path in _GATE_POST or path == KEYED_STREAM or path in _TX_GET or path in _TX_POST or path == TAGS_PATH:
            self.send_response(204)
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
            self.send_header("Access-Control-Allow-Headers", "Accept, Content-Type, Authorization")
            self.send_header("Content-Length", "0")
            return self.end_headers()
        if path not in _ROUTES and path not in {"/api/v1/openapi.json", "/api/workbench/capabilities"}:
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

    def _json_body(self) -> dict:
        if self.headers.get("Content-Type", "").split(";", 1)[0].strip() != "application/json":
            raise GateRefusal(415, "Expected application/json")
        size = int(self.headers.get("Content-Length", "0"))
        if not 0 < size <= MAX_BODY:
            raise GateRefusal(413, "Request must be between 1 and 16384 bytes")
        payload = json.loads(self.rfile.read(size))
        if not isinstance(payload, dict):
            raise ValueError("Expected a JSON object")
        return payload

    def _cookie(self, secret: str, max_age: int) -> tuple[str, str]:
        return ("Set-Cookie", f"{COOKIE_NAME}={secret}; Path=/; HttpOnly; Secure; SameSite=Strict; Max-Age={max_age}")

    def _tags(self, query: dict[str, str]) -> None:
        try:
            principal, _ = self.runtime.gate.require(self.headers, "flags")
        except GateRefusal as refusal:
            return self._refuse(refusal)
        hashes = list(dict.fromkeys(h.strip().lower() for h in str(query.get("tx") or "").split(",") if h.strip()))
        if not hashes or len(hashes) > TAGS_MAX_TX or any(len(h) != 66 or not h.startswith("0x") for h in hashes):
            return self._json(400, {"error": f"tx must list 1 to {TAGS_MAX_TX} transaction hashes"}, private=True)
        try:
            quota = self.runtime.gate.admit(principal, cost=len(hashes) // 10 + 1)
        except GateRefusal as refusal:
            return self._refuse(refusal)
        slots = self.api_slots["keyed"]
        if not slots.acquire(False):
            return self._json(503, {"error": "API request capacity reached"}, retry=1)
        try:
            marks = ",".join("?" for _ in hashes)
            with self.runtime.lp.store.reader_snapshot() as connection:
                rows = [
                    {"tx_hash": row[0], "pool_id": row[1], "block_number": row[2], "timestamp": row[3]}
                    for row in connection.execute(
                        "SELECT DISTINCT tx_hash,pool_id,block_number,timestamp FROM events INDEXED BY events_tx_log_idx "
                        f"WHERE tx_hash IN ({marks}) AND pool_id IS NOT NULL", hashes,
                    )
                ]
            tagged: dict[str, dict[str, dict]] = {h: {} for h in hashes}
            for tag in self.runtime.tags.tag(rows):
                tagged.setdefault(tag.tx_hash, {})[tag.pool_id] = {"tags": sorted(tag.tags), "basis": sorted(tag.basis)}
            return self._json(200, {"tags": tagged}, private=True, headers=_quota_headers(quota))
        finally:
            slots.release()

    def _tx_principal(self, feature: str, cost: int = 1) -> tuple[Principal, Entitlement]:
        principal, entitlement = self.runtime.gate.require(self.headers, feature)
        if principal.via == "cookie" and self.command == "POST" and not self._same_origin():
            raise GateRefusal(403, "Configured same-origin request required")
        self.runtime.gate.admit(principal, cost=cost)
        return principal, entitlement

    def _tx_core(self) -> TxCore:
        if self.runtime.tx is None:
            raise TxRefusal("trading_disabled", self.runtime.tx_unavailable or "")
        return self.runtime.tx

    def _tx_failure(self, exc: TxError) -> None:
        if isinstance(exc, TxRefusal):
            return self._json(422, {"refusal": exc.code, "detail": exc.detail}, private=True)
        if exc.code == "rpc":
            return self._json(503, {"error": "chain node unavailable", "code": exc.code}, retry=2, private=True)
        self._json(400, {"error": exc.detail or exc.code, "code": exc.code}, private=True)

    def _tx_get(self, path: str, query: dict[str, str]) -> None:
        tx = self.runtime.tx
        if path == "/api/tx/status":
            policy = self.runtime.gate.policy()
            return self._json(200, {
                "enabled": tx is not None and tx.enabled, "reason": self.runtime.tx_unavailable,
                "base_fee_bps": policy.base_fee_bps,
                "fee_tiers": [tier.public() for tier in policy.fee_tiers],
                "quote_ttl_s": tx.ttl_s if tx is not None else None,
            }, private=True)
        if path == "/api/tx/history" and query.get("feature") != "trade":
            return self._json(400, {"error": "feature must be trade"}, private=True)
        if not self.api_slots["keyed"].acquire(False):
            return self._json(503, {"error": "API request capacity reached"}, retry=1)
        try:
            principal, _ = self._tx_principal("lp" if query.get("feature") == "lp" else "trade", cost=5 if path == "/api/tx/pool" else 1)
            if path == "/api/tx/history" and (principal.kind != "session" or principal.via != "cookie"):
                raise GateRefusal(403, "browser session required", state="forbidden")
            core = self._tx_core()
            if path == "/api/tx/history":
                before = query.get("before")
                if before is not None and (not before.isdecimal() or int(before) <= 0):
                    raise TxError("invalid_intent", "before must be a positive block number")
                return self._json(200, core.history(principal.wallet, int(before) if before else None), private=True)
            if path == "/api/tx/balances":
                currencies = [c for c in str(query.get("currencies") or "").split(",") if c]
                return self._json(200, {"wallet": principal.wallet, "balances": core.balances(principal.wallet, currencies)}, private=True)
            if path == "/api/tx/pool":
                known = tuple(int(i) for i in str(query.get("ids") or "").split(",") if i.isdigit())
                return self._json(200, core.pool_view(str(query.get("pool_id") or ""), principal.wallet, known), private=True)
            return self._json(200, core.receipt(str(query.get("hash") or ""), principal.wallet).to_json(), private=True)
        except GateRefusal as refusal:
            self._refuse(refusal)
        except TxError as exc:
            self._tx_failure(exc)
        finally:
            self.api_slots["keyed"].release()

    def _tx_post(self, path: str) -> None:
        if not self.api_slots["keyed"].acquire(False):
            return self._json(503, {"error": "API request capacity reached"}, retry=1)
        try:
            payload = self._json_body()
            if path == "/api/tx/quote":
                kind = payload.get("kind")
                if kind not in ("swap", "lp"):
                    raise ValueError("kind must be swap or lp")
                principal, entitlement = self._tx_principal("trade" if kind == "swap" else "lp", cost=10)
                core = self._tx_core()
                request_policy = replace(core.policy, fee_bps=entitlement.fee_bps)
                return self._json(200, core.quote(_tx_intent(payload, principal.wallet), policy=request_policy).to_json(), private=True)
            core = self._tx_core()
            quote_id = str(payload.get("quote_id") or "")
            principal, _ = self._tx_principal("lp" if core.kind_of(quote_id) == "lp" else "trade", cost=5)
            signature = str(payload.get("permit_signature") or "")
            sigs = Signatures(permit=bytes.fromhex(signature[2:]) if signature.startswith("0x") else None)
            return self._json(200, core.prepare(quote_id, principal.wallet, sigs, batched=payload.get("batched") is True).to_json(), private=True)
        except GateRefusal as refusal:
            self._refuse(refusal)
        except TxError as exc:
            self._tx_failure(exc)
        except (ValueError, KeyError, TypeError) as exc:
            self._json(400, {"error": str(exc)}, private=True)
        except (BrokenPipeError, ConnectionResetError, TimeoutError):
            return
        except Exception:
            self._json(503, {"error": "Upstream data is temporarily unavailable"}, retry=2, private=True)
        finally:
            self.api_slots["keyed"].release()

    def _gate_post(self, path: str) -> None:
        gate = self.runtime.gate
        policy_post = path == "/api/gate/policy"
        slot = gate._policy_slots if policy_post else self.api_slots["keyed"]
        try:
            if policy_post:
                gate.policy_admit(self._client_ip())
                if self.headers.get("Content-Type", "").split(";", 1)[0].strip() != "application/json":
                    raise GateRefusal(415, "Expected application/json")
                size = int(self.headers.get("Content-Length", "0"))
                if not 0 < size <= 2048:
                    raise GateRefusal(413, "Policy request must be between 1 and 2048 bytes")
            if path == "/api/gate/session" and self.headers.get("Origin") is not None and not self._same_origin():
                raise GateRefusal(403, "Configured same-origin request required")
        except GateRefusal as refusal:
            return self._refuse(refusal)
        except (ValueError, KeyError, TypeError) as exc:
            return self._json(400, {"error": str(exc)}, private=True)
        if not slot.acquire(False):
            return self._json(503, {"error": "API request capacity reached"}, retry=1)
        try:
            payload = self._json_body()
            if policy_post:
                applied = gate.apply_policy(GatePolicy.parse(payload.get("policy")), str(payload.get("signature") or ""), via="web")
                return self._json(200, {"applied": applied.public(), **gate.status()}, private=True)
            if path == "/api/gate/session":
                principal, secret = gate.sign_in(
                    str(payload.get("message") or ""), str(payload.get("signature") or ""),
                    label=str(payload.get("label") or "browser"), client_ip=self._client_ip(),
                    host=str(self.headers.get("Host") or "") if self._loopback() else None,
                )
                return self._json(200, self._me(principal), private=True, headers=(self._cookie(secret, gate.limits.session_ttl_s),))
            principal = gate.resolve(self.headers)
            if principal is None:
                raise GateRefusal(401, "credential required", state="anonymous")
            if principal.via == "cookie" and not self._same_origin():
                raise GateRefusal(403, "Configured same-origin request required")
            if path == "/api/gate/logout":
                gate.revoke(principal.key_id, wallet=principal.wallet)
                return self._json(200, {"signed_in": False}, private=True, headers=(self._cookie("", 0),))
            op = payload.get("op")
            if op == "mint":
                minted, secret = gate.mint_key(
                    principal, label=str(payload.get("label") or "key"), ttl_s=int(payload.get("ttl_s") or _MINT_TTL_DEFAULT_S),
                )
                return self._json(200, {"key_id": minted.key_id, "secret": secret, "label": minted.label, "expires_at": minted.expires_at}, private=True)
            if op == "revoke":
                return self._json(200, {"revoked": gate.revoke(str(payload.get("key_id") or ""), wallet=principal.wallet)}, private=True)
            if op == "revoke_all":
                return self._json(200, {"revoked": gate.revoke_all(principal.wallet)}, private=True)
            raise ValueError("op must be mint, revoke or revoke_all")
        except GateRefusal as refusal:
            self._refuse(refusal)
        except (ValueError, KeyError, TypeError) as exc:
            self._json(400, {"error": str(exc)}, private=True)
        except (BrokenPipeError, ConnectionResetError, TimeoutError):
            return
        except Exception:
            self._json(503, {"error": "Upstream data is temporarily unavailable"}, retry=2, private=True)
        finally:
            slot.release()

    def do_POST(self) -> None:
        # A rejected POST leaves its body unread; closing keeps it out of the next request.
        self.close_connection = True
        try:
            path, _query = self._query()
        except ValueError as exc:
            return self._json(400, {"error": str(exc)})
        if path in _GATE_POST:
            if self.runtime is None:
                return self._starting()
            return self._gate_post(path)
        if path in _TX_POST:
            if self.runtime is None:
                return self._starting()
            return self._tx_post(path)
        if path not in {"/api/lp/allocation", "/api/workbench/simulate", "/api/workbench/prepare"}:
            return self._json(404, {"error": "Not found"})
        if self.runtime is None:
            return self._starting()
        if not self._same_origin():
            return self._json(403, {"error": "Configured same-origin request required"})
        if path != "/api/lp/allocation" and not self._loopback():
            return self._json(403, {"error": "Simulation and preparation require loopback access"})
        if path.endswith("/prepare") and not self.runtime.enable_prepare:
            return self._json(403, {"error": "Transaction preparation is disabled"})
        if self.headers.get("Content-Type", "").split(";", 1)[0].strip() != "application/json":
            return self._json(415, {"error": "Expected application/json"})
        if not self.api_slots["slow"].acquire(False):
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
            self.api_slots["slow"].release()

    def do_HEAD(self) -> None:
        self.do_GET()

    def finish(self) -> None:
        try:
            super().finish()
        finally:
            if self.runtime is not None:
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
    ap.add_argument("--api-slots", type=int, default=int(os.environ.get("RHP_API_SLOTS", DEFAULT_API_SLOTS)))
    ap.add_argument("--gate-owner", default=os.environ.get("RHP_GATE_OWNER") or None)
    ap.add_argument("--gate-db", default=os.environ.get("RHP_GATE_DB"))
    ap.add_argument("--gate-rpc-url", default=os.environ.get("RHP_GATE_RPC_URL", "http://127.0.0.1:8547"))
    ap.add_argument("--tx-fee-recipient", default=os.environ.get("RHP_TX_FEE_RECIPIENT") or None)
    return ap


def main() -> None:
    args = parser().parse_args()
    # Reduce SQLite I/O handoff delays behind CPU-heavy valuation threads.
    sys.setswitchinterval(min(sys.getswitchinterval(), 0.001))
    data_dir = Path(args.data_dir).expanduser()
    data_dir.mkdir(parents=True, exist_ok=True)
    args.data_dir = data_dir
    args.database = Path(args.database).expanduser() if args.database else data_dir / "lp_market.sqlite"
    args.gate_db = Path(args.gate_db).expanduser() if args.gate_db else data_dir / "gate.sqlite"
    startup = Startup(_load_assets())
    Handler.startup = startup
    Handler.api_slots = lane_slots(args.api_slots)
    server = LPHTTPServer((args.host, args.port), Handler)
    stopping = threading.Event()

    def halt(_signum=None, _frame=None):
        stopping.set()
    signal.signal(signal.SIGINT, halt)
    signal.signal(signal.SIGTERM, halt)
    # The socket is served from the first moment so probes learn "starting"
    # instead of queueing in the backlog; initialization keeps the main
    # thread, where a stop signal lands as soon as the store returns.
    threading.Thread(target=server.serve_forever, name="lp-http", daemon=True).start()
    print(f"RobinhoodPools listening on http://{args.host}:{args.port} (starting)", flush=True)
    runtime = None
    try:
        runtime = Runtime(args, startup)
        if not stopping.is_set():
            Handler.runtime = runtime
            startup.phase("ready")
            stopping.wait()
    finally:
        if runtime is not None:
            runtime.stopping.set()
        server.shutdown()
        server.server_close()
        if runtime is not None:
            runtime.close()


if __name__ == "__main__":
    main()
