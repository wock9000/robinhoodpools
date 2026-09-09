"""Capability-routed, bounded JSON-RPC clients for LP read and index lanes."""
from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass, field
import json
import os
from pathlib import Path
import re
import stat
import threading
import time
from typing import Any, Callable, Iterable, Mapping, Sequence
from urllib.parse import parse_qsl, unquote, urlsplit

import requests
from requests.adapters import HTTPAdapter

from .lp_chain import CHAIN_ID

MAX_RPC_RESPONSE_BYTES = 64 * 1024 * 1024
MAX_BATCH_CALLS = 100
MAX_SOURCE_CONCURRENCY = 4
_PUBLIC = "https://rpc.mainnet.chain.robinhood.com"
_PUBLICNODE = "https://robinhood-rpc.publicnode.com"
_ARROW = "https://rpc.arrowrpc.com"
_ORDO = "https://rpc.ordofi.network"
_EXPLORER = "https://robinhoodchain.blockscout.com/api"
_LOCAL_HOSTS = frozenset({"127.0.0.1", "localhost", "::1"})
_URL_RE = re.compile(r"https?://[^\s,'\"}]+")


@dataclass(frozen=True, slots=True)
class _Source:
    name: str
    url: str
    headers: tuple[tuple[str, str], ...] = ()
_EXPLORER_SOURCE = _Source("blockscout-explorer", _EXPLORER)



@dataclass(slots=True)
class _SourceState:
    failures: int = 0
    consecutive_failures: int = 0
    retry_at: float = 0.0
    last_success_at: float | None = None
    last_failure_at: float | None = None
    last_latency_ms: int | None = None
    last_error: str | None = None
    methods: set[str] = field(default_factory=set)
@dataclass(slots=True)
class _SourceGate:
    lock: threading.Lock = field(default_factory=threading.Lock)
    traffic_lock: threading.Lock = field(default_factory=threading.Lock)
    next_at: float = 0.0
    slots: threading.BoundedSemaphore = field(
        default_factory=lambda: threading.BoundedSemaphore(MAX_SOURCE_CONCURRENCY),
    )
    started_at: float = field(default_factory=time.monotonic)
    http_requests: int = 0
    rpc_calls: int = 0
    seconds: list[int] = field(default_factory=lambda: [-1] * 60)
    http_window: list[int] = field(default_factory=lambda: [0] * 60)
    calls_window: list[int] = field(default_factory=lambda: [0] * 60)

    def record(self, calls: int) -> None:
        """Count attempted HTTP requests and batch items, including failures."""
        second = int(time.monotonic())
        slot = second % 60
        with self.traffic_lock:
            if self.seconds[slot] != second:
                self.seconds[slot] = second
                self.http_window[slot] = 0
                self.calls_window[slot] = 0
            self.http_window[slot] += 1
            self.calls_window[slot] += calls
            self.http_requests += 1
            self.rpc_calls += calls

    def traffic(self) -> dict[str, Any]:
        now = time.monotonic()
        with self.traffic_lock:
            active = [i for i, second in enumerate(self.seconds) if int(now) - 60 < second <= int(now)]
            window = min(60.0, max(1.0, now - self.started_at))
            return {
                "scope": "endpoint shared across capabilities",
                "window_seconds": round(window, 1),
                "http_requests": self.http_requests,
                "rpc_calls": self.rpc_calls,
                "http_rps": round(sum(self.http_window[i] for i in active) / window, 3),
                "rpc_calls_per_second": round(sum(self.calls_window[i] for i in active) / window, 3),
            }


_GATES_LOCK = threading.Lock()
_GATES: dict[str, _SourceGate] = {}


def _gate(url: str) -> _SourceGate:
    with _GATES_LOCK:
        gate = _GATES.get(url)
        if gate is None:
            gate = _SourceGate()
            _GATES[url] = gate
        return gate


def _minimum_interval(url: str) -> float:
    host = (urlsplit(url).hostname or "").lower()
    if host in _LOCAL_HOSTS:
        return 0.0
    if host == "edge.goldsky.com":
        return 1 / 80
    if host == "rpc.mainnet.chain.robinhood.com":
        return 0.25
    if host == "rpc.ordofi.network":
        return 0.20
    return 0.10




def _split_urls(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def _valid_url(url: str) -> bool:
    parsed = urlsplit(url)
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def _local(url: str) -> bool:
    return (urlsplit(url).hostname or "").lower() in _LOCAL_HOSTS


def _configured_alchemy() -> str | None:
    """Use only an explicitly configured key; never discover private env files."""
    key = os.environ.get("ALCHEMY_KEY", "").strip()
    return f"https://robinhood-mainnet.g.alchemy.com/v2/{key}" if key else None


def _file_urls(variable: str) -> list[str]:
    """Load operator-owned credentials without exposing them in process arguments."""
    urls: list[str] = []
    for filename in _split_urls(os.environ.get(variable, "")):
        try:
            with Path(filename).expanduser().open("rb") as handle:
                info = os.fstat(handle.fileno())
                if not stat.S_ISREG(info.st_mode) or info.st_mode & 0o077 or info.st_uid != os.getuid():
                    raise ValueError(f"{variable} requires owner-only regular files")
                raw = handle.read(8193)
            if len(raw) > 8192:
                raise ValueError(f"{variable} credential file exceeds 8192 bytes")
            values = [line.strip() for line in raw.decode("utf-8").splitlines() if line.strip()]
            if not values or any(not _valid_url(url) for url in values):
                raise ValueError(f"{variable} must contain HTTP(S) URLs, one per line")
            urls.extend(values)
        except (OSError, UnicodeError, ValueError) as exc:
            raise ValueError(f"Unable to load secure RPC configuration from {variable}") from None
    return urls


def _source_list(primary_url: str, capability: str) -> tuple[_Source, ...]:
    env_name = {
        "head": "LP_RPC_HEAD_URLS",
        "state": "LP_RPC_STATE_URLS",
        "history_state": "LP_RPC_HISTORY_STATE_URLS",
        "logs": "LP_RPC_LOG_URLS",
        "receipts": "LP_RPC_RECEIPT_URLS",
        "trace": "LP_RPC_TRACE_URLS",
    }[capability]
    explicit = _split_urls(os.environ.get(env_name, ""))
    explicit.extend(_file_urls(env_name.removesuffix("URLS") + "URL_FILES"))
    generic = _split_urls(os.environ.get("RHP_RPC_URLS", ""))
    generic.extend(_file_urls("RHP_RPC_URL_FILES"))
    single = os.environ.get("RHP_RPC_URL", "").strip()
    if single:
        generic.insert(0, single)
    disable_local = os.environ.get(
        "LP_RPC_DISABLE_LOCAL_FALLBACK", ""
    ).strip().lower() in {"1", "true", "yes", "on"}
    disable_trace = os.environ.get(
        "LP_RPC_DISABLE_TRACE", ""
    ).strip().lower() in {"1", "true", "yes", "on"}
    disable_alchemy = os.environ.get(
        "LP_RPC_DISABLE_ALCHEMY", ""
    ).strip().lower() in {"1", "true", "yes", "on"}
    alchemy = (
        None
        if disable_alchemy or (disable_trace and capability == "trace")
        else _configured_alchemy()
    )
    sources: list[_Source] = []
    ordo_headers = {}
    ordo_key = os.environ.get("ORDO_API_KEY", "").strip()
    if ordo_key:
        ordo_headers["x-api-key"] = ordo_key
    publicnode_headers = {}
    publicnode_token = os.environ.get("PUBLICNODE_API_TOKEN", "").strip()
    if publicnode_token:
        publicnode_headers["Authorization"] = "Bearer " + publicnode_token

    def add(name: str, url: str, headers: Mapping[str, str] | None = None) -> None:
        if not _valid_url(url) or any(item.url == url for item in sources):
            return
        selected_headers = dict(headers or {})
        if url.rstrip("/") == _ORDO and ordo_key:
            selected_headers["x-api-key"] = ordo_key
        if url.rstrip("/") == _PUBLICNODE and publicnode_token:
            selected_headers["Authorization"] = "Bearer " + publicnode_token
        sources.append(_Source(name, url, tuple(selected_headers.items())))
    if capability == "trace" and disable_trace:
        return ()

    for index, url in enumerate(explicit, 1):
        add(f"configured-{capability}-{index}", url)
    if alchemy:
        add("configured-alchemy", alchemy)

    if capability == "trace":
        if ordo_key:
            add("ordo-authenticated", _ORDO, ordo_headers)
        return tuple(sources)

    for index, url in enumerate(generic, 1):
        if not _local(url):
            add(f"configured-rpc-{index}", url)
    if primary_url and not _local(primary_url):
        add("command-line-primary", primary_url)

    if capability == "head":
        defaults = (("official", _PUBLIC, {}), ("publicnode", _PUBLICNODE, publicnode_headers),
                    ("ordo", _ORDO, ordo_headers), ("arrow", _ARROW, {}))
    elif capability == "state":
        defaults = (("official", _PUBLIC, {}), ("publicnode", _PUBLICNODE, publicnode_headers),
                    ("ordo", _ORDO, ordo_headers), ("arrow", _ARROW, {}))
    elif capability == "history_state":
        defaults = (("ordo", _ORDO, ordo_headers), ("publicnode", _PUBLICNODE, publicnode_headers),
                    ("official", _PUBLIC, {}), ("arrow", _ARROW, {}))
    elif capability == "logs":
        defaults = (("ordo", _ORDO, ordo_headers), ("official", _PUBLIC, {}),
                    ("publicnode", _PUBLICNODE, publicnode_headers), ("arrow", _ARROW, {}))
    else:
        defaults = (("publicnode", _PUBLICNODE, publicnode_headers), ("ordo", _ORDO, ordo_headers),
                    ("official", _PUBLIC, {}), ("arrow", _ARROW, {}))
    for name, url, headers in defaults:
        add(name, url, headers)
    if not disable_local:
        for index, url in enumerate(generic, 1):
            if _local(url):
                add(f"local-fallback-{index}", url)
        if primary_url and _local(primary_url):
            add("local-fallback", primary_url)
    return tuple(sources)


def _block_tag(method: str, params: Sequence[Any]) -> Any:
    if method == "eth_call" and len(params) > 1:
        return params[1]
    if method in {"eth_getCode", "eth_getBalance", "eth_getStorageAt"} and params:
        return params[-1]
    return None


def _capability(method: str, params: Sequence[Any], lane: str) -> str:
    if method.startswith("debug_") or method.startswith("trace_") or method.startswith("arbtrace_"):
        return "trace"
    if method == "eth_getLogs":
        return "logs"
    if method in {
        "eth_getTransactionReceipt", "eth_getTransactionByHash",
        "eth_getTransactionByBlockNumberAndIndex", "eth_getBlockTransactionCountByNumber",
    }:
        return "receipts"
    state_method = method in {
        "eth_call", "eth_getCode", "eth_getBalance", "eth_getStorageAt",
    }
    tag = _block_tag(method, params)
    if (
        state_method
        and tag not in (None, "latest", "pending", "safe", "finalized")
        and lane in {"history", "backfill", "maintenance", "enrichment"}
    ):
        return "history_state"
    if state_method:
        return "state"
    return "head"


class _Registry:
    def __init__(self, primary_url: str, error_type: type[Exception]) -> None:
        self.error_type = error_type
        self.sources = {
            capability: _source_list(primary_url, capability)
            for capability in ("head", "state", "history_state", "logs", "receipts", "trace")
        }
        self._states = {
            (capability, source.name): _SourceState()
            for capability, sources in self.sources.items() for source in sources
        }
        self._states[("logs", _EXPLORER_SOURCE.name)] = _SourceState()
        self._verified_sources: set[str] = set()
        self._clients: dict[str, requests.Session] = {}
        self._active: dict[str, str] = {}
        self._lock = threading.RLock()
        self._closed = False

    def _session(self, source: _Source) -> requests.Session:
        with self._lock:
            if self._closed:
                raise self.error_type("RPC provider registry is closed")
            session = self._clients.get(source.name)
            if session is None:
                session = requests.Session()
                session.headers.update({
                    "Content-Type": "application/json",
                    "User-Agent": "deepstate-lp-provider/1",
                    **dict(source.headers),
                })
                adapter = HTTPAdapter(
                    pool_connections=2, pool_maxsize=MAX_SOURCE_CONCURRENCY, max_retries=0, pool_block=True,
                )
                session.mount("http://", adapter)
                session.mount("https://", adapter)
                self._clients[source.name] = session
            return session

    def candidates(self, capability: str) -> tuple[_Source, ...]:
        sources = self.sources[capability]
        now = time.monotonic()
        with self._lock:
            return tuple(
                source for source in sources
                if self._states[(capability, source.name)].retry_at <= now
            )

    def can_try(self, source: _Source, capability: str) -> bool:
        with self._lock:
            return self._states[(capability, source.name)].retry_at <= time.monotonic()

    @staticmethod
    def _safe_error(source: _Source, exc: BaseException) -> str:
        text = str(exc).replace(source.url, source.name)
        parsed = urlsplit(source.url)
        values = [value for _key, value in parse_qsl(parsed.query)]
        values.extend(value for _key, value in source.headers)
        if parsed.password:
            values.append(parsed.password)
        # Path-key providers (e.g. /v2/<key>) and query-key providers must
        # remain redacted even when a remote error echoes just the key.
        if "/v2/" in parsed.path:
            values.append(unquote(parsed.path.split("/v2/", 1)[1]))
        for value in values:
            if len(value) >= 4:
                text = text.replace(value, "<redacted>")
                if value.startswith("Bearer "):
                    text = text.replace(value[7:], "<redacted>")
        text = _URL_RE.sub("<redacted-url>", text)
        return f"{type(exc).__name__}: {text}"[:500]

    def success(
        self, source: _Source, capability: str, method: str, latency: float,
    ) -> None:
        with self._lock:
            state = self._states[(capability, source.name)]
            state.consecutive_failures = 0
            state.retry_at = 0.0
            state.last_success_at = time.time()
            state.last_latency_ms = round(latency * 1000)
            state.last_error = None
            state.methods.add(method)
            self._active[capability] = source.name

    def failure(self, source: _Source, capability: str, exc: BaseException) -> None:
        with self._lock:
            state = self._states[(capability, source.name)]
            state.failures += 1
            state.consecutive_failures += 1
            state.last_failure_at = time.time()
            state.last_error = self._safe_error(source, exc)
            state.retry_at = time.monotonic() + min(
                60.0, 0.5 * (2 ** min(state.consecutive_failures - 1, 7)),
            )
            if self._active.get(capability) == source.name:
                self._active.pop(capability, None)

    def verified(self, source: _Source) -> bool:
        with self._lock:
            return source.url in self._verified_sources

    def mark_verified(self, source: _Source) -> None:
        with self._lock:
            self._verified_sources.add(source.url)

    def status(self) -> dict[str, Any]:
        now = time.time()
        with self._lock:
            state_rows = {
                key: {
                    "state": (
                        "available"
                        if state.last_success_at is not None and not state.last_error
                        else ("failed" if state.last_error else "untried")
                    ),
                    "failures": state.failures,
                    "last_success_age_s": (
                        round(max(0.0, now - state.last_success_at), 1)
                        if state.last_success_at is not None else None
                    ),
                    "last_latency_ms": state.last_latency_ms,
                    "last_error": state.last_error,
                    "methods": sorted(state.methods),
                }
                for key, state in self._states.items()
            }
            active = dict(self._active)
        result = {}
        for capability, sources in self.sources.items():
            visible = sources + (
                (_EXPLORER_SOURCE,) if capability == "logs" else ()
            )
            result[capability] = {
                "active": active.get(capability),
                "sources": [
                    {"name": source.name, **state_rows[(capability, source.name)],
                     "traffic": _gate(source.url).traffic()}
                    for source in visible
                ],
                "configured": bool(visible),
            }
        return result

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            sessions = list(self._clients.values())
            self._clients.clear()
        for session in sessions:
            session.close()


class _WssRpc:
    """Small persistent WSS pool for latency-sensitive current-state RPC."""

    def __init__(self, urls: Sequence[str], error_type: type[Exception], *, size: int = 2) -> None:
        self._urls = tuple(dict.fromkeys(str(url) for url in urls if url))
        self._error_type = error_type
        self._states: list[dict[str, Any] | None] = [None] * max(1, size)
        self._slot_locks = [threading.Lock() for _ in self._states]
        self._lock = threading.Lock()
        self._next_slot = 0
        self._next_url = 0
        self._closed = False

    def _error(self, message: str) -> Exception:
        return self._error_type(message)

    @staticmethod
    def _close_state(state: Mapping[str, Any] | None) -> None:
        if state is None:
            return
        try:
            state["websocket"].close()
        except Exception:
            pass

    def _request(self, state: dict[str, Any], method: str, params: Sequence[Any]) -> Any:
        state["request_id"] += 1
        request_id = state["request_id"]
        websocket = state["websocket"]
        websocket.send(json.dumps({
            "jsonrpc": "2.0", "id": request_id, "method": method,
            "params": list(params),
        }, separators=(",", ":")))
        deadline = time.monotonic() + 4.0
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise self._error(f"WSS {method} response timeout")
            response = json.loads(websocket.recv(timeout=remaining))
            if not isinstance(response, Mapping) or response.get("id") != request_id:
                continue
            error = response.get("error")
            if error is not None:
                raise self._error(f"WSS {method}: {str(error)[:300]}")
            if "result" not in response:
                raise self._error(f"WSS {method} response omitted result")
            return response["result"]


    def _connect(self, excluded: set[str] | None = None) -> dict[str, Any]:
        from websockets.sync.client import connect

        excluded = excluded or set()
        with self._lock:
            if self._closed:
                raise self._error("WSS RPC client is closed")
            url = None
            for _candidate in range(len(self._urls)):
                candidate = self._urls[self._next_url % len(self._urls)]
                self._next_url += 1
                if candidate not in excluded:
                    url = candidate
                    break
            if url is None:
                raise self._error("WSS RPC sources already exhausted")
        websocket = connect(
            url, open_timeout=5, ping_interval=15, ping_timeout=10,
            close_timeout=2, max_queue=256, max_size=MAX_RPC_RESPONSE_BYTES,
        )
        state = {"websocket": websocket, "request_id": 0, "url": url}
        try:
            chain_id = self._request(state, "eth_chainId", ())
            if int(str(chain_id), 16) != CHAIN_ID:
                raise self._error("WSS RPC returned the wrong chain")
        except BaseException:
            self._close_state(state)
            raise
        return state

    def _slot(self) -> int:
        with self._lock:
            if self._closed:
                raise self._error("WSS RPC client is closed")
            selected = self._next_slot % len(self._states)
            self._next_slot += 1
            return selected

    def _run(self, operation: Callable[[dict[str, Any]], Any]) -> Any:
        if not self._urls:
            raise self._error("WSS RPC has no configured source")
        slot = self._slot()
        failures: list[str] = []
        attempted: set[str] = set()
        with self._slot_locks[slot]:
            for _attempt in range(len(self._urls)):
                state = self._states[slot]
                try:
                    if state is None:
                        state = self._connect(attempted)
                        self._states[slot] = state
                    return operation(state)
                except Exception as exc:
                    failures.append(str(exc))
                    if state is not None and state.get("url"):
                        attempted.add(str(state["url"]))
                    self._close_state(state)
                    self._states[slot] = None
            raise self._error("WSS RPC exhausted: " + "; ".join(failures))

    def call(self, method: str, params: Sequence[Any] | None = None) -> Any:
        return self._run(lambda state: self._request(state, method, list(params or ())))


    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
        for lock, state in zip(self._slot_locks, self._states):
            with lock:
                self._close_state(state)
        self._states = [None] * len(self._states)


class _WssPreferredRpc:
    """Use WSS for single calls and the capability router for bulk RPC."""

    def __init__(self, wss: _WssRpc, fallback: Any) -> None:
        self._wss = wss
        self._fallback = fallback

    def call(self, method: str, params: Sequence[Any] | None = None) -> Any:
        try:
            return self._wss.call(method, params)
        except Exception:
            return self._fallback.call(method, params)

    def batch(self, calls: Iterable[tuple[str, Sequence[Any]]]) -> list[Any]:
        # Bulk snapshots must use capability-specific providers and their
        # shared concurrency/rate budgets, not the public head-subscription
        # socket that is optimized for individual latency-sensitive calls.
        return self._fallback.batch(calls)

    def status(self) -> dict[str, Any]:
        return self._fallback.status()

    def close(self) -> None:
        self._wss.close()
        self._fallback.close()


class _ExecutionReverted(Exception):
    """A valid EVM outcome, not a provider's inability to serve the request."""

    def __init__(self, error: Exception, method: str) -> None:
        super().__init__(str(error))
        self.error = error
        self.method = method


class RoutedRpc:
    """One lane view over a shared capability/source registry."""

    def __init__(self, registry: _Registry, lane: str, timeout: float) -> None:
        self._registry = registry
        self._lane = lane
        self._timeout = timeout
        self._request_id = 0
        self._lock = threading.Lock()
        self._closed = False

    def _error(self, message: str, *, code: int | None = None) -> Exception:
        try:
            return self._registry.error_type(message, code=code)
        except TypeError:
            return self._registry.error_type(message)

    def _post(self, source: _Source, payload: Any) -> Any:
        response: requests.Response | None = None
        gate = _gate(source.url)
        with gate.slots:
            with gate.lock:
                delay = gate.next_at - time.monotonic()
                if delay > 0:
                    time.sleep(delay)
                calls = len(payload) if isinstance(payload, list) else 1
                cost = calls if (urlsplit(source.url).hostname or "").lower() == "edge.goldsky.com" else 1
                gate.next_at = time.monotonic() + _minimum_interval(source.url) * cost
                gate.record(calls)
            try:
                response = self._registry._session(source).post(
                    source.url, data=json.dumps(payload, separators=(",", ":")),
                    timeout=(2.0, self._timeout), stream=True,
                )
                response.raise_for_status()
                raw = response.raw.read(MAX_RPC_RESPONSE_BYTES + 1, decode_content=True)
            except requests.RequestException as exc:
                raise self._error(
                    f"{source.name} transport: {type(exc).__name__}"
                ) from None
            finally:
                if response is not None:
                    response.close()
        if len(raw) > MAX_RPC_RESPONSE_BYTES:
            raise self._error(f"{source.name} response exceeds {MAX_RPC_RESPONSE_BYTES} bytes")
        try:
            return json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise self._error(f"{source.name} returned invalid JSON") from None

    def _validate_item(self, source: _Source, item: Any, request_id: int, method: str) -> Any:
        if not isinstance(item, Mapping) or item.get("id") != request_id:
            raise self._error(f"{source.name} returned malformed {method} response")
        error = item.get("error")
        if error is not None:
            code = error.get("code") if isinstance(error, Mapping) else None
            message = error.get("message") if isinstance(error, Mapping) else error
            failure = self._error(
                f"{source.name} {method}: {self._registry._safe_error(source, RuntimeError(str(message)))}",
                code=code if isinstance(code, int) else None,
            )
            if method in {"eth_call", "eth_estimateGas"} and (
                code == 3
                or (
                    code in {-32000, -32015}
                    and str(message).lower().startswith("execution reverted")
                )
            ):
                raise _ExecutionReverted(failure, method)
            raise failure
        if "result" not in item:
            raise self._error(f"{source.name} {method} response omitted result")
        return item["result"]

    def _ensure_chain(self, source: _Source) -> None:
        if self._registry.verified(source):
            return
        self._request_id += 1
        request_id = self._request_id
        raw = self._post(source, {
            "jsonrpc": "2.0", "id": request_id, "method": "eth_chainId", "params": [],
        })
        result = self._validate_item(source, raw, request_id, "eth_chainId")
        try:
            chain_id = int(str(result), 16)
        except (TypeError, ValueError) as exc:
            raise self._error(f"{source.name} returned malformed chain id") from None
        if chain_id != CHAIN_ID:
            raise self._error(f"{source.name} is chain {chain_id}, expected {CHAIN_ID}")
        self._registry.mark_verified(source)

    @staticmethod
    def _log_matches(query: Mapping[str, Any], row: Mapping[str, Any]) -> bool:
        addresses = query.get("address")
        allowed_addresses = (
            {str(value).lower() for value in addresses}
            if isinstance(addresses, list)
            else ({str(addresses).lower()} if addresses else None)
        )
        if allowed_addresses is not None and str(row.get("address") or "").lower() not in allowed_addresses:
            return False
        actual_topics = row.get("topics")
        if not isinstance(actual_topics, list):
            return False
        for index, expected in enumerate(query.get("topics") or ()):
            if expected is None:
                continue
            options = expected if isinstance(expected, list) else [expected]
            if index >= len(actual_topics) or str(actual_topics[index] or "").lower() not in {
                str(value).lower() for value in options
            }:
                return False
        return True

    def _explorer_logs(self, values: Sequence[Any]) -> list[dict[str, Any]]:
        if len(values) != 1 or not isinstance(values[0], Mapping):
            raise self._error("blockscout explorer requires one log filter")
        query = values[0]
        try:
            start = int(str(query["fromBlock"]), 16)
            end = int(str(query["toBlock"]), 16)
        except (KeyError, TypeError, ValueError) as exc:
            raise self._error("blockscout explorer requires explicit block bounds") from exc
        if start < 0 or end < start or end - start > 2047:
            raise self._error("blockscout explorer log range exceeds 2048 blocks")
        raw_addresses = query.get("address")
        addresses = raw_addresses if isinstance(raw_addresses, list) else [raw_addresses]
        addresses = [str(value).lower() for value in addresses if value]
        if not addresses:
            addresses = [None]
        topic_filter = (query.get("topics") or [None])[0]
        topics = topic_filter if isinstance(topic_filter, list) else [topic_filter]
        topics = [str(value).lower() for value in topics if value]
        if not topics:
            topics = [None]
        if len(addresses) * len(topics) > 16:
            raise self._error("blockscout explorer log filter exceeds 16 bounded queries")
        result: dict[tuple[str, str, str], dict[str, Any]] = {}
        session = self._registry._session(_EXPLORER_SOURCE)
        gate = _gate(_EXPLORER)
        for address in addresses:
            for topic in topics:
                params: dict[str, Any] = {
                    "module": "logs", "action": "getLogs",
                    "fromBlock": start, "toBlock": end,
                }
                if address:
                    params["address"] = address
                if topic:
                    params["topic0"] = topic
                for index, expected in enumerate((query.get("topics") or ())[1:], 1):
                    if isinstance(expected, str):
                        params[f"topic{index}"] = expected
                response: requests.Response | None = None
                with gate.lock:
                    delay = gate.next_at - time.monotonic()
                    if delay > 0:
                        time.sleep(delay)
                    try:
                        response = session.get(
                            _EXPLORER, params=params,
                            timeout=(2.0, self._timeout), stream=True,
                        )
                        response.raise_for_status()
                        raw = response.raw.read(
                            MAX_RPC_RESPONSE_BYTES + 1, decode_content=True,
                        )
                    finally:
                        gate.next_at = time.monotonic() + 0.20
                        if response is not None:
                            response.close()
                if len(raw) > MAX_RPC_RESPONSE_BYTES:
                    raise self._error("blockscout explorer response is oversized")
                payload = json.loads(raw)
                rows = payload.get("result") if isinstance(payload, Mapping) else None
                if isinstance(rows, str) and "no records" in rows.lower():
                    continue
                if not isinstance(rows, list):
                    raise self._error("blockscout explorer returned malformed logs")
                for row in rows:
                    if not isinstance(row, Mapping) or not self._log_matches(query, row):
                        continue
                    item = dict(row)
                    key = (
                        str(item.get("blockNumber") or "").lower(),
                        str(item.get("transactionHash") or "").lower(),
                        str(item.get("logIndex") or "").lower(),
                    )
                    result[key] = item
        return sorted(
            result.values(),
            key=lambda row: (
                int(str(row.get("blockNumber") or "0x0"), 16),
                int(str(row.get("transactionIndex") or "0x0"), 16),
                int(str(row.get("logIndex") or "0x0"), 16),
            ),
        )

    def _local_log_range_eligible(
        self, source: _Source,
        specifications: Sequence[tuple[str, Sequence[Any]]],
    ) -> bool:
        if not _local(source.url):
            return True
        targets: list[int] = []
        for method, params in specifications:
            if method != "eth_getLogs" or len(params) != 1 or not isinstance(params[0], Mapping):
                return False
            query = params[0]
            if query.get("blockHash") is not None:
                return False
            target = query.get("toBlock")
            if isinstance(target, bool):
                return False
            try:
                if isinstance(target, int):
                    number = target
                elif isinstance(target, str) and target.startswith("0x"):
                    number = int(target, 16)
                else:
                    return False
            except ValueError:
                return False
            if number < 0:
                return False
            targets.append(number)
        if not targets:
            return False
        self._request_id += 1
        request_id = self._request_id
        raw = self._post(source, {
            "jsonrpc": "2.0", "id": request_id,
            "method": "eth_blockNumber", "params": [],
        })
        provider_head = self._validate_item(
            source, raw, request_id, "eth_blockNumber",
        )
        try:
            head_number = int(str(provider_head), 16)
        except (TypeError, ValueError) as exc:
            raise self._error(f"{source.name} returned malformed block number") from exc
        return max(targets) <= head_number

    def call(self, method: str, params: Sequence[Any] | None = None) -> Any:
        values = list(params or ())
        capability = _capability(method, values, self._lane)
        sources = self._registry.candidates(capability)
        if not sources and not self._registry.sources[capability]:
            raise self._error(
                f"{capability} RPC unavailable; configure LP_RPC_{capability.upper()}_URLS"
            )
        failures: list[str] = []
        with self._lock:
            if self._closed:
                raise self._error("RPC lane is closed")
            for source in sources:
                if not self._registry.can_try(source, capability):
                    continue
                started = time.monotonic()
                try:
                    self._ensure_chain(source)
                    if capability == "logs" and not self._local_log_range_eligible(
                        source, [(method, values)],
                    ):
                        continue
                    self._request_id += 1
                    request_id = self._request_id
                    raw = self._post(source, {
                        "jsonrpc": "2.0", "id": request_id,
                        "method": method, "params": values,
                    })
                    result = self._validate_item(source, raw, request_id, method)
                    if method == "eth_chainId" and int(str(result), 16) != CHAIN_ID:
                        raise self._error(f"{source.name} changed chain identity")
                except _ExecutionReverted as exc:
                    self._registry.success(
                        source, capability, exc.method, time.monotonic() - started,
                    )
                    raise exc.error from None
                except Exception as exc:
                    self._registry.failure(source, capability, exc)
                    failures.append(self._registry._safe_error(source, exc))
                    continue
                self._registry.success(
                    source, capability, method, time.monotonic() - started,
                )
                return result
            if capability == "logs" and self._registry.can_try(_EXPLORER_SOURCE, capability):
                started = time.monotonic()
                try:
                    result = self._explorer_logs(values)
                except Exception as exc:
                    self._registry.failure(_EXPLORER_SOURCE, capability, exc)
                    failures.append(
                        self._registry._safe_error(_EXPLORER_SOURCE, exc)
                    )
                else:
                    self._registry.success(
                        _EXPLORER_SOURCE, capability, method,
                        time.monotonic() - started,
                    )
                    return result
        if not failures:
            raise self._error(f"{capability} RPC deferred during provider cooldown")
        raise self._error(f"{capability} RPC exhausted: " + "; ".join(failures))

    def batch(self, calls: Iterable[tuple[str, Sequence[Any]]]) -> list[Any]:
        specifications = [(method, list(params)) for method, params in calls]
        if not specifications:
            return []
        if len(specifications) > MAX_BATCH_CALLS:
            raise ValueError(f"RPC batch exceeds {MAX_BATCH_CALLS} calls")
        capabilities = {_capability(method, params, self._lane) for method, params in specifications}
        if len(capabilities) != 1:
            return [self.call(method, params) for method, params in specifications]
        capability = capabilities.pop()
        sources = self._registry.candidates(capability)
        if not sources and not self._registry.sources[capability]:
            raise self._error(
                f"{capability} RPC unavailable; configure LP_RPC_{capability.upper()}_URLS"
            )
        failures: list[str] = []
        with self._lock:
            if self._closed:
                raise self._error("RPC lane is closed")
            for source in sources:
                if not self._registry.can_try(source, capability):
                    continue
                started = time.monotonic()
                try:
                    self._ensure_chain(source)
                    if capability == "logs" and not self._local_log_range_eligible(
                        source, specifications,
                    ):
                        continue
                    payload = []
                    ids = []
                    for method, params in specifications:
                        self._request_id += 1
                        ids.append(self._request_id)
                        payload.append({
                            "jsonrpc": "2.0", "id": self._request_id,
                            "method": method, "params": params,
                        })
                    raw = self._post(source, payload)
                    if not isinstance(raw, list):
                        raise self._error(f"{source.name} returned non-list batch response")
                    by_id = {item.get("id"): item for item in raw if isinstance(item, Mapping)}
                    output = [
                        self._validate_item(source, by_id.get(request_id), request_id, method)
                        for request_id, (method, _params) in zip(ids, specifications)
                    ]
                except _ExecutionReverted as exc:
                    self._registry.success(
                        source, capability, exc.method, time.monotonic() - started,
                    )
                    raise exc.error from None
                except Exception as exc:
                    self._registry.failure(source, capability, exc)
                    failures.append(self._registry._safe_error(source, exc))
                    continue
                for method, _params in specifications:
                    self._registry.success(
                        source, capability, method, time.monotonic() - started,
                    )
                return output
        if not failures:
            raise self._error(f"{capability} RPC deferred during provider cooldown")
        raise self._error(f"{capability} RPC exhausted: " + "; ".join(failures))

    def status(self) -> dict[str, Any]:
        return self._registry.status()

    def close(self) -> None:
        with self._lock:
            self._closed = True
        self._registry.close()

class RpcFactory:
    def __init__(self, primary_url: str, error_type: type[Exception]) -> None:
        self._registry = _Registry(primary_url, error_type)
        self._wss_urls = head_subscription_urls()

    def __call__(self, lane: str) -> Any:
        timeout = 15.0 if lane == "live" else 30.0
        if lane in {"workbench", "state"}:
            timeout = 6.0
        elif lane in {"backfill", "maintenance"}:
            timeout = 20.0
        routed = RoutedRpc(self._registry, lane, timeout)
        if lane in {"workbench", "receipt"} and self._wss_urls:
            return _WssPreferredRpc(
                _WssRpc(self._wss_urls, self._registry.error_type, size=2), routed,
            )
        return routed

    def status(self) -> dict[str, Any]:
        return self._registry.status()

    def close(self) -> None:
        self._registry.close()


def head_subscription_urls() -> tuple[str, ...]:
    """Return public/explicit WSS new-head sources under the RPC safety flags."""
    candidates = _split_urls(os.environ.get("LP_RPC_HEAD_WSS_URLS", ""))
    candidates.extend(_split_urls(os.environ.get("RHP_RPC_WSS", "")))
    disable_local = os.environ.get(
        "LP_RPC_DISABLE_LOCAL_FALLBACK", "",
    ).strip().lower() in {"1", "true", "yes", "on"}
    disable_alchemy = os.environ.get(
        "LP_RPC_DISABLE_ALCHEMY", "",
    ).strip().lower() in {"1", "true", "yes", "on"}
    if not disable_alchemy:
        alchemy = _configured_alchemy()
        if alchemy:
            candidates.append("wss://" + alchemy.removeprefix("https://"))
    # PublicNode exposes a standard Ethereum WebSocket subscription endpoint.
    candidates.append("wss://robinhood-rpc.publicnode.com")
    selected: list[str] = []
    for value in candidates:
        parsed = urlsplit(value)
        if parsed.scheme not in {"ws", "wss"} or not parsed.netloc:
            continue
        if disable_local and (parsed.hostname or "").lower() in _LOCAL_HOSTS:
            continue
        if value not in selected:
            selected.append(value)
    return tuple(selected)


def build_rpc_factory(primary_url: str, error_type: type[Exception]) -> RpcFactory:
    return RpcFactory(primary_url, error_type)


__all__ = ["RoutedRpc", "RpcFactory", "build_rpc_factory", "head_subscription_urls"]
