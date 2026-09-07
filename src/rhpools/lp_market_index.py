"""Live, historical, and asynchronous enrichment indexer for LP markets."""
from __future__ import annotations

from copy import deepcopy
import json
import os
import shutil
import threading
import time
from collections import OrderedDict, deque
from concurrent.futures import ThreadPoolExecutor
from collections.abc import Callable, Iterable, Mapping, Sequence
from typing import Any
from urllib.parse import urlsplit
try:
    import fcntl
except ImportError:  # pragma: no cover - the deployed indexer is POSIX.
    fcntl = None  # type: ignore[assignment]

import requests
from eth_utils import keccak
from requests.adapters import HTTPAdapter

from .lp_chain import CHAIN_ID
from .lp_market_protocols import (
    EVENT_TOPICS,
    NFT_EVENT_TOPICS,
    NFT_MANAGER_ADDRESSES,
    POOL_MANAGER,
    TRANSFER_TOPIC,
    SLIPSTREAM_FACTORY,
    V2_FACTORIES,
    V3_FACTORIES,
    V4_POSITION_MANAGER,
    V4_DONATE_TOPIC,
    V4_INITIALIZE_TOPIC,
    V4_MODIFY_LIQUIDITY_TOPIC,
    V4_PROTOCOL_FEE_UPDATED_TOPIC,
    V4_SWAP_TOPIC,
    decode_gas_record,
    decode_logs,
    decode_position_state_results,
    position_state_requests,
    unknown_pool_candidates,
    resolve_v4_tick_spacing,
)
from .lp_market_store import (
    LP_ENRICHMENT_KINDS, CanonicalConflict, MarketStore,
)


MAX_RPC_RESPONSE_BYTES = 64 * 1024 * 1024
MAX_BATCH_CALLS = 100
MAX_LOGS_PER_RESPONSE = 10_000
LIVE_MIN_CHUNK = 1
LIVE_INITIAL_CHUNK = 256
LIVE_MAX_CHUNK = 2_048
RECENT_CATCHUP_PRIORITY_BLOCKS = 512
HISTORY_MIN_CHUNK = 1
HISTORY_INITIAL_CHUNK = 4_096
HISTORY_MAX_CHUNK = 32_768
MAX_INTERVAL_STORE_SECONDS = 2.0
ENRICH_BATCH = 8
ENRICHMENT_CAPABILITY_RECHECK_S = 300.0
REPROJECT_BATCH = 128
DEFERRED_POOL_IDENTITY_BATCH = 8
DEFERRED_POOL_IDENTITY_CANDIDATES_PER_POOL = 4
DEFERRED_POOL_IDENTITY_SEED_BUSY_S = 1.0
LEGACY_V4_IDENTITY_BATCH = 4
DEFERRED_POOL_IDENTITY_PREFIX = "pool_identity_pending:"
BALANCE_BATCH = 8
BALANCE_OF_SELECTOR = "0x70a08231"
SYMBOL_SELECTOR = "0x95d89b41"
DECIMALS_SELECTOR = "0x313ce567"
FACTORY_SELECTOR = "0xc45a0155"
TOKEN0_SELECTOR = "0x0dfe1681"
TOKEN1_SELECTOR = "0xd21220a7"
FEE_SELECTOR = "0xddca3f43"
TICK_SPACING_SELECTOR = "0xd0c93a7c"
GET_PAIR_SELECTOR = "0xe6a43905"
GET_POOL_SELECTOR = "0x1698ee82"
SLIPSTREAM_GET_POOL_SELECTOR = "0x28af8d0b"
V4_POOL_KEYS_SELECTOR = "0x86b6be7d"
DEFAULT_HISTORY_DISK_RESERVE_BYTES = 4 * 1024 ** 3
MAX_POOL_CACHE_ENTRIES = 16_384
MAX_POOL_MISSES = 8_192
HEAD_FEED_MAX_EVENTS = 8192
HEAD_REPLAY_LIMIT = 64
HEAD_LOG_GRACE_S = 0.25
HEAD_POLL_S = 0.5
CURRENT_RECEIPT_MAX_PENDING = 128
CURRENT_RECEIPT_CACHE_SIZE = 2048
CURRENT_POOL_INPUT_MAX_BYTES = 64 * 1024
CURRENT_POOL_INPUT_MAX_CURRENCIES = 16
CURRENT_POOL_INPUT_MAX_TAILS = 64
WSS_ACTIVITY_TOPIC_BATCH = 7
CURRENT_UNKNOWN_POOL_MAX = 8
CURRENT_POOL_RESOLUTION_MAX_PENDING = 32
CURRENT_POOL_FAILURE_CACHE_SIZE = 2048
CURRENT_POOL_FAILURE_RETRY_S = 30.0
CURRENT_TRUSTED_EVENT_EMITTERS = frozenset({
    POOL_MANAGER, SLIPSTREAM_FACTORY, *V2_FACTORIES, *V3_FACTORIES,
    *NFT_MANAGER_ADDRESSES,
})
CURRENT_V4_POOL_EVENT_TOPICS = frozenset({
    V4_DONATE_TOPIC,
    V4_INITIALIZE_TOPIC,
    V4_MODIFY_LIQUIDITY_TOPIC,
    V4_PROTOCOL_FEE_UPDATED_TOPIC,
    V4_SWAP_TOPIC,
})


class RpcError(RuntimeError):
    """A JSON-RPC request failed or returned an invalid or incomplete result."""

    def __init__(self, message: str, *, code: int | None = None) -> None:
        self.code = code
        super().__init__(message)

    @property
    def range_too_large(self) -> bool:
        text = str(self).lower()
        return self.code in {-32002, -32005, -32602} or any(fragment in text for fragment in (
            "too many", "more than", "response size", "query returned", "block range",
            "limit exceeded", "request entity too large", "timeout",
        ))


class _HttpRpc:
    """Bounded pooled HTTP JSON-RPC client dedicated to one indexer lane."""

    def __init__(self, url: str, *, timeout: float, lane: str) -> None:
        if not isinstance(url, str) or not url.startswith(("http://", "https://")):
            raise ValueError("rpc_url must be an http(s) URL")
        self.url = url
        self.timeout = timeout
        self._lane = lane
        self._request_id = 0
        self._lock = threading.Lock()
        self._closed = False
        self.session = requests.Session()
        self.session.headers.update({
            "Content-Type": "application/json",
            "User-Agent": f"deepstate-lp-market/{lane}",
        })
        adapter = HTTPAdapter(pool_connections=1, pool_maxsize=2, max_retries=0, pool_block=True)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    def _post(self, payload: Any) -> Any:
        response: requests.Response | None = None
        try:
            response = self.session.post(
                self.url, data=json.dumps(payload, separators=(",", ":")),
                timeout=(2.0, self.timeout), stream=True,
            )
            response.raise_for_status()
            raw = response.raw.read(MAX_RPC_RESPONSE_BYTES + 1, decode_content=True)
        except requests.RequestException as exc:
            raise RpcError(f"RPC transport: {exc}") from exc
        finally:
            if response is not None:
                response.close()
        if len(raw) > MAX_RPC_RESPONSE_BYTES:
            raise RpcError(f"RPC response exceeds {MAX_RPC_RESPONSE_BYTES} bytes")
        try:
            return json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise RpcError(f"RPC returned invalid JSON: {exc}") from exc

    def call(self, method: str, params: Sequence[Any] | None = None) -> Any:
        with self._lock:
            if self._closed:
                raise RpcError("RPC client is closed")
            self._request_id += 1
            request_id = self._request_id
            response = self._post({
                "jsonrpc": "2.0", "id": request_id, "method": method,
                "params": list(params or ()),
            })
        if not isinstance(response, Mapping) or response.get("id") != request_id:
            raise RpcError(f"malformed {method} response")
        error = response.get("error")
        if error is not None:
            code = error.get("code") if isinstance(error, Mapping) else None
            raise RpcError(f"{method}: {error}", code=code if isinstance(code, int) else None)
        if "result" not in response:
            raise RpcError(f"{method} response omitted result")
        return response["result"]

    def batch(self, calls: Iterable[tuple[str, Sequence[Any]]]) -> list[Any]:
        specifications = list(calls)
        if not specifications:
            return []
        if len(specifications) > MAX_BATCH_CALLS:
            raise ValueError(f"RPC batch exceeds {MAX_BATCH_CALLS} calls")
        with self._lock:
            if self._closed:
                raise RpcError("RPC client is closed")
            payload = []
            ids = []
            for method, params in specifications:
                self._request_id += 1
                ids.append(self._request_id)
                payload.append({
                    "jsonrpc": "2.0", "id": self._request_id,
                    "method": method, "params": list(params),
                })
            response = self._post(payload)
        if not isinstance(response, list):
            raise RpcError("batch RPC returned a non-list")
        by_id = {item.get("id"): item for item in response if isinstance(item, Mapping)}
        results: list[Any] = []
        for request_id, (method, _params) in zip(ids, specifications):
            item = by_id.get(request_id)
            if item is None:
                raise RpcError(f"batch RPC omitted {method}")
            error = item.get("error")
            if error is not None:
                code = error.get("code") if isinstance(error, Mapping) else None
                raise RpcError(f"{method}: {error}", code=code if isinstance(code, int) else None)
            if "result" not in item:
                raise RpcError(f"batch RPC {method} omitted result")
            results.append(item["result"])
        return results

    def close(self) -> None:
        with self._lock:
            if not self._closed:
                self._closed = True
                self.session.close()



def _hex_int(value: Any, name: str) -> int:
    if isinstance(value, bool):
        raise RpcError(f"{name} is not an integer")
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value, 16) if value.startswith("0x") else int(value)
        except ValueError as exc:
            raise RpcError(f"{name} is not an integer") from exc
    raise RpcError(f"{name} is not an integer")


def _lower(value: Any) -> str:
    return str(value or "").lower()


def _header(raw: Any, expected: int | None = None) -> dict[str, Any]:
    if not isinstance(raw, Mapping):
        raise RpcError("block RPC returned no header")
    number = _hex_int(raw.get("number"), "block number")
    if expected is not None and number != expected:
        raise RpcError(f"requested block {expected}, received {number}")
    block_hash = _lower(raw.get("hash"))
    parent_hash = _lower(raw.get("parentHash"))
    timestamp = _hex_int(raw.get("timestamp"), "block timestamp")
    if len(block_hash) != 66 or len(parent_hash) != 66:
        raise RpcError(f"block {number} has malformed hashes")
    return {
        **dict(raw), "number": hex(number), "hash": block_hash,
        "parentHash": parent_hash, "timestamp": hex(timestamp),
    }


def _number(header: Mapping[str, Any]) -> int:
    return _hex_int(header["number"], "block number")


def _timestamp(header: Mapping[str, Any]) -> int:
    return _hex_int(header["timestamp"], "block timestamp")


class MarketIndexer:
    """Canonical LP event index with independent live/history/enrichment lanes.

    ``rpc`` is an optional deterministic injected client (or ``lane -> client``
    factory) implementing ``call`` and optionally ``batch``.  Production uses
    dedicated live/history sessions and a bounded worker-local enrichment pool.
    """

    def __init__(
        self,
        store: MarketStore,
        market: Any,
        rpc_url: str,
        *,
        history_days: int = 30,
        rpc: Any | Callable[[str], Any] | None = None,
        v3_balances: bool = False,
        history_disk_reserve_bytes: int = DEFAULT_HISTORY_DISK_RESERVE_BYTES,
        current_observer: Any | None = None,
    ) -> None:
        if history_days < 0:
            raise ValueError("history_days must be nonnegative")
        if history_disk_reserve_bytes < 0:
            raise ValueError("history_disk_reserve_bytes must be nonnegative")
        self.store = store
        self.market = market
        self.current_observer = current_observer
        self.rpc_url = rpc_url
        self.history_days = int(history_days)
        self.v3_balances = bool(v3_balances)
        self.history_disk_reserve_bytes = int(history_disk_reserve_bytes)
        self._rpc_factory = rpc if callable(rpc) and not hasattr(rpc, "call") else None
        self._owns_clients = rpc is None or (callable(rpc) and not hasattr(rpc, "call"))
        if rpc is None:
            from .lp_rpc import build_rpc_factory, head_subscription_urls
            self._rpc_factory = build_rpc_factory(rpc_url, RpcError)
            self._clients = {
                lane: self._rpc_factory(lane)
                for lane in ("head", "live", "history", "enrichment", "receipt")
            }
            # Pool identity lookups must not queue behind receipt enrichment.
            self._clients["pool"] = self._rpc_factory("workbench")
            self._head_wss_urls = head_subscription_urls()
        elif callable(rpc) and not hasattr(rpc, "call"):
            self._clients = {
                lane: rpc(lane) for lane in ("live", "history", "enrichment")
            }
            self._clients["head"] = self._clients["live"]
            self._clients["receipt"] = self._clients["head"]
            self._clients["pool"] = rpc("workbench")
            self._head_wss_urls = ()
        else:
            self._clients = {
                lane: rpc
                for lane in (
                    "head", "live", "history", "enrichment", "receipt", "pool"
                )
            }
        if self._rpc_factory is not None:
            # Interval boundary headers use a second lane so a slow log response
            # cannot serialize the canonical checks that frame that response.
            self._clients["live_header"] = self._rpc_factory("live")
            self._clients["history_header"] = self._rpc_factory("history")
        else:
            self._clients["live_header"] = self._clients["live"]
            self._clients["history_header"] = self._clients["history"]
            self._head_wss_urls = ()
        for lane, client in self._clients.items():
            if not callable(getattr(client, "call", None)):
                raise TypeError(f"injected {lane} RPC must implement call(method, params)")
        self._runtime_status: dict[str, Any] = {}
        self._runtime_persist_at = 0.0
        self._runtime_error_signature: tuple[tuple[str, str], ...] = ()
        self._stop = threading.Event()
        self._started = False
        self._initialized = threading.Event()
        self._lifecycle_lock = threading.Lock()
        self._reorg_lock = threading.Lock()
        self._status_lock = threading.Lock()
        self._errors: dict[str, str] = {}
        self._latency: dict[str, float] = {}
        self._fetch_metrics: dict[str, dict[str, Any]] = {}
        self._pool_cache: dict[str, dict[str, Any]] = {}
        self._pool_misses: set[str] = set()
        self._identity_checked: set[str] = set()
        self._identity_resolve_lock = threading.Lock()
        self._cache_lock = threading.Lock()
        self._live_chunk = LIVE_INITIAL_CHUNK
        self._history_chunk = HISTORY_INITIAL_CHUNK
        self._threads: list[threading.Thread] = []
        self._process_lock_fd: int | None = None
        self._history_verified = False
        self._enrichment_verified = False
        self._enrichment_deferred_until = 0.0
        self._enrichment_deferred_reason: str | None = None
        self._projection_verified = False
        self._publish_after_id = ""
        self._market_epoch = int(self.store.status().get("epoch", 0))
        self._enrichment_local = threading.local()
        self._worker_clients: list[Any] = []
        self._worker_clients_lock = threading.Lock()
        self._feed_condition = threading.Condition()
        self._feed_events: deque[dict[str, Any]] = deque(maxlen=HEAD_FEED_MAX_EVENTS)
        self._feed_epoch = f"{time.time_ns():x}"
        self._feed_sequence = 0
        self._observed_blocks: OrderedDict[int, tuple[dict[str, Any], list[dict[str, Any]]]] = OrderedDict()
        self._observed_last_header: dict[str, Any] | None = None
        self._current_receipt_lock = threading.Lock()
        self._current_receipt_pending: set[tuple[str, str]] = set()
        self._current_receipt_cache: OrderedDict[tuple[str, str], bool] = OrderedDict()
        self._current_pool_pending: set[str] = set()
        self._current_pool_failures: OrderedDict[tuple[str, str], float] = OrderedDict()
        self._deferred_identity_seed_at = 0.0
        self._deferred_identity_seed_after_id = ""
        self._legacy_v4_identity_after_id = ""
        self._legacy_v4_identity_complete = False
        self._deferred_identity_seed_complete = False
        self._current_receipt_executor = ThreadPoolExecutor(
            max_workers=2, thread_name_prefix="lp-current-receipt",
        )
        self._current_pool_executor = ThreadPoolExecutor(
            max_workers=2, thread_name_prefix="lp-current-pool",
        )
        self._enrichment_executor = ThreadPoolExecutor(
            max_workers=4, thread_name_prefix="lp-market-fetch",
        )
        self._header_executor = ThreadPoolExecutor(
            max_workers=2, thread_name_prefix="lp-market-headers",
        )
        self._market_observer_executor = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="lp-current-observer",
        )

    @staticmethod
    def _head_source(url: str, prefix: str = "wss") -> str:
        host = (urlsplit(url).hostname or "unknown").lower()
        return f"{prefix}:{host}"

    def _emit_feed(self, event: str, data: Mapping[str, Any]) -> None:
        with self._feed_condition:
            self._feed_sequence += 1
            payload = dict(data)
            payload["sequence"] = self._feed_sequence
            payload["feed_epoch"] = self._feed_epoch
            self._feed_events.append({
                "event": event, "sequence": self._feed_sequence, "data": payload,
            })
            self._feed_condition.notify_all()

    def _run_market_observers(
        self, name: str, args: tuple[Any, ...],
    ) -> None:
        observers = (
            ("market_observer", self.market),
            ("current_observer", self.current_observer),
        )
        seen: set[int] = set()
        for lane, observer in observers:
            if observer is None or id(observer) in seen:
                continue
            seen.add(id(observer))
            callback = getattr(observer, name, None)
            if not callable(callback):
                continue
            try:
                callback(*args)
            except Exception as exc:
                self._set_runtime(lane, error=f"{name}: {exc}")

    def _submit_market_observer(self, name: str, *args: Any) -> None:
        if self._stop.is_set():
            return
        if not any(
            callable(getattr(observer, name, None))
            for observer in (self.market, self.current_observer)
            if observer is not None
        ):
            return
        try:
            self._market_observer_executor.submit(
                self._run_market_observers, name, args,
            )
        except RuntimeError:
            # Shutdown may race an already accepted websocket notification.
            if not self._stop.is_set():
                raise

    def feed_status(self) -> dict[str, Any]:
        with self._feed_condition:
            header = dict(self._observed_last_header or {})
            return {
                "feed_epoch": self._feed_epoch,
                "feed_sequence": self._feed_sequence,
                "observed_head": _number(header) if header else None,
                "observed_head_hash": header.get("hash"),
                "observed_head_timestamp": _timestamp(header) if header else None,
            }

    def feed_updates(
        self, after: int = 0, feed_epoch: str | None = None,
    ) -> dict[str, Any]:
        with self._feed_condition:
            latest_sequence = self._feed_sequence
            if not self._feed_events:
                return {
                    "feed_epoch": self._feed_epoch, "sequence": latest_sequence,
                    "events": [], "reset": feed_epoch not in (None, self._feed_epoch),
                }
            after = int(after)
            earliest = int(self._feed_events[0]["sequence"])
            resumable = (
                feed_epoch == self._feed_epoch
                and earliest - 1 <= after <= latest_sequence
            )
            if resumable:
                rows = []
                for item in reversed(self._feed_events):
                    if int(item["sequence"]) <= after:
                        break
                    rows.append(deepcopy(item))
                rows.reverse()
                return {
                    "feed_epoch": self._feed_epoch, "sequence": latest_sequence,
                    "events": rows, "reset": False,
                }

            # A reset needs only the latest block and activity emitted after it.
            # Walk the tail rather than rescanning the retained feed prefix.
            suffix = []
            latest_block = None
            for item in reversed(self._feed_events):
                suffix.append(item)
                if item.get("event") == "block":
                    latest_block = item
                    break
            if latest_block is None:
                rows = []
            else:
                block_number = latest_block["data"].get("number")
                rows = [
                    deepcopy(item) for item in reversed(suffix)
                    if item.get("event") == "block"
                    or item.get("data", {}).get("block", {}).get("number")
                    == block_number
                ]
                block = rows[0]["data"]
                reconnect_gap = {
                    "reason": "feed_cursor_unavailable",
                    "requested_sequence": after,
                    "available_from_sequence": earliest,
                }
                prior_gap = block.get("gap")
                block["gap"] = (
                    {**prior_gap, "reconnect": reconnect_gap}
                    if isinstance(prior_gap, Mapping) else reconnect_gap
                )
            return {
                "feed_epoch": self._feed_epoch, "sequence": latest_sequence,
                "events": rows, "reset": True,
            }

    def wait_feed(self, after: int, timeout: float = 1.0) -> bool:
        with self._feed_condition:
            return self._feed_condition.wait_for(
                lambda: self._feed_sequence > int(after) or self._stop.is_set(),
                timeout=max(0.0, float(timeout)),
            )

    @staticmethod
    def _current_event_key(event: Mapping[str, Any]) -> tuple[str, str, int]:
        return (
            _lower(event.get("block_hash")), _lower(event.get("tx_hash")),
            int(event.get("log_index") or 0),
        )

    def _emit_current_activity(
        self, header: Mapping[str, Any], events: Sequence[Mapping[str, Any]],
        *, source: str, late: bool = False, observe: bool = True,
    ) -> None:
        if not events:
            return
        rows = []
        for event in events:
            row = dict(event)
            if isinstance(row.get("pool"), Mapping):
                row["pool"] = self._normalize_pool(row["pool"])
            rows.append(row)
        if observe:
            self._submit_market_observer(
                "observe_current_events", dict(header),
                tuple(dict(row) for row in rows),
            )
        self._emit_feed("activity", {
            "type": "activity",
            "lane": "current",
            "qualification": "provisional_canonical",
            "block": {
                "number": _number(header), "hash": header["hash"],
                "timestamp": _timestamp(header),
            },
            "rows": rows,
            "as_of": time.time(), "source": source, "late": late,
        })

    @staticmethod
    def _verified_receipt_transfers(
        receipt: Mapping[str, Any], block_hash: str, tx_hash: str,
    ) -> tuple[list[dict[str, Any]], bool]:
        if (
            _lower(receipt.get("blockHash")) != block_hash
            or _lower(receipt.get("transactionHash")) != tx_hash
        ):
            raise RpcError("current receipt canonical identity mismatch")
        logs = receipt.get("logs")
        if not isinstance(logs, list):
            raise RpcError("current receipt logs are unavailable")
        transfers: list[dict[str, Any]] = []
        truncated = False
        for raw in logs:
            if not isinstance(raw, Mapping) or raw.get("removed"):
                continue
            topics = list(raw.get("topics") or ())
            if len(topics) != 3 or _lower(topics[0]) != TRANSFER_TOPIC:
                continue
            token = _lower(raw.get("address"))
            sender = _lower(topics[1])
            recipient = _lower(topics[2])
            data = str(raw.get("data") or "")
            if (
                len(token) != 42 or len(sender) != 66 or len(recipient) != 66
                or not data.startswith("0x") or len(data) != 66
            ):
                continue
            try:
                amount = int(data, 16)
                log_index = _hex_int(raw.get("logIndex"), "transfer log index")
            except (TypeError, ValueError, RpcError):
                continue
            if amount <= 0:
                continue
            if len(transfers) >= 64:
                truncated = True
                continue
            transfers.append({
                "token": token,
                "amount": str(amount),
                "from": "0x" + sender[-40:],
                "to": "0x" + recipient[-40:],
                "log_index": log_index,
            })
        transfers.sort(key=lambda row: int(row["log_index"]))
        return transfers, truncated

    @staticmethod
    def _transaction_pool_flows(
        event: Mapping[str, Any], transfers: Sequence[Mapping[str, Any]],
    ) -> tuple[str | None, str | None]:
        pool = event.get("pool")
        if not isinstance(pool, Mapping):
            return None, None
        manager = _lower(pool.get("address"))
        if not manager:
            return None, None
        values: list[int | None] = [None, None]
        for side in (0, 1):
            token = _lower(pool.get(f"token{side}"))
            if not token:
                continue
            total = 0
            observed = False
            for transfer in transfers:
                if _lower(transfer.get("token")) != token:
                    continue
                sender, recipient = _lower(transfer.get("from")), _lower(transfer.get("to"))
                if sender == manager and recipient != manager:
                    total += int(transfer["amount"])
                    observed = True
                elif recipient == manager and sender != manager:
                    total -= int(transfer["amount"])
                    observed = True
            if observed:
                values[side] = total
        return tuple(str(value) if value is not None else None for value in values)

    def _schedule_current_receipts(
        self, header: Mapping[str, Any], events: Sequence[Mapping[str, Any]],
        *, source: str,
    ) -> None:
        block_hash = _lower(header.get("hash"))
        tx_hashes = {
            _lower(event.get("tx_hash"))
            for event in events
            if event.get("protocol") == "v4"
            and event.get("kind") in {"add", "remove", "collect"}
            and event.get("cashflow0") is None and event.get("cashflow1") is None
            and event.get("amount0") is None and event.get("amount1") is None
        }
        for tx_hash in sorted(tx_hashes):
            if not block_hash or not tx_hash:
                continue
            key = (block_hash, tx_hash)
            with self._current_receipt_lock:
                if key in self._current_receipt_pending or key in self._current_receipt_cache:
                    continue
                if len(self._current_receipt_pending) >= CURRENT_RECEIPT_MAX_PENDING:
                    continue
                self._current_receipt_pending.add(key)
            self._current_receipt_executor.submit(
                self._enrich_current_receipt, key, _number(header), source,
            )

    def _enrich_current_receipt(
        self, key: tuple[str, str], number: int, source: str,
    ) -> None:
        block_hash, tx_hash = key
        receipt: Mapping[str, Any] | None = None
        try:
            for attempt in range(2):
                if self._stop.is_set():
                    return
                try:
                    candidate = self._clients["receipt"].call(
                        "eth_getTransactionReceipt", [tx_hash],
                    )
                    if isinstance(candidate, Mapping):
                        receipt = candidate
                        break
                except Exception:
                    pass
                if attempt == 0:
                    self._stop.wait(0.1)
            with self._feed_condition:
                cached = self._observed_blocks.get(int(number))
                if cached is None or cached[0]["hash"] != block_hash:
                    return
                header, current_events = cached
                indexes = [
                    index for index, event in enumerate(current_events)
                    if _lower(event.get("tx_hash")) == tx_hash
                    and event.get("protocol") == "v4"
                    and event.get("kind") in {"add", "remove", "collect"}
                ]
            if not indexes:
                return
            if receipt is None:
                transfers, truncated = [], False
            else:
                try:
                    transfers, truncated = self._verified_receipt_transfers(
                        receipt, block_hash, tx_hash,
                    )
                except (RpcError, TypeError, ValueError):
                    receipt = None
                    transfers, truncated = [], False
            updates: list[dict[str, Any]] = []
            with self._feed_condition:
                cached = self._observed_blocks.get(int(number))
                if cached is None or cached[0]["hash"] != block_hash:
                    return
                header, current_events = cached
                for index in indexes[:1]:
                    event = dict(current_events[index])
                    event["transaction_transfers"] = [dict(row) for row in transfers]
                    event["transaction_transfers_truncated"] = truncated
                    flow0, flow1 = self._transaction_pool_flows(event, transfers)
                    event["transaction_flow0"] = flow0
                    event["transaction_flow1"] = flow1
                    event["flow_scope"] = "transaction"
                    event["flow_complete"] = receipt is not None and not truncated
                    event["flow_qualification"] = (
                        "verified_receipt_transfer_logs_not_position_attributed"
                        if receipt is not None else "receipt_unavailable_after_2_attempts"
                    )
                    # PoolManager transfers settle transaction-wide credits.
                    # Even one hookless ModifyLiquidity can use ERC-6909 claims;
                    # receipt nets are not evidence of position cashflows.
                    current_events[index] = event
                    updates.append(event)
            if updates:
                self._emit_current_activity(
                    header, updates, source=source + "+receipt", late=True,
                )
        finally:
            with self._current_receipt_lock:
                self._current_receipt_pending.discard(key)
                self._current_receipt_cache[key] = receipt is not None
                self._current_receipt_cache.move_to_end(key)
                while len(self._current_receipt_cache) > CURRENT_RECEIPT_CACHE_SIZE:
                    self._current_receipt_cache.popitem(last=False)

    @staticmethod
    def _v4_pool_id(log: Mapping[str, Any]) -> str:
        if (
            log.get("removed")
            or _lower(log.get("address")) != POOL_MANAGER
        ):
            return ""
        topics = list(log.get("topics") or ())
        if len(topics) < 2 or _lower(topics[0]) not in CURRENT_V4_POOL_EVENT_TOPICS:
            return ""
        pool_id = _lower(topics[1])
        if (
            len(pool_id) != 66 or not pool_id.startswith("0x")
            or any(char not in "0123456789abcdef" for char in pool_id[2:])
        ):
            return ""
        return pool_id

    def _pool_identity_candidates(
        self, log: Mapping[str, Any],
    ) -> frozenset[str]:
        emitter = _lower(log.get("address"))
        pool_id = self._v4_pool_id(log)
        return frozenset(candidate for candidate in (emitter, pool_id) if candidate)

    def _unresolved_pool_identities(
        self, logs: Sequence[Mapping[str, Any]],
        pools: Mapping[str, Mapping[str, Any]],
    ) -> set[str]:
        unresolved = set(unknown_pool_candidates(logs, pools))
        unresolved.update(
            pool_id for log in logs
            if _lower((log.get("topics") or ("",))[0]) == V4_MODIFY_LIQUIDITY_TOPIC
            and (pool_id := self._v4_pool_id(log))
            and pool_id not in pools
        )
        return unresolved

    def _schedule_current_pool_resolutions(
        self, header: Mapping[str, Any], logs: Sequence[Mapping[str, Any]],
        *, source: str,
    ) -> None:
        if self._stop.is_set():
            return
        copied = [dict(log) for log in logs]
        pools = self._pools_for_logs(copied)
        candidates: list[str] = []
        for log in copied:
            pool_id = self._v4_pool_id(log)
            if pool_id and pool_id not in pools and pool_id not in candidates:
                candidates.append(pool_id)
        candidates.extend(unknown_pool_candidates(copied, pools))
        # If deferred history identity work owns the resolver lock, still enqueue
        # current work. It will run first when the deferred one-address slice
        # releases the lock instead of disappearing from the activity feed.
        for address in candidates[:CURRENT_UNKNOWN_POOL_MAX]:
            if (
                len(address) == 42
                and self._identity_resolve_lock.acquire(blocking=False)
            ):
                try:
                    if address in self._identity_checked:
                        continue
                finally:
                    self._identity_resolve_lock.release()
            candidate_logs = [
                log for log in copied
                if address in self._pool_identity_candidates(log)
            ]
            if not candidate_logs:
                continue
            self._queue_current_pool_resolution(address, header, candidate_logs, source)

    def _queue_current_pool_resolution(
        self, pool_id: str, header: Mapping[str, Any],
        logs: list[dict[str, Any]], source: str, transaction_hash: str = "",
    ) -> None:
        transaction_hash = _lower(transaction_hash) or next(
            (
                _lower(log.get("transactionHash"))
                for log in logs if log.get("transactionHash")
            ),
            "",
        )
        failure_key = (pool_id, transaction_hash)
        with self._current_receipt_lock:
            retry_at = self._current_pool_failures.get(failure_key, 0.0)
            if retry_at > time.monotonic():
                return
            self._current_pool_failures.pop(failure_key, None)
            if (
                self._stop.is_set() or pool_id in self._current_pool_pending
                or len(self._current_pool_pending) >= CURRENT_POOL_RESOLUTION_MAX_PENDING
            ):
                return
            self._current_pool_pending.add(pool_id)
        self._current_pool_executor.submit(
            self._resolve_current_pool, pool_id, _number(header),
            _lower(header.get("hash")), logs, source, transaction_hash,
        )

    def request_pool_resolution(self, pool_id: str, transaction_hash: str = "") -> None:
        """Resolve a cold inspector without waiting for its next pool event."""
        pool_id = str(pool_id or "").strip().lower()
        if (
            len(pool_id) not in {42, 66} or not pool_id.startswith("0x")
            or any(char not in "0123456789abcdef" for char in pool_id[2:])
        ):
            return
        lookup = getattr(self.market, "_pool_by_id", None)
        if callable(lookup) and lookup(pool_id) is not None:
            return
        pool = self._verified_pool(pool_id)
        if pool is not None:
            self._publish_event_pools([{"pool": pool}])
            return
        if len(pool_id) != 66:
            return
        transaction_hash = _lower(transaction_hash)
        if transaction_hash and (
            len(transaction_hash) != 66 or not transaction_hash.startswith("0x")
            or any(char not in "0123456789abcdef" for char in transaction_hash[2:])
        ):
            return
        with self._feed_condition:
            header = dict(self._observed_last_header or {})
        if header:
            self._queue_current_pool_resolution(
                pool_id, header, [], "inspector", transaction_hash,
            )

    def _resolve_current_v4_pool(
        self, pool_id: str, number: int, block_hash: str,
    ) -> dict[str, Any] | None:
        # PosM stores a bytes25 prefix; only the complete PoolKey hash proves
        # identity. A zero/mismatched getter result is not a discovered pool.
        result = self._clients["pool"].call("eth_call", [{
            "to": V4_POSITION_MANAGER,
            "data": V4_POOL_KEYS_SELECTOR + pool_id[2:52] + "00" * 7,
        }, hex(number)])
        if not isinstance(result, str) or len(result) != 322 or not result.startswith("0x"):
            return None
        return self._current_v4_pool_key(
            pool_id, bytes.fromhex(result[2:]), number, block_hash,
            source="PositionManager.poolKeys",
        )

    def _current_v4_pool_key(
        self, pool_id: str, raw: bytes, number: int, block_hash: str, *, source: str,
    ) -> dict[str, Any] | None:
        if len(raw) != 160 or "0x" + keccak(raw).hex() != pool_id:
            return None
        words = [int.from_bytes(raw[offset:offset + 32], "big") for offset in range(0, 160, 32)]
        token0, token1, fee, spacing, hook = words
        if not (
            0 <= token0 < token1 < 1 << 160
            and fee < 1 << 24 and 0 < spacing <= 32767 and hook < 1 << 160
        ):
            return None
        dynamic = bool(fee & 0x800000)
        pool = self._normalize_pool({
            "id": pool_id, "protocol": "v4", "address": POOL_MANAGER,
            "token0": f"0x{token0:040x}", "token1": f"0x{token1:040x}",
            "fee_ppm": None if dynamic else fee, "tick_spacing": spacing,
            "hook": f"0x{hook:040x}", "factory": POOL_MANAGER,
            "source": source,
            "metadata_json": {
                "configured_fee": fee, "dynamic_fee": dynamic,
                "creation_block_known": False,
                "discovery_basis": "full_poolKey_hash",
                "identity_verified_block": number, "identity_verified_hash": block_hash,
            },
        })
        if pool is not None:
            pool.update({
                "_observed_block": number,
                "_observed_hash": block_hash,
                "_observation_basis": f"{source}:full_poolKey_hash",
            })
        return pool

    def _current_v4_input_key(
        self, pool_id: str, transaction: Mapping[str, Any],
        receipt: Mapping[str, Any], header: Mapping[str, Any], tx_hash: str,
    ) -> dict[str, Any] | None:
        number, block_hash = _number(header), _lower(header["hash"])
        if (
            _lower(transaction.get("hash")) != tx_hash
            or _lower(transaction.get("blockHash")) != block_hash
        ):
            raise RpcError("PoolKey transaction canonical identity mismatch")
        transfers, _ = self._verified_receipt_transfers(receipt, block_hash, tx_hash)
        manager_logs = [
            log for log in receipt["logs"]
            if isinstance(log, Mapping) and not log.get("removed")
            and _lower(log.get("address")) == POOL_MANAGER
        ]
        events = decode_logs(manager_logs, {}, {number: header})
        if not any(_lower(event.get("pool_id")) == pool_id for event in events):
            return None
        encoded = transaction.get("input")
        if (
            not isinstance(encoded, str) or not encoded.startswith("0x")
            or len(encoded) > 2 + CURRENT_POOL_INPUT_MAX_BYTES * 2
        ):
            return None
        raw = bytes.fromhex(encoded[2:])
        currencies = {
            _lower(row["token"]) for row in transfers
            if POOL_MANAGER in {row["from"], row["to"]}
        }
        if len(currencies) > CURRENT_POOL_INPUT_MAX_CURRENCIES:
            return None
        currencies.add("0x" + "00" * 20)  # V4's native currency has no Transfer log.
        words = [int(address, 16).to_bytes(32, "big") for address in sorted(currencies)]
        tails: set[bytes] = set()
        zero29, zero12 = bytes(29), bytes(12)
        # ABI data can be nested or packed inside router bytes. Only observed
        # fee/spacing/hook triples are candidates; never enumerate guessed fees
        # or spacings. A complete hash match, not layout resemblance, admits it.
        for offset in range(max(0, len(raw) - 95)):
            if not (
                raw.startswith(zero29, offset) and raw.startswith(zero29, offset + 32)
                and raw.startswith(zero12, offset + 64)
            ):
                continue
            fee = int.from_bytes(raw[offset + 29:offset + 32], "big")
            spacing = int.from_bytes(raw[offset + 61:offset + 64], "big")
            if (fee > 1_000_000 and fee != 0x800000) or not 0 < spacing <= 32767:
                continue
            tails.add(raw[offset:offset + 96])
            if len(tails) > CURRENT_POOL_INPUT_MAX_TAILS:
                return None
        for index, token0 in enumerate(words):
            for token1 in words[index + 1:]:
                prefix = token0 + token1
                for tail in tails:
                    pool = self._current_v4_pool_key(
                        pool_id, prefix + tail, number, block_hash,
                        source="transaction.PoolKey",
                    )
                    if pool is not None:
                        return pool
        return None

    def _resolve_current_v4_input(
        self, pool_id: str, logs: Sequence[Mapping[str, Any]], transaction_hash: str = "",
    ) -> dict[str, Any] | None:
        tx_hash = transaction_hash or next(
            (_lower(log.get("transactionHash")) for log in logs if log.get("transactionHash")),
            "",
        )
        if not tx_hash:
            with self._feed_condition:
                for header, events in reversed(self._observed_blocks.values()):
                    event = next((row for row in events if row.get("pool_id") == pool_id), None)
                    if event is not None:
                        tx_hash = _lower(event["tx_hash"])
                        break
        if not tx_hash:
            return None
        client = self._clients["pool"]
        transaction = client.call("eth_getTransactionByHash", [tx_hash])
        if not isinstance(transaction, Mapping):
            raise RpcError("PoolKey transaction is temporarily unavailable")
        receipt = client.call("eth_getTransactionReceipt", [tx_hash])
        if not isinstance(receipt, Mapping):
            raise RpcError("PoolKey transaction receipt is temporarily unavailable")
        number = _hex_int(transaction.get("blockNumber"), "PoolKey transaction block")
        block_hash = _lower(transaction.get("blockHash"))
        with self._feed_condition:
            cached = self._observed_blocks.get(number)
        if cached is not None and cached[0]["hash"] == block_hash:
            header = cached[0]
        else:
            header = client.call("eth_getBlockByNumber", [hex(number), False])
            if not isinstance(header, Mapping):
                raise RpcError("PoolKey transaction header is temporarily unavailable")
            if (
                _number(header) != number
                or _lower(header.get("hash")) != block_hash
            ):
                raise CanonicalConflict("PoolKey transaction is no longer canonical")
        pool = self._current_v4_input_key(
            pool_id, transaction, receipt, header, tx_hash,
        )
        if cached is not None:
            with self._feed_condition:
                current = self._observed_blocks.get(number)
                if current is None or current[0]["hash"] != block_hash:
                    return None
        else:
            canonical = client.call("eth_getBlockByNumber", [hex(number), False])
            if (
                not isinstance(canonical, Mapping)
                or _number(canonical) != number
                or _lower(canonical.get("hash")) != block_hash
            ):
                raise CanonicalConflict(
                    "PoolKey transaction changed during identity recovery"
                )
        return pool

    def _resolve_current_pool(
        self, address: str, number: int, block_hash: str,
        logs: list[dict[str, Any]], source: str, transaction_hash: str = "",
    ) -> None:
        resolved = False
        try:
            if self._stop.is_set():
                return
            if len(address) == 66:
                pool = self._resolve_current_v4_pool(address, number, block_hash)
                if pool is None:
                    pool = self._resolve_current_v4_input(address, logs, transaction_hash)
            else:
                pools = self._pools_for_logs(logs)
                self._resolve_unknown_pools("pool", logs, pools)
                pool = self._verified_pool(address)
            if pool is None:
                return
            with self._feed_condition:
                cached = self._observed_blocks.get(int(number))
                if cached is None or cached[0]["hash"] != block_hash:
                    return
            durable_candidate = dict(pool)
            durable_candidate.setdefault("_observed_block", int(number))
            durable_candidate.setdefault("_observed_hash", block_hash)
            durable_candidate.setdefault(
                "_observation_basis", f"{source}:canonical_pool_identity",
            )
            self.store.upsert_pools([durable_candidate])
            stored = self.store.pool(str(pool["id"]))
            if stored is None:
                raise RuntimeError("resolved pool persistence was lost")
            durable = self._stored_pool(stored)
            self._remember_pool(durable)
            self._publish_event_pools([{"pool": durable}])
            self._publish_late_current_logs(
                number, logs, source=source + "+pool",
            )
            resolved = True
        except Exception as exc:
            self._set_runtime(
                "activity_feed", error=f"pool resolution: {exc}",
                activity_feed_source=source,
            )
        finally:
            with self._current_receipt_lock:
                self._current_pool_pending.discard(address)
                if resolved:
                    for key in tuple(self._current_pool_failures):
                        if key[0] == address:
                            self._current_pool_failures.pop(key, None)
                elif len(address) == 66:
                    failure_key = (address, transaction_hash)
                    self._current_pool_failures[failure_key] = (
                        time.monotonic() + CURRENT_POOL_FAILURE_RETRY_S
                    )
                    self._current_pool_failures.move_to_end(failure_key)
                    while (
                        len(self._current_pool_failures)
                        > CURRENT_POOL_FAILURE_CACHE_SIZE
                    ):
                        self._current_pool_failures.popitem(last=False)

    def _publish_late_current_logs(
        self, number: int, logs: Sequence[Mapping[str, Any]], *, source: str,
    ) -> None:
        with self._feed_condition:
            cached = self._observed_blocks.get(int(number))
        if cached is None:
            return
        header, _ = cached
        selected = [dict(log) for log in logs
                    if not log.get("removed")
                    and _lower(log.get("blockHash")) == header["hash"]]
        if not selected:
            return
        events = self._decode_current(selected, {int(number): header})
        fresh: list[dict[str, Any]] = []
        enriched: list[dict[str, Any]] = []
        with self._feed_condition:
            current = self._observed_blocks.get(int(number))
            if current is None or current[0]["hash"] != header["hash"]:
                return
            prior = {self._current_event_key(event): event for event in current[1]}
            for event in events:
                previous = prior.get(self._current_event_key(event))
                if previous is None:
                    current[1].append(dict(event))
                    fresh.append(event)
                elif not previous.get("pool") and event.get("pool"):
                    # Keep receipt enrichment already published for this event.
                    previous["pool"] = event["pool"]
                    transfers = previous.get("transaction_transfers")
                    if transfers is not None:
                        flow0, flow1 = self._transaction_pool_flows(previous, transfers)
                        previous["transaction_flow0"] = flow0
                        previous["transaction_flow1"] = flow1
                    enriched.append(dict(previous))
        updates = fresh + enriched
        if updates:
            self._publish_event_pools(updates)
            self._emit_current_activity(header, updates, source=source, late=True)
            self._schedule_current_receipts(header, fresh, source=source)
        self._schedule_current_pool_resolutions(
            header, selected, source=source,
        )

    def _publish_enriched_current_events(
        self, events: Sequence[Mapping[str, Any]], *, source: str,
    ) -> None:
        """Replace still-current provisional rows with canonical enrichment."""
        by_block: dict[int, list[Mapping[str, Any]]] = {}
        for event in events:
            by_block.setdefault(int(event["block_number"]), []).append(event)
        for number, block_events in sorted(by_block.items()):
            updates: list[dict[str, Any]] = []
            with self._feed_condition:
                current = self._observed_blocks.get(number)
                if current is None:
                    continue
                header, current_events = current
                indexes = {
                    self._current_event_key(event): index
                    for index, event in enumerate(current_events)
                }
                for enriched in sorted(block_events, key=lambda row: (
                    int(row["tx_index"]), int(row["log_index"]),
                )):
                    if _lower(enriched.get("block_hash")) != header["hash"]:
                        continue
                    index = indexes.get(self._current_event_key(enriched))
                    if index is None:
                        continue
                    previous = current_events[index]
                    merged = {**previous, **dict(enriched)}
                    previous_data = previous.get("data")
                    enriched_data = enriched.get("data")
                    merged["data"] = {
                        **(dict(previous_data) if isinstance(previous_data, Mapping) else {}),
                        **(dict(enriched_data) if isinstance(enriched_data, Mapping) else {}),
                    }
                    if (
                        merged.get("position_key") is not None
                        and merged.get("cashflow0") is not None
                        and merged.get("cashflow1") is not None
                    ):
                        merged["flow_scope"] = "position_event"
                        merged["flow_complete"] = True
                        merged["flow_qualification"] = (
                            merged.get("accounting_basis") or "event_exact"
                        )
                    current_events[index] = merged
                    updates.append(dict(merged))
                if updates:
                    self._emit_current_activity(
                        header, updates, source=source, late=True,
                    )

    def _publish_current_block(
        self, header: Mapping[str, Any], logs: Sequence[Mapping[str, Any]],
        *, source: str, gap: Mapping[str, Any] | None = None,
    ) -> None:
        current = _header(header)
        number = _number(current)
        with self._feed_condition:
            previous = dict(self._observed_last_header or {})
        previous_number = _number(previous) if previous else None
        if previous_number is not None and number < previous_number:
            return
        if previous_number == number and previous.get("hash") == current["hash"]:
            return
        observed_gap = dict(gap) if gap else None
        if previous_number == number and previous.get("hash") != current["hash"]:
            observed_gap = {
                "reason": "same_height_reorg", "from": number, "to": number,
                "replaced_hash": previous.get("hash"),
            }
        elif previous_number is not None and number == previous_number + 1:
            if current["parentHash"] != previous.get("hash"):
                observed_gap = {
                    "reason": "parent_hash_mismatch", "from": number, "to": number,
                    "expected_parent": previous.get("hash"),
                    "observed_parent": current["parentHash"],
                }
        elif previous_number is not None and number > previous_number + 1:
            observed_gap = observed_gap or {
                "reason": "head_gap", "from": previous_number + 1, "to": number - 1,
            }
        selected_logs: list[dict[str, Any]] = []
        for raw in logs:
            item = dict(raw)
            if item.get("removed"):
                observed_gap = observed_gap or {
                    "reason": "removed_log", "from": number, "to": number,
                }
                continue
            if _hex_int(item.get("blockNumber"), "log block number") != number:
                continue
            block_hash = _lower(item.get("blockHash"))
            if block_hash and block_hash != current["hash"]:
                observed_gap = observed_gap or {
                    "reason": "log_block_hash_mismatch", "from": number, "to": number,
                }
                continue
            item["blockHash"] = current["hash"]
            selected_logs.append(item)
        try:
            events = self._decode_current(selected_logs, {number: current})
        except Exception as exc:
            events = []
            observed_gap = observed_gap or {
                "reason": "current_decode_error", "from": number, "to": number,
                "detail": str(exc)[:200],
            }
        block_payload: dict[str, Any] = {
            "number": number,
            "hash": current["hash"],
            "parent_hash": current["parentHash"],
            "timestamp": _timestamp(current),
            "as_of": time.time(),
            "source": source,
        }
        if observed_gap:
            block_payload["gap"] = observed_gap
        with self._feed_condition:
            if previous_number == number or observed_gap and observed_gap.get("reason") in {
                "same_height_reorg", "parent_hash_mismatch",
            }:
                for cached_number in tuple(self._observed_blocks):
                    if cached_number >= number:
                        self._observed_blocks.pop(cached_number, None)
            self._observed_last_header = current
            self._observed_blocks[number] = (current, [dict(event) for event in events])
            self._observed_blocks.move_to_end(number)
            while len(self._observed_blocks) > HEAD_REPLAY_LIMIT * 2:
                self._observed_blocks.popitem(last=False)
        self._submit_market_observer("observe_current_block", dict(current))
        self._submit_market_observer(
            "observe_current_events", dict(current),
            tuple(dict(event) for event in events),
        )
        self._emit_feed("block", block_payload)
        self._publish_event_pools(events)
        self._emit_current_activity(
            current, events, source=source, observe=False,
        )
        self._schedule_current_receipts(current, events, source=source)
        self._schedule_current_pool_resolutions(current, selected_logs, source=source)

    def _fetch_current_range(
        self, start: int, end: int, *, source: str,
        first_gap: Mapping[str, Any] | None = None,
    ) -> None:
        if end < start:
            return
        headers = self._blocks("head", range(start, end + 1))
        for number in range(start, end + 1):
            self._publish_current_block(
                headers[number], (), source=source,
                gap=first_gap if number == start else None,
            )

    def _reconcile_current_head(self, raw: Mapping[str, Any], *, source: str) -> None:
        head = _header(raw)
        number = _number(head)
        with self._feed_condition:
            previous = dict(self._observed_last_header or {})
        if not previous:
            cursor = self.store.cursor("live") or {}
            indexed = cursor.get("block_number")
            gap = None
            if indexed is not None and int(indexed) < number:
                gap = {
                    "reason": "current_lane_snapshot",
                    "from": int(indexed) + 1, "to": number - 1,
                }
            self._publish_current_block(head, (), source=source, gap=gap)
            return
        previous_number = _number(previous)
        if number == previous_number and head["hash"] == previous["hash"]:
            return
        if number <= previous_number:
            self._publish_current_block(head, (), source=source)
            return
        start = previous_number + 1
        gap = None
        if number - start + 1 > HEAD_REPLAY_LIMIT:
            bounded_start = number - HEAD_REPLAY_LIMIT + 1
            gap = {
                "reason": "bounded_head_backfill",
                "from": start, "to": bounded_start - 1,
            }
            start = bounded_start
        self._fetch_current_range(start, number, source=source, first_gap=gap)

    def _head_wss_once(self, url: str) -> None:
        from websockets.sync.client import connect

        source = self._head_source(url)
        with connect(
            url, open_timeout=10, ping_interval=15, ping_timeout=10,
            close_timeout=5, max_queue=4096,
        ) as websocket:
            websocket.send(json.dumps({
                "jsonrpc": "2.0", "id": 1,
                "method": "eth_subscribe", "params": ["newHeads"],
            }, separators=(",", ":")))
            early: list[dict[str, Any]] = []
            deadline = time.monotonic() + 10.0
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise RpcError("newHeads subscription acknowledgement timeout")
                message = json.loads(websocket.recv(timeout=remaining))
                if message.get("id") == 1:
                    if "error" in message:
                        raise RpcError("newHeads subscription rejected")
                    break
                if message.get("method") == "eth_subscription":
                    early.append(message)
            latest = self._clients["head"].call(
                "eth_getBlockByNumber", ["latest", False],
            )
            self._reconcile_current_head(latest, source=source + "+reconcile")
            self._set_runtime(
                "head_feed", head_feed_source=source,
                head_feed_logs="independent_subscription",
            )

            def receive(message: Mapping[str, Any]) -> None:
                if message.get("method") != "eth_subscription":
                    return
                result = (message.get("params") or {}).get("result")
                if not isinstance(result, Mapping) or result.get("number") is None:
                    return
                head = _header(result)
                with self._feed_condition:
                    previous = dict(self._observed_last_header or {})
                if (
                    previous
                    and _number(head) == _number(previous) + 1
                    and head["parentHash"] == previous["hash"]
                ):
                    self._publish_current_block(head, (), source=source)
                else:
                    self._reconcile_current_head(head, source=source + "+catchup")

            for message in early:
                receive(message)
            while not self._stop.is_set():
                try:
                    raw_message = websocket.recv(timeout=0.25)
                except TimeoutError:
                    continue
                receive(json.loads(raw_message))

    def _activity_wss_once(self, url: str) -> None:
        from websockets.sync.client import connect

        source = self._head_source(url, "wss-logs")
        with connect(
            url, open_timeout=10, ping_interval=15, ping_timeout=10,
            close_timeout=5, max_queue=8192,
        ) as websocket:
            # Publicnode acknowledges a large OR-list but can silently omit all
            # matching notifications.  Disjoint bounded filters preserve the
            # same event set without duplicating delivery.
            topic_batches = tuple(
                EVENT_TOPICS[offset:offset + WSS_ACTIVITY_TOPIC_BATCH]
                for offset in range(0, len(EVENT_TOPICS), WSS_ACTIVITY_TOPIC_BATCH)
            )
            specifications = {
                request_id: ["logs", {"topics": [list(topics)]}]
                for request_id, topics in enumerate(topic_batches, 1)
            }
            if NFT_MANAGER_ADDRESSES and NFT_EVENT_TOPICS:
                specifications[len(specifications) + 1] = ["logs", {
                    "address": list(NFT_MANAGER_ADDRESSES),
                    "topics": [list(NFT_EVENT_TOPICS)],
                }]
            for request_id, params in specifications.items():
                websocket.send(json.dumps({
                    "jsonrpc": "2.0", "id": request_id,
                    "method": "eth_subscribe", "params": params,
                }, separators=(",", ":")))
            pending_acks = set(specifications)
            accepted = 0
            early: list[dict[str, Any]] = []
            deadline = time.monotonic() + 10.0
            while pending_acks:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise RpcError("activity subscription acknowledgement timeout")
                message = json.loads(websocket.recv(timeout=remaining))
                request_id = message.get("id")
                if request_id in pending_acks:
                    pending_acks.remove(request_id)
                    accepted += int("error" not in message)
                elif message.get("method") == "eth_subscription":
                    early.append(message)
            if accepted != len(specifications):
                raise RpcError(
                    f"activity log subscriptions incomplete "
                    f"({accepted}/{len(specifications)} accepted)"
                )
            self._set_runtime(
                "activity_feed", activity_feed_source=source,
                activity_feed_subscriptions=accepted,
            )
            pending_logs: dict[int, list[dict[str, Any]]] = {}
            first_seen: dict[int, float] = {}
            newest_log_block = -1

            def receive(message: Mapping[str, Any]) -> None:
                nonlocal newest_log_block
                if message.get("method") != "eth_subscription":
                    return
                result = (message.get("params") or {}).get("result")
                if not isinstance(result, Mapping) or result.get("blockNumber") is None:
                    return
                if result.get("removed"):
                    raise CanonicalConflict("activity subscription reported a removed log")
                number = _hex_int(result.get("blockNumber"), "log block number")
                pending_logs.setdefault(number, []).append(dict(result))
                first_seen.setdefault(number, time.monotonic())
                newest_log_block = max(newest_log_block, number)

            def flush_ready(force: bool = False) -> None:
                now = time.monotonic()
                with self._feed_condition:
                    observed = (
                        _number(self._observed_last_header)
                        if self._observed_last_header else -1
                    )
                    cached_numbers = set(self._observed_blocks)
                ready = sorted(
                    number for number in pending_logs
                    if number in cached_numbers and (
                        number < newest_log_block
                        or force and observed >= number
                        or observed == number
                        and now - first_seen.get(number, now) >= HEAD_LOG_GRACE_S
                    )
                )
                for number in ready:
                    logs = pending_logs.pop(number)
                    first_seen.pop(number, None)
                    try:
                        self._publish_late_current_logs(number, logs, source=source)
                    except Exception as exc:
                        self._set_runtime("activity_feed", error=exc,
                                          activity_feed_source=source)
                if len(pending_logs) > HEAD_REPLAY_LIMIT * 2:
                    discarded = sorted(pending_logs)[:-HEAD_REPLAY_LIMIT]
                    for number in discarded:
                        pending_logs.pop(number, None)
                        first_seen.pop(number, None)
                    self._set_runtime(
                        "activity_feed",
                        error=f"discarded {len(discarded)} stale activity blocks",
                        activity_feed_source=source,
                    )

            for message in early:
                receive(message)
            flush_ready()
            while not self._stop.is_set():
                try:
                    raw_message = websocket.recv(timeout=0.05)
                except TimeoutError:
                    flush_ready(force=True)
                    continue
                receive(json.loads(raw_message))
                flush_ready()

    def _activity_run(self) -> None:
        backoff = 1.0
        while not self._stop.is_set():
            for url in self._head_wss_urls:
                if self._stop.is_set():
                    return
                try:
                    self._activity_wss_once(url)
                except Exception as exc:
                    self._set_runtime(
                        "activity_feed", error=exc,
                        activity_feed_source=self._head_source(url, "wss-logs"),
                    )
                    continue
            self._stop.wait(backoff)
            backoff = min(15.0, backoff * 2.0)

    def _head_run(self) -> None:
        retry_delay = 1.0
        while not self._stop.is_set():
            connected = False
            for url in self._head_wss_urls:
                if self._stop.is_set():
                    return
                try:
                    self._head_wss_once(url)
                except Exception as exc:
                    self._set_runtime("head_feed", error=exc,
                                      head_feed_source=self._head_source(url))
                    continue
                connected = True
                retry_delay = 1.0
            if connected:
                continue
            retry_at = time.monotonic() + retry_delay
            while not self._stop.is_set() and time.monotonic() < retry_at:
                try:
                    latest = self._clients["head"].call(
                        "eth_getBlockByNumber", ["latest", False],
                    )
                    self._reconcile_current_head(latest, source="rpc_poll:head")
                except Exception as exc:
                    self._set_runtime("head_feed", error=exc,
                                      head_feed_source="rpc_poll:head")
                self._stop.wait(HEAD_POLL_S)
            retry_delay = min(15.0, retry_delay * 2.0)

    def _acquire_process_lock(self) -> None:
        if self._process_lock_fd is not None or str(self.store.path) == ":memory:":
            return
        if fcntl is None:
            raise RuntimeError("MarketIndexer requires POSIX flock for exclusive writer ownership")
        lock_path = f"{self.store.path.resolve()}.indexer.lock"
        fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o644)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            os.ftruncate(fd, 0)
            os.write(fd, f"{os.getpid()}\n".encode())
        except BlockingIOError as exc:
            os.close(fd)
            raise RuntimeError(
                f"another MarketIndexer owns the canonical writer lock {lock_path}"
            ) from exc
        except BaseException:
            os.close(fd)
            raise
        self._process_lock_fd = fd

    def _release_process_lock(self) -> None:
        fd, self._process_lock_fd = self._process_lock_fd, None
        if fd is None:
            return
        if fcntl is not None:
            fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)

    def _worker_rpc(self) -> Any:
        if not self._owns_clients:
            return self._clients["enrichment"]
        client = getattr(self._enrichment_local, "client", None)
        if client is None:
            client = (
                self._rpc_factory("enrichment")
                if self._rpc_factory is not None
                else _HttpRpc(self.rpc_url, timeout=30.0, lane="enrichment-worker")
            )
            if not callable(getattr(client, "call", None)):
                raise TypeError("enrichment worker RPC must implement call(method, params)")
            self._enrichment_local.client = client
            with self._worker_clients_lock:
                self._worker_clients.append(client)
        return client

    def _close_owned_rpc_clients(self) -> None:
        if not self._owns_clients:
            return
        with self._worker_clients_lock:
            clients = [*self._clients.values(), *self._worker_clients]
            self._worker_clients.clear()
        closed: set[int] = set()
        for client in clients:
            close = getattr(client, "close", None)
            if id(client) not in closed and callable(close):
                close()
                closed.add(id(client))
    def source_status(self) -> dict[str, Any]:
        for client in self._clients.values():
            status = getattr(client, "status", None)
            if callable(status):
                return dict(status())
        return {}

    def _batch_on(
        self, client: Any, calls: list[tuple[str, Sequence[Any]]],
    ) -> list[Any]:
        if not calls:
            return []
        if len(calls) > MAX_BATCH_CALLS:
            output: list[Any] = []
            for index in range(0, len(calls), MAX_BATCH_CALLS):
                output.extend(self._batch_on(client, calls[index:index + MAX_BATCH_CALLS]))
            return output
        batch = getattr(client, "batch", None)
        if callable(batch):
            return list(batch(calls))
        return [client.call(method, list(params)) for method, params in calls]

    def _rpc_batch(self, lane: str, calls: list[tuple[str, Sequence[Any]]]) -> list[Any]:
        return self._batch_on(self._clients[lane], calls)

    def _block(self, lane: str, number: int) -> dict[str, Any]:
        return _header(
            self._clients[lane].call("eth_getBlockByNumber", [hex(number), False]),
            number,
        )

    def _blocks(self, lane: str, numbers: Iterable[int]) -> dict[int, dict[str, Any]]:
        ordered = list(dict.fromkeys(int(number) for number in numbers))
        results = self._rpc_batch(
            lane, [("eth_getBlockByNumber", [hex(number), False]) for number in ordered],
        )
        return {number: _header(raw, number) for number, raw in zip(ordered, results)}
    def _timed_blocks(
        self, lane: str, numbers: Iterable[int],
    ) -> tuple[dict[int, dict[str, Any]], float]:
        started = time.monotonic()
        return self._blocks(lane, numbers), time.monotonic() - started


    def _set_runtime(
        self, lane: str, *, error: BaseException | str | None = None,
        latency: float | None = None, **values: Any,
    ) -> None:
        now = time.monotonic()
        with self._status_lock:
            if error is None:
                self._errors.pop(lane, None)
            else:
                self._errors[lane] = str(error)[:1000]
            if latency is not None:
                self._latency[lane] = round(float(latency), 6)
            self._runtime_status.update(values)
            self._runtime_status.update({
                "chain_id": CHAIN_ID,
                "state": "stopped" if self._stop.is_set()
                else ("degraded" if self._errors else "live"),
                "errors": dict(self._errors),
                "latency": dict(self._latency),
            })
            payload = dict(self._runtime_status)
            error_signature = tuple(sorted(self._errors.items()))
            urgent = (
                error_signature != self._runtime_error_signature
                or any(key in values for key in ("reorg", "startup", "storage_paused"))
                or lane == "lifecycle"
            )
            persist = urgent or now - self._runtime_persist_at >= 2.0
            if persist:
                self._runtime_persist_at = now
                self._runtime_error_signature = error_signature
        if persist and self.store.lock.acquire(blocking=False):
            try:
                # Operational health must never queue behind ledger writers.
                self.store.update_status(**payload)
            finally:
                self.store.lock.release()

    def runtime_status(self) -> dict[str, Any]:
        with self._status_lock:
            return dict(self._runtime_status)

    def _verify_chain(self, lane: str) -> None:
        chain_id = _hex_int(self._clients[lane].call("eth_chainId", []), "chain id")
        if chain_id != CHAIN_ID:
            raise RpcError(f"RPC chain id {chain_id} does not match {CHAIN_ID}")

    def _history_target(self, head: Mapping[str, Any]) -> tuple[int, int]:
        target_timestamp = max(0, _timestamp(head) - self.history_days * 86_400)
        if self.history_days == 0:
            return _number(head), target_timestamp
        low, high = 0, _number(head)
        while low < high:
            middle = (low + high) // 2
            candidate = self._block("history_header", middle)
            if _timestamp(candidate) < target_timestamp:
                low = middle + 1
            else:
                high = middle
        return low, target_timestamp

    def _initialize_cursors(self, head: Mapping[str, Any]) -> None:
        head_number = _number(head)
        live = self.store.cursor("live")
        if not isinstance(live, dict) or live.get("block_number") is None:
            anchor_number = max(0, head_number - 1)
            anchor = head if anchor_number == head_number else self._block("live", anchor_number)
            self.store.ingest(
                [anchor], [], lane="live",
                cursor={
                    "block_number": anchor_number,
                    "block_hash": anchor["hash"],
                    "timestamp": _timestamp(anchor),
                },
            )
        history = self.store.cursor("history")
        if not isinstance(history, dict) or history.get("next_to") is None:
            anchor_number = max(0, head_number - 1)
            anchor = head if anchor_number == head_number else self._block("live", anchor_number)
            target_timestamp = max(0, _timestamp(head) - self.history_days * 86_400)
            self.store.ingest(
                [anchor], [], lane="history",
                cursor={
                    "next_to": anchor_number,
                    "low_block": anchor_number + 1,
                    "block_hash": anchor["hash"],
                    "target_block": head_number if self.history_days == 0 else None,
                    "target_timestamp": target_timestamp,
                    "target_pending": self.history_days > 0,
                    "origin_head": head_number,
                    "complete": self.history_days == 0,
                    "has_coverage": False,
                },
            )

    def _bootstrap(self) -> tuple[int, dict[str, Any], float]:
        started = time.monotonic()
        self._verify_chain("live")
        head_number = _hex_int(
            self._clients["live"].call("eth_blockNumber", []), "head",
        )
        head = self._block("live", head_number)
        self._initialize_cursors(head)
        self._sync_market_reorg()
        return head_number, head, time.monotonic() - started

    def start(self, *, deferred: bool = False) -> "MarketIndexer":
        with self._lifecycle_lock:
            if self._started:
                return self
            if self._stop.is_set():
                raise RuntimeError("closed MarketIndexer cannot be restarted")
            try:
                self._acquire_process_lock()
                if not deferred:
                    head_number, head, latency = self._bootstrap()
                    self._initialized.set()
                    self._set_runtime(
                        "live", latency=latency,
                        head=head_number, head_hash=head["hash"],
                        head_timestamp=_timestamp(head),
                    )
                else:
                    self._set_runtime(
                        "startup", state="warming",
                        startup="RPC bootstrap is running asynchronously; stored canonical data remains readable",
                    )
                self._threads = [
                    threading.Thread(target=self._live_run, name="lp-market-live", daemon=True),
                    threading.Thread(target=self._history_run, name="lp-market-history", daemon=True),
                    threading.Thread(
                        target=self._enrichment_run,
                        name="lp-market-enrichment",
                        daemon=True,
                    ),
                    threading.Thread(
                        target=self._projection_run,
                        name="lp-market-projection",
                        daemon=True,
                    ),
                ]
                if self._head_wss_urls:
                    self._threads[0:0] = [
                        threading.Thread(
                            target=self._head_run,
                            name="lp-market-head",
                            daemon=True,
                        ),
                        threading.Thread(
                            target=self._activity_run,
                            name="lp-market-activity",
                            daemon=True,
                        ),
                    ]
                self._started = True
                for thread in self._threads:
                    thread.start()
            except BaseException:
                self._stop.set()
                self._initialized.set()
                for thread in self._threads:
                    if thread.ident is not None:
                        thread.join()
                self._current_receipt_executor.shutdown(
                    wait=True, cancel_futures=True,
                )
                self._current_pool_executor.shutdown(
                    wait=True, cancel_futures=True,
                )
                self._enrichment_executor.shutdown(
                    wait=True, cancel_futures=True,
                )
                self._header_executor.shutdown(
                    wait=True, cancel_futures=True,
                )
                self._market_observer_executor.shutdown(
                    wait=True, cancel_futures=True,
                )
                self._close_owned_rpc_clients()
                self._started = False
                self._release_process_lock()
                raise
        return self

    @staticmethod
    def _log_key(log: Mapping[str, Any]) -> tuple[str, str, int]:
        return (
            _lower(log.get("blockHash")), _lower(log.get("transactionHash")),
            _hex_int(log.get("logIndex"), "log index"),
        )

    def _required_batch(
        self, lane: str, calls: list[tuple[str, Sequence[Any]]],
    ) -> list[Any]:
        try:
            return self._rpc_batch(lane, calls)
        except RpcError:
            client = self._clients[lane]
            return [client.call(method, list(params)) for method, params in calls]

    def _receipt_logs(
        self, lane: str, block_number: int, topics: Sequence[str],
        addresses: Sequence[str] | None,
    ) -> list[dict[str, Any]]:
        count = _hex_int(
            self._clients[lane].call(
                "eth_getBlockTransactionCountByNumber", [hex(block_number)],
            ),
            "block transaction count",
        )
        if count < 0:
            raise RpcError("block transaction count is negative")
        selected_topics = {str(topic).lower() for topic in topics}
        selected_addresses = (
            {str(address).lower() for address in addresses} if addresses else None
        )
        matched: list[dict[str, Any]] = []
        for offset in range(0, count, MAX_BATCH_CALLS):
            indexes = range(offset, min(count, offset + MAX_BATCH_CALLS))
            transactions = self._required_batch(
                lane,
                [
                    (
                        "eth_getTransactionByBlockNumberAndIndex",
                        [hex(block_number), hex(index)],
                    )
                    for index in indexes
                ],
            )
            tx_hashes: list[str] = []
            for transaction in transactions:
                if not isinstance(transaction, Mapping) or not isinstance(
                    transaction.get("hash"), str
                ):
                    raise RpcError("indexed transaction fallback returned malformed data")
                tx_hashes.append(str(transaction["hash"]))
            receipts = self._required_batch(
                lane,
                [("eth_getTransactionReceipt", [tx_hash]) for tx_hash in tx_hashes],
            )
            for receipt in receipts:
                if not isinstance(receipt, Mapping) or not isinstance(receipt.get("logs"), list):
                    raise RpcError("receipt fallback returned malformed logs")
                for raw in receipt["logs"]:
                    if not isinstance(raw, Mapping):
                        raise RpcError("receipt fallback returned a malformed log")
                    log_topics = raw.get("topics")
                    if not isinstance(log_topics, list) or not log_topics:
                        continue
                    if str(log_topics[0]).lower() not in selected_topics:
                        continue
                    if (
                        selected_addresses is not None
                        and _lower(raw.get("address")) not in selected_addresses
                    ):
                        continue
                    matched.append(dict(raw))
        return matched

    def _split_single_block_logs(
        self, lane: str, block_number: int, topics: Sequence[str],
        addresses: Sequence[str] | None,
    ) -> list[dict[str, Any]]:
        if len(topics) > 1:
            middle = len(topics) // 2
            return (
                self._log_query(lane, block_number, block_number, topics[:middle], addresses)
                + self._log_query(lane, block_number, block_number, topics[middle:], addresses)
            )
        if addresses and len(addresses) > 1:
            middle = len(addresses) // 2
            return (
                self._log_query(lane, block_number, block_number, topics, addresses[:middle])
                + self._log_query(lane, block_number, block_number, topics, addresses[middle:])
            )
        return self._receipt_logs(lane, block_number, topics, addresses)

    def _log_query(
        self,
        lane: str,
        start: int,
        end: int,
        topics: Sequence[str],
        addresses: Sequence[str] | None = None,
    ) -> list[dict[str, Any]]:
        if not topics:
            return []
        request: dict[str, Any] = {
            "fromBlock": hex(start), "toBlock": hex(end), "topics": [list(topics)],
        }
        if addresses:
            request["address"] = list(addresses)
        try:
            result = self._clients[lane].call("eth_getLogs", [request])
        except RpcError as exc:
            if start == end and exc.range_too_large:
                return self._split_single_block_logs(lane, start, topics, addresses)
            raise
        if not isinstance(result, list) or any(not isinstance(log, Mapping) for log in result):
            raise RpcError("eth_getLogs returned a non-list or malformed log")
        if len(result) >= MAX_LOGS_PER_RESPONSE:
            if start == end:
                return self._split_single_block_logs(lane, start, topics, addresses)
            raise RpcError(
                f"eth_getLogs returned {len(result)} logs at safety threshold",
                code=-32005,
            )
        return [dict(log) for log in result]

    def _fetch_interval(
        self, lane: str, start: int, end: int,
    ) -> tuple[list[dict[str, Any]], dict[int, dict[str, Any]]]:
        if start < 0 or end < start:
            raise ValueError("invalid inclusive log interval")
        fetch_started = time.monotonic()
        header_lane = f"{lane}_header"
        if header_lane not in self._clients:
            header_lane = lane
        overlap = self._clients[header_lane] is not self._clients[lane]
        boundary_future = (
            self._header_executor.submit(
                self._timed_blocks, header_lane, (start, end),
            )
            if overlap else None
        )
        if boundary_future is None:
            boundaries, boundary_s = self._timed_blocks(
                header_lane, (start, end),
            )
        logs_started = time.monotonic()
        try:
            logs = self._log_query(lane, start, end, tuple(EVENT_TOPICS))
            if NFT_MANAGER_ADDRESSES and NFT_EVENT_TOPICS:
                logs.extend(self._log_query(
                    lane, start, end, tuple(NFT_EVENT_TOPICS),
                    tuple(NFT_MANAGER_ADDRESSES),
                ))
        except BaseException:
            if boundary_future is not None:
                try:
                    boundary_future.result()
                except BaseException:
                    pass
            raise
        logs_s = time.monotonic() - logs_started
        if boundary_future is not None:
            boundaries, boundary_s = boundary_future.result()
        deduplicated: dict[tuple[str, str, int], dict[str, Any]] = {}
        for log in logs:
            block_number = _hex_int(log.get("blockNumber"), "log block number")
            if not start <= block_number <= end:
                raise RpcError("eth_getLogs returned a log outside the requested interval")
            key = self._log_key(log)
            prior = deduplicated.get(key)
            if prior is not None and prior != log:
                raise RpcError("eth_getLogs returned conflicting duplicate log identities")
            deduplicated[key] = log
        logs = sorted(
            deduplicated.values(),
            key=lambda log: (
                _hex_int(log.get("blockNumber"), "log block number"),
                _hex_int(log.get("transactionIndex"), "transaction index"),
                _hex_int(log.get("logIndex"), "log index"),
            ),
        )
        event_blocks = {
            _hex_int(log.get("blockNumber"), "log block number") for log in logs
        }
        missing = event_blocks.difference(boundaries)
        event_headers_started = time.monotonic()
        headers = {
            **boundaries,
            **self._blocks(header_lane, sorted(missing)),
        }
        event_headers_s = time.monotonic() - event_headers_started
        for log in logs:
            number = _hex_int(log["blockNumber"], "log block number")
            observed_hash = _lower(log.get("blockHash"))
            if observed_hash and observed_hash != headers[number]["hash"]:
                raise CanonicalConflict(f"log block {number} changed during interval fetch")
            log["blockHash"] = headers[number]["hash"]
        verify_started = time.monotonic()
        verified_end = self._block(header_lane, end)
        verify_s = time.monotonic() - verify_started
        if verified_end["hash"] != boundaries[end]["hash"]:
            raise CanonicalConflict(f"interval end block {end} changed during fetch")
        with self._status_lock:
            self._fetch_metrics[lane] = {
                "seconds": round(time.monotonic() - fetch_started, 6),
                "logs_seconds": round(logs_s, 6),
                "boundary_headers_seconds": round(boundary_s, 6),
                "event_headers_seconds": round(event_headers_s, 6),
                "end_verify_seconds": round(verify_s, 6),
                "header_log_overlap": overlap,
                "event_header_count": len(missing),
            }
        return logs, headers

    def _known_token_metadata(self, address: str) -> dict[str, Any]:
        universe = getattr(self.market, "universe", None)
        tokens = getattr(universe, "tokens", None)
        metadata = tokens.get(address) if isinstance(tokens, Mapping) else None
        if isinstance(metadata, Mapping):
            known = {
                "symbol": metadata.get("symbol"),
                "decimals": metadata.get("decimals"),
            }
        elif metadata is not None:
            known = {
                "symbol": getattr(metadata, "symbol", None),
                "decimals": getattr(metadata, "decimals", None),
            }
        else:
            known = {}
        if known.get("symbol") is not None and known.get("decimals") is not None:
            return known
        stored = self.store.read().execute(
            "SELECT symbol,decimals FROM token_metadata WHERE address=?",
            (address,),
        ).fetchone()
        if stored is not None:
            if known.get("symbol") is None:
                known["symbol"] = stored["symbol"]
            if known.get("decimals") is None:
                known["decimals"] = stored["decimals"]
        return known

    def _normalize_pool(self, pool: Any) -> dict[str, Any] | None:
        if pool is None:
            return None
        get = pool.get if isinstance(pool, Mapping) else lambda key, default=None: getattr(pool, key, default)
        pool_id = _lower(get("id"))
        protocol = _lower(get("protocol") or get("kind"))
        address = _lower(get("address"))
        token0 = _lower(get("token0"))
        token1 = _lower(get("token1"))
        if not pool_id or protocol not in {"v2", "v3", "v4"} or not address or not token0 or not token1:
            return None
        metadata0 = self._known_token_metadata(token0)
        metadata1 = self._known_token_metadata(token1)
        supplied_metadata = get("metadata_json")
        if isinstance(supplied_metadata, str):
            try:
                supplied_metadata = json.loads(supplied_metadata)
            except ValueError:
                supplied_metadata = None
        extra = dict(supplied_metadata) if isinstance(supplied_metadata, Mapping) else {}
        declared_dynamic = get("dynamic_fee")
        if declared_dynamic is None:
            declared_dynamic = extra.get("dynamic_fee")
        result = {
            "id": pool_id,
            "protocol": protocol,
            "address": address,
            "token0": token0,
            "token1": token1,
            "symbol0": get("symbol0") if get("symbol0") is not None else metadata0.get("symbol"),
            "symbol1": get("symbol1") if get("symbol1") is not None else metadata1.get("symbol"),
            "decimals0": get("decimals0") if get("decimals0") is not None else metadata0.get("decimals"),
            "decimals1": get("decimals1") if get("decimals1") is not None else metadata1.get("decimals"),
            "fee_ppm": get("fee_ppm"),
            "tick_spacing": get("tick_spacing"),
            "hook": _lower(get("hook")) or None,
            "factory": _lower(get("factory")) or None,
            "created_block": get("created_block"),
            "source": get("source") or "observed",
            "metadata_json": extra or None,
        }
        if protocol != "v4":
            if declared_dynamic is not None:
                extra["dynamic_fee"] = bool(declared_dynamic)
                result["metadata_json"] = extra
            return result

        configured_fee = extra.get("configured_fee")
        if configured_fee is None:
            configured_fee = (
                0x800000
                if declared_dynamic is True and result["fee_ppm"] is None
                else result["fee_ppm"]
            )
        try:
            configured_fee = int(configured_fee)
        except (TypeError, ValueError):
            return result
        dynamic = bool(configured_fee & 0x800000)
        if declared_dynamic is not None and bool(declared_dynamic) != dynamic:
            return result
        spacing = resolve_v4_tick_spacing(
            pool_id=pool_id,
            currency0=token0,
            currency1=token1,
            fee=configured_fee,
            hooks=result["hook"] or "",
            tick_spacing=result["tick_spacing"],
        )
        if spacing is None:
            return result
        extra.update({
            "configured_fee": configured_fee,
            "dynamic_fee": dynamic,
        })
        result["fee_ppm"] = None if dynamic else configured_fee
        result["tick_spacing"] = spacing
        result["metadata_json"] = extra
        return result

    def _remember_pool(self, pool: Mapping[str, Any]) -> None:
        with self._cache_lock:
            self._pool_cache[_lower(pool["id"])] = dict(pool)
            self._pool_cache[_lower(pool["address"])] = dict(pool)
            self._pool_misses.discard(_lower(pool["id"]))
            self._pool_misses.discard(_lower(pool["address"]))
            while len(self._pool_cache) > MAX_POOL_CACHE_ENTRIES:
                self._pool_cache.pop(next(iter(self._pool_cache)))

    @staticmethod
    def _identity_verified(pool: Mapping[str, Any]) -> bool:
        metadata = pool.get("metadata_json")
        if isinstance(metadata, str):
            try:
                metadata = json.loads(metadata)
            except ValueError:
                return False
        protocol = _lower(pool.get("protocol"))
        if protocol == "v4":
            if (
                _lower(pool.get("address")) != POOL_MANAGER
                or _lower(pool.get("factory")) != POOL_MANAGER
            ):
                return False
            configured_fee = (
                metadata.get("configured_fee")
                if isinstance(metadata, Mapping) else None
            )
            if configured_fee is None:
                configured_fee = (
                    0x800000
                    if isinstance(metadata, Mapping)
                    and metadata.get("dynamic_fee") is True
                    and pool.get("fee_ppm") is None
                    else pool.get("fee_ppm")
                )
            try:
                configured_fee = int(configured_fee)
                spacing = int(pool.get("tick_spacing"))
            except (TypeError, ValueError):
                return False
            dynamic = bool(configured_fee & 0x800000)
            if (
                isinstance(metadata, Mapping)
                and metadata.get("dynamic_fee") is not None
                and bool(metadata["dynamic_fee"]) != dynamic
            ):
                return False
            return resolve_v4_tick_spacing(
                pool_id=_lower(pool.get("id")),
                currency0=_lower(pool.get("token0")),
                currency1=_lower(pool.get("token1")),
                fee=configured_fee,
                hooks=_lower(pool.get("hook")),
                tick_spacing=spacing,
            ) is not None
        if not isinstance(metadata, Mapping):
            return False
        basis = metadata.get("discovery_basis")
        if basis == "verified_workbench_catalog":
            return True
        factory = _lower(pool.get("factory"))
        if (
            factory not in V2_FACTORIES
            and factory not in V3_FACTORIES
            and factory != SLIPSTREAM_FACTORY
        ):
            return False
        return basis in {
            "factory_creation_event",
            "pinned_factory_getPair_membership",
            "pinned_factory_getPool_membership",
        }

    def _qualified_stored_v4_pool(
        self, candidate: str, stored: Mapping[str, Any],
    ) -> dict[str, Any] | None:
        if _lower(stored.get("protocol")) != "v4":
            return None
        # Live ingestion may already own the writer lock. Reuse that reentrant
        # lock rather than taking a second mutex in the opposite order to a
        # history lane that must persist its recovered identity.
        with self.store.lock:
            current = self.store.pool(candidate) or dict(stored)
            decoded = self._stored_pool(current)
            normalized = self._normalize_pool(decoded)
            if normalized is None or not self._identity_verified(normalized):
                return None
            fields = (
                "id", "protocol", "address", "token0", "token1",
                "symbol0", "symbol1", "decimals0", "decimals1",
                "fee_ppm", "tick_spacing", "hook", "factory",
                "created_block", "source", "metadata_json",
            )
            if any(decoded.get(field) != normalized.get(field) for field in fields):
                # Publish only after the complete, hash-qualified identity is
                # durable. This is independent of a chain checkpoint: the
                # missing legacy word is proven by the pool id itself.
                self.store.upsert_pools([normalized])
                durable = self.store.pool(candidate)
                if durable is None:
                    raise RuntimeError("recovered V4 pool persistence was lost")
                normalized = self._normalize_pool(self._stored_pool(durable))
                if normalized is None or not self._identity_verified(normalized):
                    raise RuntimeError("persisted V4 pool identity is not canonical")
            return normalized

    def _verified_pool(self, candidate: str) -> dict[str, Any] | None:
        pool = self._pool(candidate)
        if pool is not None and not self._identity_verified(pool):
            return None
        return pool

    def _pool(self, candidate: str) -> dict[str, Any] | None:
        candidate = candidate.lower()
        with self._cache_lock:
            if candidate in self._pool_cache:
                return self._pool_cache[candidate]
            if candidate in self._pool_misses:
                return None
        stored = self.store.pool(candidate)
        qualified_stored = (
            self._qualified_stored_v4_pool(candidate, stored)
            if stored is not None else None
        )
        if qualified_stored is not None:
            pool = qualified_stored
        elif stored is not None and self._identity_verified(stored):
            pool = stored
        else:
            lookup = getattr(self.market, "_pool_by_id", None)
            raw_catalog = lookup(candidate) if callable(lookup) else None
            catalog = self._normalize_pool(raw_catalog)
            trusted_sources = {
                "census", "factory-live", "backfill-v4", "backfill-legacy",
                "PoolManager.Initialize", "PositionManager.poolKeys", "transaction.PoolKey",
            }
            source = (
                raw_catalog.get("source")
                if isinstance(raw_catalog, Mapping)
                else getattr(raw_catalog, "source", None)
            )
            if catalog is not None and source in trusted_sources:
                metadata = catalog.get("metadata_json")
                metadata = dict(metadata) if isinstance(metadata, Mapping) else {}
                metadata["discovery_basis"] = "verified_workbench_catalog"
                catalog["metadata_json"] = metadata
            else:
                catalog = None
            if stored is not None and catalog is not None:
                agrees = all(
                    stored.get(field) is None
                    or catalog.get(field) is None
                    or stored[field] == catalog[field]
                    for field in (
                        "protocol", "address", "token0", "token1", "fee_ppm",
                        "tick_spacing", "hook", "factory",
                    )
                )
                if not agrees:
                    catalog = None
            pool = catalog or stored
            if pool is None:
                pool = self._normalize_pool(raw_catalog)
                if pool is not None and source not in trusted_sources:
                    pool = None
            if stored is not None and catalog is not None:
                pool = {
                    key: (stored.get(key) if stored.get(key) is not None else value)
                    for key, value in catalog.items()
                }
                pool["metadata_json"] = catalog["metadata_json"]
        if pool is None:
            with self._cache_lock:
                self._pool_misses.add(candidate)
                if len(self._pool_misses) > MAX_POOL_MISSES:
                    self._pool_misses.pop()
        else:
            self._remember_pool(pool)
        return pool

    def _pools_for_logs(self, logs: Iterable[Mapping[str, Any]]) -> dict[str, dict[str, Any]]:
        candidates: dict[str, None] = {}
        for log in logs:
            emitter = _lower(log.get("address"))
            if emitter:
                candidates.setdefault(emitter, None)
            topics = list(log.get("topics") or ())
            if len(topics) > 1:
                text = _lower(topics[1])
                if len(text) == 66:
                    candidates.setdefault(text, None)
                    candidates.setdefault("0x" + text[-40:], None)
        pools: dict[str, dict[str, Any]] = {}
        for candidate in candidates:
            pool = self._pool(candidate)
            if pool is not None:
                pools[_lower(pool["id"])] = pool
                pools[_lower(pool["address"])] = pool
        return pools

    @staticmethod
    def _abi_address(value: Any) -> str | None:
        if not isinstance(value, str) or not value.startswith("0x") or len(value) != 66:
            return None
        try:
            word = int(value, 16)
        except ValueError:
            return None
        if word >> 160:
            return None
        return "0x" + value[-40:].lower()

    def _optional_identity_batch(
        self, lane: str, calls: list[tuple[str, Sequence[Any]]],
    ) -> list[Any]:
        try:
            return self._rpc_batch(lane, calls)
        except RpcError:
            results: list[Any] = []
            client = self._clients[lane]
            for method, params in calls:
                try:
                    results.append(client.call(method, list(params)))
                except RpcError as exc:
                    if "revert" in str(exc).lower():
                        results.append(None)
                    else:
                        raise
            return results

    def _resolve_unknown_pools(
        self, lane: str, logs: list[dict[str, Any]],
        pools: dict[str, dict[str, Any]], *,
        identity_block: int | None = None,
        identity_hash: str | None = None,
    ) -> None:
        if (identity_block is None) != (identity_hash is None):
            raise ValueError("current identity resolution requires a block and hash")
        unknown = set(unknown_pool_candidates(logs, pools))
        unresolved: dict[str, dict[str, Any] | None] = {
            address: None for address in unknown
        }
        first_blocks: dict[str, int] = {}
        first_hashes: dict[str, str] = {}
        for log in logs:
            number = _hex_int(log.get("blockNumber"), "pool identity block")
            block_hash = _lower(log.get("blockHash"))
            emitter = _lower(log.get("address"))
            candidates = [emitter]
            topics = list(log.get("topics") or ())
            if len(topics) > 1:
                topic = _lower(topics[1])
                if len(topic) == 66:
                    candidates.extend((topic, "0x" + topic[-40:]))
            if emitter in unknown:
                first_blocks[emitter] = min(first_blocks.get(emitter, number), number)
                if first_blocks[emitter] == number:
                    first_hashes[emitter] = block_hash
            for candidate in candidates:
                pool = pools.get(candidate)
                if (
                    pool is None
                    or pool.get("protocol") not in {"v2", "v3"}
                    or self._identity_verified(pool)
                ):
                    continue
                address = _lower(pool["address"])
                unresolved[address] = pool
                first_blocks[address] = min(first_blocks.get(address, number), number)
                if first_blocks[address] == number:
                    first_hashes[address] = block_hash
        if not unresolved:
            return

        unresolved_addresses = set(unresolved)
        for key, pool in tuple(pools.items()):
            if _lower(pool.get("address")) in unresolved_addresses:
                pools.pop(key, None)

        with self._identity_resolve_lock:
            available: list[dict[str, Any]] = []
            resolved: list[dict[str, Any]] = []
            checked: set[str] = set()
            for address in sorted(unresolved):
                current = self._pool(address)
                if current is not None and self._identity_verified(current):
                    available.append(current)
                    self._identity_checked.add(address)
                    continue
                if address in self._identity_checked:
                    continue
                prior = current or unresolved[address]
                observed_block = first_blocks.get(address)
                if observed_block is None:
                    continue
                observed_hash = first_hashes.get(address)
                verification_block = (
                    int(identity_block)
                    if identity_block is not None else observed_block
                )
                verification_hash = (
                    _lower(identity_hash)
                    if identity_hash is not None else observed_hash
                )
                tag = hex(verification_block)
                identity = self._optional_identity_batch(lane, [
                    ("eth_call", [{"to": address, "data": FACTORY_SELECTOR}, tag]),
                    ("eth_call", [{"to": address, "data": TOKEN0_SELECTOR}, tag]),
                    ("eth_call", [{"to": address, "data": TOKEN1_SELECTOR}, tag]),
                ])
                factory = self._abi_address(identity[0])
                token0 = self._abi_address(identity[1])
                token1 = self._abi_address(identity[2])
                if (
                    factory not in V2_FACTORIES
                    and factory not in V3_FACTORIES
                    and factory != SLIPSTREAM_FACTORY
                ) or token0 is None or token1 is None or token0 == token1:
                    checked.add(address)
                    continue
                protocol = "v2" if factory in V2_FACTORIES else "v3"
                if prior is not None and (
                    prior.get("protocol") != protocol
                    or _lower(prior.get("token0")) != token0
                    or _lower(prior.get("token1")) != token1
                ):
                    checked.add(address)
                    continue
                token_args = token0[2:].rjust(64, "0") + token1[2:].rjust(64, "0")
                fee: int | None = None
                spacing: int | None = None
                if protocol == "v2":
                    membership_data = GET_PAIR_SELECTOR + token_args
                    discovery_basis = "pinned_factory_getPair_membership"
                else:
                    details = self._optional_identity_batch(lane, [
                        ("eth_call", [{"to": address, "data": FEE_SELECTOR}, tag]),
                        ("eth_call", [{"to": address, "data": TICK_SPACING_SELECTOR}, tag]),
                    ])
                    try:
                        fee = _hex_int(details[0], "pool fee")
                        spacing_word = _hex_int(details[1], "pool tick spacing")
                    except RpcError:
                        checked.add(address)
                        continue
                    spacing = (
                        spacing_word - (1 << 256)
                        if spacing_word >= 1 << 255 else spacing_word
                    )
                    if not 0 <= fee <= 1_000_000 or not 0 < spacing <= 32767:
                        checked.add(address)
                        continue
                    membership_data = (
                        SLIPSTREAM_GET_POOL_SELECTOR
                        + token_args
                        + f"{spacing & ((1 << 256) - 1):064x}"
                        if factory == SLIPSTREAM_FACTORY
                        else GET_POOL_SELECTOR + token_args + f"{fee:064x}"
                    )
                    discovery_basis = "pinned_factory_getPool_membership"
                membership = self._optional_identity_batch(lane, [
                    ("eth_call", [{"to": factory, "data": membership_data}, tag]),
                ])
                if self._abi_address(membership[0]) != address:
                    checked.add(address)
                    continue

                payload = dict(prior or {})
                metadata = payload.get("metadata_json")
                if isinstance(metadata, str):
                    try:
                        metadata = json.loads(metadata)
                    except ValueError:
                        metadata = None
                verified_metadata = dict(metadata) if isinstance(metadata, Mapping) else {}
                verified_metadata.setdefault(
                    "creation_block_known", payload.get("created_block") is not None,
                )
                verified_metadata.update({
                    "discovery_basis": discovery_basis,
                    "identity_verified_block": verification_block,
                    "identity_verified_hash": verification_hash,
                    "identity_state_basis": (
                        "current_canonical_factory_membership"
                        if identity_block is not None
                        else "first_observed_canonical_factory_membership"
                    ),
                })
                if identity_block is not None:
                    verified_metadata.update({
                        "identity_observed_block": observed_block,
                        "identity_observed_hash": observed_hash,
                    })
                if protocol == "v3":
                    verified_metadata["pool_family"] = (
                        "slipstream" if factory == SLIPSTREAM_FACTORY else "v3"
                    )
                payload.update({
                    "id": address,
                    "protocol": protocol,
                    "address": address,
                    "token0": token0,
                    "token1": token1,
                    "factory": factory,
                    "source": payload.get("source") or (
                        "historical_event_with_current_factory_membership"
                        if identity_block is not None
                        else "first_observed_event_with_pinned_factory_membership"
                    ),
                    "metadata_json": verified_metadata,
                })
                if protocol == "v3":
                    payload["fee_ppm"] = fee
                    payload["tick_spacing"] = spacing
                pool = self._normalize_pool(payload)
                if pool is None:
                    checked.add(address)
                    continue
                resolved.append(pool)
                checked.add(address)

            if identity_block is not None and checked:
                canonical = self._block(lane, int(identity_block))
                if canonical["hash"] != _lower(identity_hash):
                    raise CanonicalConflict(
                        "current pool identity anchor changed during resolution"
                    )
            for pool in (*available, *resolved):
                pools[_lower(pool["id"])] = pool
                pools[_lower(pool["address"])] = pool
            for pool in resolved:
                self._remember_pool(pool)
            self._identity_checked.update(checked)

    @staticmethod
    def _pool_identity_marker(value: Any) -> dict[str, Any] | None:
        if not isinstance(value, str) or not value.startswith(
            DEFERRED_POOL_IDENTITY_PREFIX
        ):
            return None
        try:
            payload = json.loads(value[len(DEFERRED_POOL_IDENTITY_PREFIX):])
        except (TypeError, ValueError):
            return None
        if not isinstance(payload, Mapping):
            return None
        addresses = payload.get("addresses")
        logs = payload.get("logs")
        if not isinstance(addresses, list) or not isinstance(logs, list):
            return None
        normalized = dict(payload)
        normalized["addresses"] = sorted({
            identity for address in addresses
            if isinstance(address, str)
            and (identity := _lower(address)).startswith("0x")
            and len(identity) in {42, 66}
            and all(char in "0123456789abcdef" for char in identity[2:])
        })
        normalized["logs"] = [
            dict(log) for log in logs if isinstance(log, Mapping)
        ]
        return normalized

    @staticmethod
    def _encode_pool_identity_marker(payload: Mapping[str, Any]) -> str:
        return DEFERRED_POOL_IDENTITY_PREFIX + json.dumps(
            dict(payload), separators=(",", ":"), sort_keys=True,
        )

    def _verified_pools_for_deferred_logs(
        self, logs: list[dict[str, Any]],
        events: Iterable[Mapping[str, Any]] = (),
    ) -> dict[str, dict[str, Any]]:
        pools = self._pools_for_logs(logs)
        for key, pool in tuple(pools.items()):
            if not self._identity_verified(pool):
                pools.pop(key, None)
        for event in events:
            raw_pool = event.get("pool")
            if not isinstance(raw_pool, Mapping):
                data = event.get("data")
                raw_pool = (
                    data.get("pool") if isinstance(data, Mapping) else None
                )
            if (
                isinstance(raw_pool, Mapping)
                and self._identity_verified(raw_pool)
            ):
                pool = dict(raw_pool)
                pools[_lower(pool.get("id"))] = pool
                pools[_lower(pool.get("address"))] = pool
        return pools

    def _queue_deferred_pool_identities(
        self, connection: Any, logs: list[dict[str, Any]],
        events: Iterable[Mapping[str, Any]],
    ) -> int:
        pools = self._verified_pools_for_deferred_logs(logs, events)
        unknown = self._unresolved_pool_identities(logs, pools)
        if not unknown:
            return 0
        grouped: dict[str, dict[str, Any]] = {}
        assigned: dict[str, set[str]] = {}
        for log in logs:
            tx_hash = _lower(log.get("transactionHash"))
            identities = {
                identity
                for identity in self._pool_identity_candidates(log).intersection(unknown)
                if tx_hash in assigned.get(identity, set())
                or len(assigned.get(identity, set()))
                < DEFERRED_POOL_IDENTITY_CANDIDATES_PER_POOL
            }
            if not identities:
                continue
            block_hash = _lower(log.get("blockHash"))
            block_number = _hex_int(
                log.get("blockNumber"), "deferred pool identity block",
            )
            if (
                len(tx_hash) != 66 or not tx_hash.startswith("0x")
                or any(char not in "0123456789abcdef" for char in tx_hash[2:])
                or len(block_hash) != 66 or not block_hash.startswith("0x")
                or any(char not in "0123456789abcdef" for char in block_hash[2:])
            ):
                raise RpcError("deferred pool identity log has malformed hashes")
            entry = grouped.setdefault(tx_hash, {
                "tx_hash": tx_hash,
                "block_number": block_number,
                "block_hash": block_hash,
                "addresses": set(),
                "logs": {},
            })
            if (
                entry["block_number"] != block_number
                or entry["block_hash"] != block_hash
            ):
                raise RpcError("transaction logs span conflicting canonical blocks")
            entry["addresses"].update(identities)
            entry["logs"][self._log_key(log)] = dict(log)
            for identity in identities:
                assigned.setdefault(identity, set()).add(tx_hash)
        if not grouped:
            return 0

        self.store._queue_enrichment(connection, [
            {
                "kind": "checkpoint",
                "tx_hash": entry["tx_hash"],
                "block_number": entry["block_number"],
                "block_hash": entry["block_hash"],
            }
            for entry in grouped.values()
        ])
        now = time.time()
        for tx_hash, entry in grouped.items():
            row = connection.execute(
                "SELECT last_error,next_attempt FROM pending_enrichment "
                "WHERE tx_hash=?",
                (tx_hash,),
            ).fetchone()
            if row is None:
                raise RuntimeError("deferred pool identity queue insert was lost")
            prior = self._pool_identity_marker(row["last_error"])
            if prior is None:
                payload: dict[str, Any] = {
                    "addresses": [],
                    "logs": [],
                    "prior_error": row["last_error"],
                    "prior_next_attempt": float(row["next_attempt"]),
                    "identity_attempts": 0,
                }
            else:
                payload = prior
            addresses = set(payload["addresses"])
            addresses.update(entry["addresses"])
            raw_logs = {
                self._log_key(log): dict(log)
                for log in payload["logs"]
            }
            raw_logs.update(entry["logs"])
            payload.update({
                "addresses": sorted(addresses),
                "logs": [
                    raw_logs[key] for key in sorted(raw_logs)
                ],
            })
            connection.execute(
                "UPDATE pending_enrichment SET next_attempt=0,last_error=?,"
                "updated_at=? WHERE tx_hash=?",
                (self._encode_pool_identity_marker(payload), now, tx_hash),
            )
        return len(grouped)

    @staticmethod
    def _stored_v4_modify_log(row: Mapping[str, Any]) -> dict[str, Any] | None:
        pool_id = _lower(row.get("pool_id"))
        custody = _lower(row.get("custody"))
        tx_hash = _lower(row.get("tx_hash"))
        block_hash = _lower(row.get("block_hash"))
        if any(
            len(value) != size
            or not value.startswith("0x")
            or any(char not in "0123456789abcdef" for char in value[2:])
            for value, size in (
                (pool_id, 66), (custody, 42),
                (tx_hash, 66), (block_hash, 66),
            )
        ):
            return None
        data = row.get("data")
        if isinstance(data, str):
            try:
                data = json.loads(data)
            except ValueError:
                return None
        salt = _lower(data.get("salt")) if isinstance(data, Mapping) else ""
        if (
            len(salt) != 66 or not salt.startswith("0x")
            or any(char not in "0123456789abcdef" for char in salt[2:])
        ):
            return None
        try:
            lower = int(row["tick_lower"])
            upper = int(row["tick_upper"])
            liquidity = int(row["liquidity_delta"])
            block_number = int(row["block_number"])
            tx_index = int(row["tx_index"])
            log_index = int(row["log_index"])
        except (TypeError, ValueError):
            return None
        if not (
            -(1 << 23) <= lower < 1 << 23
            and -(1 << 23) <= upper < 1 << 23
            and -(1 << 255) <= liquidity < 1 << 255
            and min(block_number, tx_index, log_index) >= 0
        ):
            return None

        def word(value: int) -> str:
            return f"{value & ((1 << 256) - 1):064x}"

        return {
            "address": POOL_MANAGER,
            "blockNumber": hex(block_number),
            "blockHash": block_hash,
            "transactionHash": tx_hash,
            "transactionIndex": hex(tx_index),
            "logIndex": hex(log_index),
            "topics": [
                V4_MODIFY_LIQUIDITY_TOPIC,
                pool_id,
                "0x" + word(int(custody, 16)),
            ],
            "data": "0x" + "".join(
                word(value)
                for value in (lower, upper, liquidity, int(salt, 16))
            ),
            "removed": False,
        }

    def _seed_deferred_v4_pool_identities(self) -> int:
        now = time.monotonic()
        if (
            self._deferred_identity_seed_complete
            or now < self._deferred_identity_seed_at
        ):
            return 0
        pool_rows = self.store.read().execute(
            "SELECT DISTINCT pool_id FROM events INDEXED BY events_pool_time_idx "
            "WHERE pool_id>? AND protocol='v4' "
            "AND kind IN ('add','remove','collect') "
            "ORDER BY pool_id LIMIT ?",
            (
                self._deferred_identity_seed_after_id,
                DEFERRED_POOL_IDENTITY_BATCH,
            ),
        ).fetchall()
        if not pool_rows:
            self._deferred_identity_seed_complete = True
            return 0
        next_after = _lower(pool_rows[-1]["pool_id"])
        logs: list[dict[str, Any]] = []
        reader = self.store.read()
        for pool_row in pool_rows:
            pool_id = _lower(pool_row["pool_id"])
            stored = self.store.pool(pool_id)
            if stored is not None and self._identity_verified(stored):
                continue
            pending = reader.execute(
                "SELECT 1 FROM pending_enrichment "
                "WHERE last_error GLOB 'pool_identity_pending:*' "
                "AND instr(last_error,?)>0 LIMIT 1",
                (pool_id,),
            ).fetchone()
            if pending is not None:
                continue
            rows = reader.execute(
                "SELECT * FROM events WHERE pool_id=? AND protocol='v4' "
                "AND kind IN ('add','remove','collect') "
                "ORDER BY timestamp DESC,id DESC LIMIT ?",
                (
                    pool_id,
                    DEFERRED_POOL_IDENTITY_CANDIDATES_PER_POOL,
                ),
            ).fetchall()
            for row in rows:
                log = self._stored_v4_modify_log(dict(row))
                if log is not None:
                    logs.append(log)
        if logs:
            with self.store.transaction() as connection:
                queued = self._queue_deferred_pool_identities(
                    connection, logs, (),
                )
        else:
            queued = 0
        self._deferred_identity_seed_after_id = next_after
        self._deferred_identity_seed_at = (
            now + DEFERRED_POOL_IDENTITY_SEED_BUSY_S
        )
        return queued


    def _pending_pool_identity_replays(
        self, limit: int = DEFERRED_POOL_IDENTITY_BATCH,
    ) -> list[dict[str, Any]]:
        rows = self.store.read().execute(
            "SELECT * FROM pending_enrichment WHERE next_attempt<=? "
            "AND last_error GLOB 'pool_identity_pending:*' "
            "ORDER BY block_number,tx_hash LIMIT ?",
            (
                time.time(),
                max(1, min(int(limit), DEFERRED_POOL_IDENTITY_BATCH)),
            ),
        ).fetchall()
        return [dict(row) for row in rows]

    def _mark_pool_identity_replay_error(
        self, row: Mapping[str, Any], marker: str,
        payload: Mapping[str, Any], error: BaseException | str,
    ) -> None:
        attempts = int(payload.get("identity_attempts", 0)) + 1
        updated = dict(payload)
        updated.update({
            "identity_attempts": attempts,
            "identity_error": str(error)[:1000],
        })
        delay = min(300.0, 2.0 ** min(attempts, 8))
        with self.store.transaction() as connection:
            connection.execute(
                "UPDATE pending_enrichment SET next_attempt=?,last_error=?,"
                "updated_at=? WHERE tx_hash=? AND last_error=?",
                (
                    time.time() + delay,
                    self._encode_pool_identity_marker(updated),
                    time.time(), _lower(row["tx_hash"]), marker,
                ),
            )

    def _finish_pool_identity_replay(
        self, row: Mapping[str, Any], marker: str,
        payload: Mapping[str, Any], processed: set[str],
    ) -> None:
        remaining = set(payload["addresses"]).difference(processed)
        with self.store.transaction() as connection:
            if remaining:
                updated = dict(payload)
                updated.update({
                    "addresses": sorted(remaining),
                    "logs": [
                        log for log in payload["logs"]
                        if self._pool_identity_candidates(log).intersection(remaining)
                    ],
                    "identity_attempts": 0,
                })
                updated.pop("identity_error", None)
                connection.execute(
                    "UPDATE pending_enrichment SET next_attempt=0,last_error=?,"
                    "updated_at=? WHERE tx_hash=? AND last_error=?",
                    (
                        self._encode_pool_identity_marker(updated), time.time(),
                        _lower(row["tx_hash"]), marker,
                    ),
                )
                return
            placeholders = ",".join("?" for _ in LP_ENRICHMENT_KINDS)
            needs_enrichment = connection.execute(
                f"SELECT 1 FROM events WHERE tx_hash=? AND kind IN "
                f"({placeholders}) LIMIT 1",
                (_lower(row["tx_hash"]), *sorted(LP_ENRICHMENT_KINDS)),
            ).fetchone()
            if needs_enrichment is not None:
                connection.execute(
                    "UPDATE pending_enrichment SET next_attempt=?,last_error=?,"
                    "updated_at=? WHERE tx_hash=? AND last_error=?",
                    (
                        float(payload.get("prior_next_attempt", 0.0)),
                        payload.get("prior_error"), time.time(),
                        _lower(row["tx_hash"]), marker,
                    ),
                )
                return
            removed = connection.execute(
                "DELETE FROM pending_enrichment WHERE tx_hash=? AND last_error=?",
                (_lower(row["tx_hash"]), marker),
            ).rowcount
            if removed:
                self.store._bump(
                    connection, "pending_enrichment", -int(removed),
                )

    def _resolve_deferred_pool_identities_once(self) -> bool:
        self._seed_deferred_v4_pool_identities()
        rows = self._pending_pool_identity_replays()
        if not rows:
            return False
        with self._current_receipt_lock:
            if self._current_pool_pending:
                return False
        started = time.monotonic()
        try:
            identity_block = _hex_int(
                self._clients["pool"].call("eth_blockNumber", []),
                "current pool identity block",
            )
            identity_header = self._block("pool", identity_block)
        except Exception as exc:
            for row in rows:
                marker = str(row.get("last_error") or "")
                payload = self._pool_identity_marker(marker)
                if payload is not None:
                    self._mark_pool_identity_replay_error(
                        row, marker, payload, exc,
                    )
            self._set_runtime("pool_identity", error=exc)
            return True

        address_budget = 1
        replayed = 0
        rejected = 0
        inserted_count = 0
        batch_error: Exception | None = None
        for row in rows:
            if address_budget <= 0 or self._stop.is_set():
                break
            marker = str(row.get("last_error") or "")
            payload = self._pool_identity_marker(marker)
            if payload is None:
                continue
            addresses = set(payload["addresses"][:address_budget])
            if not addresses:
                self._finish_pool_identity_replay(row, marker, payload, set())
                continue
            address_budget -= len(addresses)
            try:
                stored = self.store.read().execute(
                    "SELECT hash,parent_hash,timestamp FROM blocks WHERE number=?",
                    (int(row["block_number"]),),
                ).fetchone()
                if stored is None or _lower(stored["hash"]) != _lower(
                    row["block_hash"]
                ):
                    self._finish_pool_identity_replay(
                        row, marker, payload, set(payload["addresses"]),
                    )
                    rejected += len(addresses)
                    continue
                header = {
                    "number": hex(int(row["block_number"])),
                    "hash": _lower(stored["hash"]),
                    "parentHash": _lower(stored["parent_hash"]),
                    "timestamp": hex(int(stored["timestamp"])),
                }
                logs = [
                    dict(log) for log in payload["logs"]
                    if self._pool_identity_candidates(log).intersection(addresses)
                ]
                observed_addresses: set[str] = set()
                for log in logs:
                    identities = self._pool_identity_candidates(log).intersection(
                        addresses
                    )
                    if (
                        not identities
                        or _lower(log.get("transactionHash"))
                        != _lower(row["tx_hash"])
                        or _hex_int(log.get("blockNumber"), "replay block")
                        != int(row["block_number"])
                        or _lower(log.get("blockHash")) != header["hash"]
                        or log.get("removed")
                    ):
                        raise CanonicalConflict(
                            "deferred pool log no longer matches its canonical queue"
                        )
                    observed_addresses.update(identities)
                if observed_addresses != addresses:
                    raise RpcError("deferred pool identity queue omitted raw logs")
                pools = self._verified_pools_for_deferred_logs(logs)
                legacy_addresses = {
                    address for address in addresses if len(address) == 42
                }
                if legacy_addresses:
                    self._resolve_unknown_pools(
                        "pool", logs, pools,
                        identity_block=identity_block,
                        identity_hash=identity_header["hash"],
                    )
                resolved_v4: list[dict[str, Any]] = []
                for pool_id in sorted(
                    address for address in addresses if len(address) == 66
                ):
                    pool = self._verified_pool(pool_id)
                    if pool is not None:
                        pools[pool_id] = pool
                        resolved_v4.append(pool)
                        continue
                    pool = self._resolve_current_v4_pool(
                        pool_id, identity_block, identity_header["hash"],
                    )
                    if pool is None:
                        pool = self._resolve_current_v4_input(
                            pool_id, logs, _lower(row["tx_hash"]),
                        )
                    if pool is not None:
                        pools[pool_id] = pool
                        resolved_v4.append(pool)
                canonical = self._block("pool", identity_block)
                if canonical["hash"] != identity_header["hash"]:
                    raise CanonicalConflict(
                        "current V4 pool identity anchor changed during resolution"
                    )
                events = self._decode(
                    "pool", logs, {int(row["block_number"]): header},
                    resolve_unknown=False,
                    known_pools=resolved_v4,
                )
                inserted = self.store.ingest(
                    [header], events, lane="history",
                )
                inserted_count += len(inserted)
                resolved_pools = {
                    address: self._verified_pool(address)
                    for address in addresses
                }
                resolved_count = sum(
                    pool is not None for pool in resolved_pools.values()
                )
                replayed += resolved_count
                rejected += len(addresses) - resolved_count
                self._finish_pool_identity_replay(
                    row, marker, payload, addresses,
                )
                publish = list(inserted)
                publish.extend(
                    {"pool": self._stored_pool(stored)}
                    for pool in resolved_v4
                    if (stored := self.store.pool(str(pool["id"]))) is not None
                )
                if publish:
                    self._publish_event_pools(publish)
            except Exception as exc:
                batch_error = exc
                self._mark_pool_identity_replay_error(
                    row, marker, payload, exc,
                )
        self._set_runtime(
            "pool_identity", error=batch_error,
            latency=time.monotonic() - started,
            pool_identity_replayed=replayed,
            pool_identity_rejected=rejected,
            pool_identity_inserted_events=inserted_count,
        )
        return True

    def _decode_current(
        self, logs: list[dict[str, Any]], headers: dict[int, dict[str, Any]],
    ) -> list[dict[str, Any]]:
        pools = self._pools_for_logs(logs)
        selected: list[dict[str, Any]] = []
        for log in logs:
            emitter = _lower(log.get("address"))
            candidates = {emitter}
            topics = list(log.get("topics") or ())
            if len(topics) > 1:
                topic = _lower(topics[1])
                if len(topic) == 66:
                    candidates.update((topic, "0x" + topic[-40:]))
            if (
                emitter in CURRENT_TRUSTED_EVENT_EMITTERS
                or any(candidate in pools for candidate in candidates)
            ):
                selected.append(dict(log))
        if not selected:
            return []
        return self._decode(
            "head", selected, headers, resolve_unknown=False,
        )

    def _decode(
        self,
        lane: str,
        logs: list[dict[str, Any]],
        headers: dict[int, dict[str, Any]],
        *,
        receipts: dict[str, dict[str, Any]] | None = None,
        traces: dict[str, dict[str, Any]] | None = None,
        resolve_unknown: bool = True,
        known_pools: Iterable[Mapping[str, Any]] = (),
    ) -> list[dict[str, Any]]:
        pools = self._pools_for_logs(logs)
        for pool in known_pools:
            if self._identity_verified(pool):
                pools[_lower(pool.get("id"))] = dict(pool)
                pools[_lower(pool.get("address"))] = dict(pool)
        if resolve_unknown:
            self._resolve_unknown_pools(lane, logs, pools)
        else:
            for key, pool in tuple(pools.items()):
                if not self._identity_verified(pool):
                    pools.pop(key, None)
        events = [dict(event) for event in decode_logs(logs, pools, headers, receipts, traces)]
        activation_logs: list[dict[str, Any]] = []
        activated: set[str] = set()
        for event in events:
            data = event.get("data")
            raw_pool = (
                data.get("pool")
                if isinstance(data, Mapping) and isinstance(data.get("pool"), Mapping)
                else None
            )
            if raw_pool is None:
                continue
            supplied = (
                pools.get(_lower(raw_pool.get("id")))
                or pools.get(_lower(raw_pool.get("address")))
                or self._normalize_pool(raw_pool)
            )
            if (
                supplied is None
                or supplied.get("protocol") not in {"v2", "v3"}
                or self._identity_verified(supplied)
            ):
                continue
            address = _lower(supplied["address"])
            pools[_lower(supplied["id"])] = supplied
            pools[address] = supplied
            if address in activated:
                continue
            block = int(event["block_number"])
            activation_logs.append({
                "address": address,
                "blockNumber": hex(block),
                "blockHash": _lower(event.get("block_hash")) or headers[block]["hash"],
                "topics": [],
            })
            activated.add(address)
        if activation_logs and resolve_unknown:
            self._resolve_unknown_pools(lane, activation_logs, pools)
            events = [
                dict(event)
                for event in decode_logs(logs, pools, headers, receipts, traces)
            ]
        remembered: dict[str, dict[str, Any]] = {}
        for event in events:
            pool: dict[str, Any] | None = None
            data = event.get("data")
            if isinstance(data, Mapping) and isinstance(data.get("pool"), Mapping):
                raw_pool = data["pool"]
                pool = (
                    pools.get(_lower(raw_pool.get("id")))
                    or pools.get(_lower(raw_pool.get("address")))
                )
                if pool is None:
                    supplied = self._normalize_pool(raw_pool)
                    if supplied is not None and self._identity_verified(supplied):
                        pool = supplied
                normalized_data = dict(data)
                if pool is None:
                    normalized_data.pop("pool", None)
                else:
                    normalized_data["pool"] = pool
                event["data"] = normalized_data
            if pool is None and event.get("pool_id"):
                pool_id = _lower(event["pool_id"])
                pool = pools.get(pool_id) or self._verified_pool(pool_id)
            if pool is not None:
                event["pool"] = pool
                remembered.setdefault(_lower(pool["id"]), pool)
        for pool in remembered.values():
            self._remember_pool(pool)
        return events

    def _publish_event_pools(self, events: Iterable[Mapping[str, Any]]) -> None:
        register = getattr(self.market, "register_index_pool", None)
        if not callable(register):
            return
        published: set[str] = set()
        try:
            for event in events:
                pool = event.get("pool")
                pool_id = str(pool.get("id") or "") if isinstance(pool, Mapping) else ""
                if (
                    isinstance(pool, Mapping)
                    and pool_id not in published
                    and self._identity_verified(pool)
                ):
                    register(dict(pool))
                    published.add(pool_id)
        except BaseException:
            self._publish_after_id = ""
            raise

    @staticmethod
    def _stored_pool(row: Mapping[str, Any]) -> dict[str, Any]:
        pool = dict(row)
        metadata = pool.get("metadata_json")
        if isinstance(metadata, str):
            try:
                pool["metadata_json"] = json.loads(metadata)
            except ValueError:
                pass
        return pool

    def _publish_stored_pool_page(self, limit: int = 256) -> bool:
        register = getattr(self.market, "register_index_pool", None)
        if not callable(register):
            return False
        rows = self.store.read().execute(
            "SELECT * FROM pools WHERE id>? ORDER BY id LIMIT ?",
            (self._publish_after_id, max(1, min(int(limit), 1024))),
        ).fetchall()
        if not rows:
            return False
        for row in rows:
            pool = self._stored_pool(row)
            if self._identity_verified(pool):
                register(pool)
        self._publish_after_id = str(rows[-1]["id"])
        return True


    def _recover_legacy_v4_pool_page(
        self, limit: int = LEGACY_V4_IDENTITY_BATCH,
    ) -> bool:
        if self._legacy_v4_identity_complete:
            return False
        rows = self.store.read().execute(
            "SELECT * FROM pools WHERE protocol='v4' AND tick_spacing IS NULL "
            "AND id>? ORDER BY id LIMIT ?",
            (
                self._legacy_v4_identity_after_id,
                max(1, min(int(limit), LEGACY_V4_IDENTITY_BATCH)),
            ),
        ).fetchall()
        if not rows:
            self._legacy_v4_identity_complete = True
            return False
        recovered: list[dict[str, Any]] = []
        for row in rows:
            pool = self._qualified_stored_v4_pool(str(row["id"]), dict(row))
            if pool is not None:
                self._remember_pool(pool)
                recovered.append(pool)
        self._legacy_v4_identity_after_id = str(rows[-1]["id"])
        if recovered:
            self._publish_event_pools({"pool": pool} for pool in recovered)
        return True
    def _publish_token_pools(self, address: str, register: Callable[[dict[str, Any]], Any]) -> None:
        after = ""
        while not self._stop.is_set():
            rows = self.store.read().execute(
                "SELECT * FROM pools WHERE id>? AND (token0=? OR token1=?) "
                "ORDER BY id LIMIT 256",
                (after, address, address),
            ).fetchall()
            if not rows:
                return
            for row in rows:
                pool = self._stored_pool(row)
                if self._identity_verified(pool):
                    register(pool)
            after = str(rows[-1]["id"])

    def _sync_market_reorg(self) -> bool:
        worked = False
        unregister = getattr(self.market, "unregister_index_pools", None)
        while not self._stop.is_set():
            pool_ids = self.store.pending_pool_unpublishes(256)
            if not pool_ids:
                break
            if not callable(unregister):
                raise RuntimeError(
                    "market does not support unregister_index_pools required for reorg recovery"
                )
            unregister(pool_ids)
            self.store.complete_pool_unpublishes(pool_ids)
            worked = True
        epoch = int(self.store.status().get("epoch", 0))
        if epoch != self._market_epoch:
            self._market_epoch = epoch
            worked = True
        return worked

    def _resize_after_success(
        self, lane: str, log_count: int, store_seconds: float = 0.0,
        scanned_blocks: int | None = None,
    ) -> None:
        current = self._live_chunk if lane == "live" else self._history_chunk
        minimum = LIVE_MIN_CHUNK if lane == "live" else HISTORY_MIN_CHUNK
        maximum = LIVE_MAX_CHUNK if lane == "live" else HISTORY_MAX_CHUNK
        sample_blocks = max(
            1, current if scanned_blocks is None else int(scanned_blocks),
        )
        if store_seconds > MAX_INTERVAL_STORE_SECONDS and current > minimum:
            # Size the next transaction from measured durable-store throughput.
            # This bounds both writers' lock residency during traffic or memory
            # pressure, while successful cheap intervals grow again below.
            resized = max(
                minimum,
                min(
                    current - 1,
                    int(
                        sample_blocks * MAX_INTERVAL_STORE_SECONDS
                        / max(store_seconds, 1e-9)
                    ),
                ),
            )
            if lane == "live":
                self._live_chunk = resized
            else:
                self._history_chunk = resized
            return
        # Size by measured density and leave 20% response headroom. This grows
        # dense ranges substantially without bouncing between a blind doubling
        # and the provider's bounded 10k-log rejection.
        count = max(0, int(log_count))
        if count == 0:
            candidate = current * 2
        else:
            candidate = min(
                current * 2,
                sample_blocks * (MAX_LOGS_PER_RESPONSE * 4 // 5) // count,
            )
        if candidate <= current:
            return
        resized = min(maximum, max(current + 1, candidate))
        if lane == "live":
            self._live_chunk = resized
        else:
            self._history_chunk = resized

    def _shrink(self, lane: str) -> bool:
        if lane == "live":
            if self._live_chunk <= LIVE_MIN_CHUNK:
                return False
            self._live_chunk = max(LIVE_MIN_CHUNK, self._live_chunk // 2)
        else:
            if self._history_chunk <= HISTORY_MIN_CHUNK:
                return False
            self._history_chunk = max(HISTORY_MIN_CHUNK, self._history_chunk // 2)
        return True

    def _sparse_common_ancestor(self, client: Any, maximum: int) -> tuple[int, dict[str, Any]]:
        below = maximum + 1
        connection = self.store.read()
        while not self._stop.is_set():
            rows = connection.execute(
                "SELECT number,hash FROM blocks WHERE number<? ORDER BY number DESC LIMIT 100",
                (below,),
            ).fetchall()
            if not rows:
                genesis = _header(client.call("eth_getBlockByNumber", ["0x0", False]), 0)
                return 0, genesis
            calls = [("eth_getBlockByNumber", [hex(int(row["number"])), False]) for row in rows]
            if callable(getattr(client, "batch", None)):
                results = client.batch(calls)
            else:
                results = [client.call(method, params) for method, params in calls]
            for row, raw in zip(rows, results):
                canonical = _header(raw, int(row["number"]))
                if canonical["hash"] == row["hash"]:
                    return int(row["number"]), canonical
            below = int(rows[-1]["number"])
        raise RpcError("indexer closed while finding common ancestor")

    def _find_common_ancestor(self, number: int, old_hash: str) -> tuple[int, dict[str, Any], str]:
        client = self._clients["live"]
        try:
            orphan = client.call("eth_getBlockByHash", [old_hash, False]) if old_hash else None
        except Exception:
            orphan = None
        while isinstance(orphan, Mapping) and not self._stop.is_set():
            old = _header(orphan)
            old_number = _number(old)
            if old_number > number:
                orphan = client.call("eth_getBlockByHash", [old["parentHash"], False])
                continue
            canonical = _header(
                client.call("eth_getBlockByNumber", [hex(old_number), False]), old_number,
            )
            if canonical["hash"] == old["hash"]:
                return old_number, canonical, "orphan-parent-walk"
            if old_number == 0:
                return 0, canonical, "genesis"
            orphan = client.call("eth_getBlockByHash", [old["parentHash"], False])
        ancestor, header = self._sparse_common_ancestor(client, number)
        return ancestor, header, "sparse-anchor"

    def _recover_reorg(
        self, cursor: Mapping[str, Any], reason: str, *, supplied_anchor: bool = False,
    ) -> None:
        with self._reorg_lock:
            current = dict(cursor) if supplied_anchor else (self.store.cursor("live") or dict(cursor))
            number = int(current.get("block_number", cursor.get("block_number", 0)))
            old_hash = _lower(current.get("block_hash") or cursor.get("block_hash"))
            head_number = _hex_int(
                self._clients["live"].call("eth_blockNumber", []), "head",
            )
            if number <= head_number:
                canonical = self._block("live", number)
                if old_hash and canonical["hash"] == old_hash:
                    return
            previous_history = self.store.cursor("history") or {}
            ancestor, header, method = self._find_common_ancestor(
                min(number, head_number), old_hash,
            )
            self.store.rollback(ancestor, header=header)
            self.store.ingest(
                [header], [], lane="live",
                cursor={
                    "block_number": ancestor, "block_hash": header["hash"],
                    "timestamp": _timestamp(header),
                },
            )
            history = self.store.cursor("history")
            if not history or history.get("next_to") is None:
                target_value = previous_history.get("target_block")
                target = int(target_value) if target_value is not None else 0
                target_timestamp = int(previous_history.get("target_timestamp") or 0)
                target_pending = bool(previous_history.get("target_pending"))
                self.store.ingest(
                    [header], [], lane="history",
                    cursor={
                        "next_to": ancestor - 1,
                        "low_block": ancestor,
                        "block_hash": header["hash"],
                        "target_block": target if not target_pending else None,
                        "target_timestamp": target_timestamp,
                        "target_pending": target_pending,
                        "origin_head": previous_history.get("origin_head", ancestor),
                        "complete": not target_pending and ancestor - 1 < target,
                        "has_coverage": False,
                    },
                )
            self._sync_market_reorg()
            with self._cache_lock:
                self._pool_cache.clear()
                self._pool_misses.clear()
            with self._identity_resolve_lock:
                self._identity_checked.clear()
            self._set_runtime(
                "live", reorg={
                    "ancestor": ancestor, "method": method, "reason": reason,
                    "at": int(time.time()),
                },
            )

    def _scan_live_once(self) -> bool:
        started = time.monotonic()
        observed_at = time.time()
        cursor, cursor_epoch = self.store.cursor_state("live")
        if not cursor or cursor.get("block_number") is None:
            raise RuntimeError("live cursor is not initialized")
        current_number = int(cursor["block_number"])
        current_hash = _lower(cursor.get("block_hash"))
        head_number = _hex_int(
            self._clients["live"].call("eth_blockNumber", []), "head",
        )
        head = self._block("live_header", head_number)
        # Publish the observed head even when the following log scan fails.
        # Otherwise a stalled cursor can misleadingly keep reporting zero lag.
        with self._status_lock:
            prior_error = self._errors.get("live")
        self._set_runtime(
            "live", error=prior_error, head=head_number, head_hash=head["hash"],
            head_timestamp=_timestamp(head),
            lag_s=max(
                0, _timestamp(head)
                - int(cursor.get("timestamp") or _timestamp(head)),
            ),
        )
        if head_number < current_number:
            self._recover_reorg(cursor, "chain head moved behind live cursor")
            return True
        if head_number == current_number:
            if head["hash"] != current_hash:
                self._recover_reorg(
                    cursor, "stored live cursor hash is no longer canonical",
                )
                return True
            self._set_runtime(
                "live", latency=time.monotonic() - started,
                head=head_number, head_hash=head["hash"],
                head_timestamp=_timestamp(head), lag_s=0,
            )
            return False
        start = current_number + 1
        requested_chunk = self._live_chunk
        end = min(head_number, start + requested_chunk - 1)
        with self._status_lock:
            self._fetch_metrics.pop("live", None)
        try:
            fetch_started = time.monotonic()
            logs, headers = self._fetch_interval("live", start, end)
            fetch_s = time.monotonic() - fetch_started
            if headers[start]["parentHash"] != current_hash:
                raise CanonicalConflict(f"block {start} does not extend live cursor")
            decode_started = time.monotonic()
            events = self._decode(
                "live", logs, headers, resolve_unknown=False,
            )
            decode_s = time.monotonic() - decode_started
            interval_end = headers[end]
            store_started = time.monotonic()
            with self.store.transaction() as connection:
                store_acquired = time.monotonic()
                store_wait_s = store_acquired - store_started
                deferred_identities = self._queue_deferred_pool_identities(
                    connection, logs, events,
                )
                inserted = self.store.ingest(
                    headers, events, lane="live",
                    cursor={
                        "from_block": start, "to_block": end,
                        "block_number": end, "block_hash": interval_end["hash"],
                        "_expected_block_number": current_number,
                        "_expected_block_hash": current_hash,
                        "_expected_epoch": cursor_epoch,
                        "timestamp": _timestamp(interval_end),
                    },
                )
            store_s = time.monotonic() - store_acquired
        except CanonicalConflict as exc:
            self._recover_reorg(cursor, str(exc))
            return True
        except RpcError as exc:
            if exc.range_too_large and self._shrink("live"):
                return True
            raise
        postprocess_started = time.monotonic()
        try:
            self._publish_event_pools(inserted)
        except Exception as exc:
            self._set_runtime("metadata", error=exc)
        if self.v3_balances:
            latest_by_pool: dict[str, dict[str, Any]] = {}
            for event in inserted:
                pool = event.get("pool")
                if isinstance(pool, Mapping) and pool.get("protocol") == "v3":
                    latest_by_pool[str(pool["id"])] = event
            self.store.queue_v3_balances(
                latest_by_pool, end, interval_end["hash"],
            )
        postprocess_s = time.monotonic() - postprocess_started
        self._resize_after_success(
            "live", len(logs), store_s, end - start + 1,
        )
        elapsed = time.monotonic() - started
        blocks = end - start + 1
        with self._status_lock:
            fetch_detail = dict(self._fetch_metrics.get("live", {}))
        phases = {
            "fetch": fetch_s,
            "decode": decode_s,
            "store": store_s,
            "writer_wait": store_wait_s,
            "postprocess": postprocess_s,
        }
        scan = {
            "observed_at": observed_at,
            "from_block": start,
            "to_block": end,
            "blocks": blocks,
            "logs": len(logs),
            "decoded_events": len(events),
            "inserted_events": len(inserted),
            "deferred_pool_identity_transactions": deferred_identities,
            "requested_chunk": requested_chunk,
            "next_chunk": self._live_chunk,
            "seconds": round(elapsed, 6),
            "fetch_seconds": round(fetch_s, 6),
            "decode_seconds": round(decode_s, 6),
            "store_seconds": round(store_s, 6),
            "store_lock_wait_seconds": round(store_wait_s, 6),
            "postprocess_seconds": round(postprocess_s, 6),
            "blocks_per_second": round(blocks / max(elapsed, 1e-9), 3),
            "events_per_second": round(
                len(events) / max(elapsed, 1e-9), 3,
            ),
            "bottleneck": max(phases, key=lambda phase: phases[phase]),
            "bottleneck_seconds": round(max(phases.values()), 6),
            "head_lag_blocks": max(0, head_number - end),
            "fetch": fetch_detail,
        }
        self._set_runtime(
            "live", latency=elapsed,
            head=head_number, head_hash=head["hash"],
            head_timestamp=_timestamp(head),
            lag_s=max(0, _timestamp(head) - _timestamp(interval_end)),
            live_scan=scan,
        )
        return True

    def _storage_allows_history(self) -> bool:
        if str(self.store.path) == ":memory:":
            return True
        try:
            free = int(shutil.disk_usage(self.store.path.resolve().parent).free)
        except OSError as exc:
            self._set_runtime(
                "storage", error=f"cannot measure index storage: {exc}",
                storage_paused=True,
                storage_free_bytes=None,
                storage_reserve_bytes=self.history_disk_reserve_bytes,
            )
            return False
        paused = free < self.history_disk_reserve_bytes
        self._set_runtime(
            "storage",
            error=(
                f"history paused: {free} free bytes below "
                f"{self.history_disk_reserve_bytes} byte reserve"
                if paused else None
            ),
            storage_paused=paused,
            storage_free_bytes=free,
            storage_reserve_bytes=self.history_disk_reserve_bytes,
        )
        return not paused

    def _recent_catchup_pending(self) -> bool:
        cursor = self.store.cursor("live")
        if not cursor or cursor.get("block_number") is None:
            return False
        with self._feed_condition:
            observed = self._observed_last_header
            observed_number = _number(observed) if observed is not None else 0
        with self._status_lock:
            head = max(observed_number, int(self._runtime_status.get("head") or 0))
            lag = max(0, head - int(cursor["block_number"]))
            pending = lag > RECENT_CATCHUP_PRIORITY_BLOCKS
            self._runtime_status.update({
                "recent_catchup_priority": pending,
                "recent_catchup_lag_blocks": lag,
                "history_scheduling": "recent_gap_first" if pending else "concurrent",
            })
        return pending

    def _scan_history_once(self) -> bool:
        if self._recent_catchup_pending():
            return False
        started = time.monotonic()
        cursor, cursor_epoch = self.store.cursor_state("history")
        if not cursor or cursor.get("next_to") is None:
            raise RuntimeError("history cursor is not initialized")
        if not self._history_verified:
            self._verify_chain("history")
            self._history_verified = True
        if cursor.get("target_pending"):
            origin = self._block(
                "history_header", int(cursor["origin_head"]),
            )
            target_block, target_timestamp = self._history_target(origin)
            resolved = {
                **cursor,
                "target_block": target_block,
                "target_timestamp": target_timestamp,
                "target_pending": False,
                "complete": int(cursor["next_to"]) < target_block,
                "_expected_next_to": cursor["next_to"],
                "_expected_epoch": cursor_epoch,
            }
            self.store.ingest([origin], [], lane="history", cursor=resolved)
            self._set_runtime("history", latency=time.monotonic() - started)
            return True
        if cursor.get("complete"):
            return False
        if not self._storage_allows_history():
            return False
        # Archive enrichment cannot gate raw history, but the missing recent
        # interval has priority over extending the older lookback.
        end = int(cursor["next_to"])
        target = int(cursor["target_block"])
        if end < target:
            completed = dict(cursor)
            completed["complete"] = True
            completed["_expected_next_to"] = cursor["next_to"]
            completed["_expected_epoch"] = cursor_epoch
            anchor_number = max(
                0, int(cursor.get("low_block", target)) - 1,
            )
            anchor = self._block("history_header", anchor_number)
            self.store.ingest(
                [anchor], [], lane="history", cursor=completed,
            )
            return True
        start = max(target, end - self._history_chunk + 1)
        requested_chunk = self._history_chunk
        with self._status_lock:
            self._fetch_metrics.pop("history", None)
        try:
            fetch_started = time.monotonic()
            logs, headers = self._fetch_interval("history", start, end)
            fetch_s = time.monotonic() - fetch_started
        except RpcError as exc:
            if exc.range_too_large and self._shrink("history"):
                return True
            raise
        prior_low = cursor.get("low_block")
        prior_hash = _lower(cursor.get("block_hash"))
        if (
            cursor.get("has_coverage")
            and prior_low is not None
            and int(prior_low) == end + 1
            and prior_hash
        ):
            prior_header = self._block(
                "history_header", int(prior_low),
            )
            if (
                prior_header["hash"] != prior_hash
                or prior_header["parentHash"] != headers[end]["hash"]
            ):
                self._recover_reorg(
                    {
                        "block_number": int(prior_low),
                        "block_hash": prior_hash,
                    },
                    "history boundary no longer joins canonical chain",
                    supplied_anchor=True,
                )
                return True
        decode_started = time.monotonic()
        events = self._decode(
            "history", logs, headers, resolve_unknown=False,
        )
        decode_s = time.monotonic() - decode_started
        complete = start <= target
        store_started = time.monotonic()
        with self.store.transaction() as connection:
            store_acquired = time.monotonic()
            store_wait_s = store_acquired - store_started
            deferred_identities = self._queue_deferred_pool_identities(
                connection, logs, events,
            )
            inserted = self.store.ingest(
                headers, events, lane="history",
                cursor={
                    **cursor,
                    "from_block": start, "to_block": end,
                    "next_to": start - 1, "low_block": start,
                    "block_number": start, "block_hash": headers[start]["hash"],
                    "_expected_next_to": end,
                    "_expected_epoch": cursor_epoch,
                    "complete": complete,
                    "has_coverage": True,
                },
            )
        store_s = time.monotonic() - store_acquired
        publish_started = time.monotonic()
        try:
            self._publish_event_pools(inserted)
        except Exception as exc:
            self._set_runtime("metadata", error=exc)
        publish_s = time.monotonic() - publish_started
        self._resize_after_success(
            "history", len(logs), store_s, end - start + 1,
        )
        elapsed = time.monotonic() - started
        blocks = end - start + 1
        with self._status_lock:
            fetch_detail = dict(self._fetch_metrics.get("history", {}))
        phases = {
            "fetch": fetch_s,
            "decode": decode_s,
            "store": store_s,
            "writer_wait": store_wait_s,
            "publish": publish_s,
        }
        scan = {
            "observed_at": time.time(),
            "from_block": start,
            "to_block": end,
            "blocks": blocks,
            "logs": len(logs),
            "decoded_events": len(events),
            "inserted_events": len(inserted),
            "deferred_pool_identity_transactions": deferred_identities,
            "requested_chunk": requested_chunk,
            "next_chunk": self._history_chunk,
            "seconds": round(elapsed, 6),
            "fetch_seconds": round(fetch_s, 6),
            "decode_seconds": round(decode_s, 6),
            "store_seconds": round(store_s, 6),
            "store_lock_wait_seconds": round(store_wait_s, 6),
            "publish_seconds": round(publish_s, 6),
            "blocks_per_second": round(
                blocks / max(elapsed, 1e-9), 3,
            ),
            "events_per_second": round(
                len(events) / max(elapsed, 1e-9), 3,
            ),
            "bottleneck": max(phases, key=lambda phase: phases[phase]),
            "bottleneck_seconds": round(max(phases.values()), 6),
            "remaining_blocks": max(0, start - target),
            "queue_policy": "raw_history_independent",
            "fetch": fetch_detail,
        }
        self._set_runtime(
            "history", latency=elapsed, history_scan=scan,
        )
        return True

    @staticmethod
    def _state_rpc_calls(requests_: Sequence[Any]) -> list[tuple[str, Sequence[Any]]]:
        calls: list[tuple[str, Sequence[Any]]] = []
        for request in requests_:
            if isinstance(request, Mapping):
                method, params = request.get("method"), request.get("params")
            elif isinstance(request, (tuple, list)) and len(request) >= 2:
                method, params = request[0], request[1]
            else:
                raise ValueError("position state request must contain method and params")
            if not isinstance(method, str) or not isinstance(params, (tuple, list)):
                raise ValueError("position state request must contain method and params")
            calls.append((method, params))
        return calls

    def _rpc_state_batch(
        self, calls: list[tuple[str, Sequence[Any]]], client: Any | None = None,
    ) -> list[Any]:
        selected = client or self._clients["enrichment"]
        try:
            return self._batch_on(selected, calls)
        except RpcError:
            results: list[Any] = []
            for method, params in calls:
                if self._stop.is_set():
                    raise RpcError("indexer closed during pinned state batch")
                try:
                    results.append(selected.call(method, list(params)))
                except RpcError as exc:
                    if "revert" in str(exc).lower():
                        results.append({"error": str(exc)})
                    else:
                        raise
            return results

    def _enrich_transaction(
        self, pending: Mapping[str, Any],
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        if self._stop.is_set():
            raise RpcError("indexer closed before enrichment fetch")
        client = self._worker_rpc()
        tx_hash = _lower(pending["tx_hash"])
        receipt, transaction = self._batch_on(client, [
            ("eth_getTransactionReceipt", [tx_hash]),
            ("eth_getTransactionByHash", [tx_hash]),
        ])
        if not isinstance(receipt, Mapping) or not isinstance(transaction, Mapping):
            raise RpcError(f"transaction {tx_hash} receipt/body unavailable")
        block_number = _hex_int(receipt.get("blockNumber"), "receipt block number")
        block_hash = _lower(receipt.get("blockHash"))
        if block_number != int(pending["block_number"]) or block_hash != _lower(pending["block_hash"]):
            raise CanonicalConflict(f"pending transaction {tx_hash} moved to another block")
        trace = client.call("debug_traceTransaction", [
            tx_hash,
            {
                "tracer": "callTracer", "tracerConfig": {"withLog": True},
                # Never rebuild missing historical state on the authoritative
                # live node. Unavailable traces remain pending, not fabricated.
                "reexec": 0, "timeout": "5s",
            },
        ])
        if not isinstance(trace, Mapping):
            raise RpcError(f"transaction {tx_hash} returned a malformed call trace")
        raw_logs = receipt.get("logs")
        if not isinstance(raw_logs, list):
            raise RpcError(f"transaction {tx_hash} receipt omitted logs")
        if any(not isinstance(log, Mapping) for log in raw_logs):
            raise RpcError(f"transaction {tx_hash} receipt contains malformed logs")
        header = _header(
            client.call("eth_getBlockByNumber", [hex(block_number), False]),
            block_number,
        )
        if header["hash"] != block_hash:
            raise CanonicalConflict(f"pending transaction {tx_hash} is orphaned")
        events = self._decode(
            "enrichment",
            [dict(log) for log in raw_logs],
            {block_number: header},
            receipts={tx_hash: dict(receipt)},
            traces={tx_hash: dict(trace)},
            resolve_unknown=False,
        )
        if not events:
            raise ValueError(f"enrichment decoder returned no events for {tx_hash}")
        requests_ = list(position_state_requests(events))
        if requests_:
            state_results = self._rpc_state_batch(
                self._state_rpc_calls(requests_), client,
            )
            state_updates = decode_position_state_results(requests_, state_results)
            if state_updates is not None:
                by_identity = {
                    (
                        _lower(event.get("block_hash")), _lower(event.get("tx_hash")),
                        int(event.get("log_index", -1)),
                    ): event
                    for event in events
                }
                for update in state_updates:
                    identity = update.get("identity")
                    if not isinstance(identity, Mapping):
                        raise ValueError("position state decoder omitted event identity")
                    key = (
                        _lower(identity.get("block_hash")), _lower(identity.get("tx_hash")),
                        int(identity.get("log_index", -1)),
                    )
                    event = by_identity.get(key)
                    if event is None:
                        raise ValueError("position state decoder returned an unknown event identity")
                    data_update = update.get("data")
                    if not isinstance(data_update, Mapping):
                        raise ValueError("position state decoder returned malformed event data")
                    data = event.get("data")
                    merged = dict(data) if isinstance(data, Mapping) else {}
                    merged.update(data_update)
                    event["data"] = merged
        gas = decode_gas_record(dict(receipt), dict(transaction))
        if not isinstance(gas, Mapping):
            raise ValueError("gas decoder returned a malformed record")
        gas_record = dict(gas)
        if gas_record.get("gas_native") is not None:
            gas_record["gas_native"] = str(gas_record["gas_native"])
        return events, gas_record

    @staticmethod
    def _trace_capability_error(error: BaseException) -> bool:
        text = str(error).lower()
        return (
            "trace rpc unavailable" in text
            or "trace rpc deferred during provider cooldown" in text
            or "trace rpc exhausted" in text
        )

    def _defer_enrichment(self, reason: str, delay: float) -> None:
        self._enrichment_deferred_reason = str(reason)[:1000]
        self._enrichment_deferred_until = time.monotonic() + max(
            1.0, float(delay),
        )
        self._set_runtime(
            "enrichment", error=self._enrichment_deferred_reason,
            enrichment_deferred=True,
            enrichment_deferred_reason=self._enrichment_deferred_reason,
            enrichment_retry_in_s=round(max(1.0, float(delay)), 1),
        )

    def _pool_identity_is_pending(self, tx_hash: str) -> bool:
        row = self.store.read().execute(
            "SELECT last_error FROM pending_enrichment WHERE tx_hash=?",
            (_lower(tx_hash),),
        ).fetchone()
        return (
            row is not None
            and self._pool_identity_marker(row["last_error"]) is not None
        )

    def _enrich_once(self) -> bool:
        now = time.monotonic()
        if now < self._enrichment_deferred_until:
            retry_in = self._enrichment_deferred_until - now
            self._set_runtime(
                "enrichment", error=self._enrichment_deferred_reason,
                enrichment_deferred=True,
                enrichment_deferred_reason=self._enrichment_deferred_reason,
                enrichment_retry_in_s=round(retry_in, 1),
            )
            return False
        pending = [
            row for row in self.store.pending_enrichments(ENRICH_BATCH)
            if self._pool_identity_marker(row.get("last_error")) is None
        ]
        if not pending:
            return False
        trace = self.source_status().get("trace")
        if isinstance(trace, Mapping) and trace.get("configured") is False:
            self._defer_enrichment(
                "trace RPC unavailable; configure LP_RPC_TRACE_URLS",
                ENRICHMENT_CAPABILITY_RECHECK_S,
            )
            return False
        started = time.monotonic()
        jobs = [
            (
                row,
                self._enrichment_executor.submit(
                    self._enrich_transaction, row,
                ),
            )
            for row in pending
        ]
        successful_rows: list[Mapping[str, Any]] = []
        events: list[dict[str, Any]] = []
        transactions: list[dict[str, Any]] = []
        orphan: Mapping[str, Any] | None = None
        batch_error: Exception | None = None
        capability_error: Exception | None = None
        for row, future in jobs:
            try:
                row_events, transaction = future.result()
            except CanonicalConflict:
                orphan = orphan or row
            except Exception as exc:
                batch_error = exc
                if self._trace_capability_error(exc):
                    # This is a lane-level prerequisite, not a failure of this
                    # canonical job. Preserve its attempts/error fields.
                    capability_error = capability_error or exc
                    continue
                if self._pool_identity_is_pending(str(row["tx_hash"])):
                    continue
                attempts = int(row.get("attempts", 0)) + 1
                self.store.mark_enrichment_error(
                    str(row["tx_hash"]), str(exc),
                    delay=min(300.0, 2.0 ** min(attempts, 8)),
                )
                self._set_runtime("enrichment", error=exc)
            else:
                if self._pool_identity_is_pending(str(row["tx_hash"])):
                    continue
                successful_rows.append(row)
                events.extend(row_events)
                transactions.append(transaction)
        if orphan is not None:
            self._recover_reorg(
                {
                    "block_number": int(orphan["block_number"]),
                    "block_hash": str(orphan["block_hash"]),
                },
                f"enrichment detected orphan {orphan['tx_hash']}",
                supplied_anchor=True,
            )
            return True
        if transactions:
            try:
                self.store.enrich(events, transactions=transactions)
            except CanonicalConflict:
                row = successful_rows[0]
                self._recover_reorg(
                    {
                        "block_number": int(row["block_number"]),
                        "block_hash": str(row["block_hash"]),
                    },
                    f"batched enrichment detected orphan {row['tx_hash']}",
                    supplied_anchor=True,
                )
            except Exception as exc:
                for row in successful_rows:
                    attempts = int(row.get("attempts", 0)) + 1
                    self.store.mark_enrichment_error(
                        str(row["tx_hash"]), str(exc),
                        delay=min(300.0, 2.0 ** min(attempts, 8)),
                    )
                self._set_runtime("enrichment", error=exc)
            else:
                self._publish_enriched_current_events(
                    events, source="enrichment",
                )
                if batch_error is None:
                    self._set_runtime(
                        "enrichment", latency=time.monotonic() - started,
                        enrichment_batch=len(transactions),
                        enrichment_deferred=False,
                        enrichment_deferred_reason=None,
                        enrichment_retry_in_s=0.0,
                    )
                else:
                    self._set_runtime(
                        "enrichment", error=batch_error,
                        latency=time.monotonic() - started,
                        enrichment_batch=len(transactions),
                    )
        if capability_error is not None:
            delay = (
                ENRICHMENT_CAPABILITY_RECHECK_S
                if "unavailable" in str(capability_error).lower()
                else 60.0
            )
            self._defer_enrichment(str(capability_error), delay)
        elif successful_rows:
            self._enrichment_deferred_until = 0.0
            self._enrichment_deferred_reason = None
        return True

    @staticmethod
    def _decode_token_symbol(value: Any) -> str:
        if isinstance(value, Mapping) and value.get("error") is not None:
            raise RpcError(f"symbol eth_call failed: {value['error']}")
        if not isinstance(value, str) or not value.startswith("0x"):
            raise RpcError("symbol eth_call returned malformed data")
        body = value[2:]
        try:
            if len(body) == 64:
                raw = bytes.fromhex(body).rstrip(b"\0")
            elif len(body) >= 128:
                offset = int(body[:64], 16) * 2
                if offset < 64 or offset + 64 > len(body):
                    raise ValueError("invalid ABI offset")
                length = int(body[offset:offset + 64], 16)
                if length > 256 or offset + 64 + length * 2 > len(body):
                    raise ValueError("invalid ABI string length")
                raw = bytes.fromhex(body[offset + 64:offset + 64 + length * 2])
            else:
                raise ValueError("short ABI result")
            symbol = raw.decode("utf-8").strip().strip("\0")
        except (ValueError, UnicodeDecodeError) as exc:
            raise RpcError(f"cannot decode token symbol: {exc}") from exc
        if not symbol or len(symbol) > 256:
            raise RpcError("token symbol is empty or oversized")
        return symbol

    def _metadata_once(self) -> bool:
        pending = self.store.pending_token_metadata(8)
        if not pending:
            return False
        register = getattr(self.market, "register_index_pool", None)
        for row in pending:
            if self._stop.is_set():
                break
            address = str(row["address"]).lower()
            try:
                results = self._rpc_state_batch([
                    ("eth_call", [{"to": address, "data": SYMBOL_SELECTOR}, "latest"]),
                    ("eth_call", [{"to": address, "data": DECIMALS_SELECTOR}, "latest"]),
                ])
                symbol = self._decode_token_symbol(results[0])
                if isinstance(results[1], Mapping) and results[1].get("error") is not None:
                    raise RpcError(f"decimals eth_call failed: {results[1]['error']}")
                decimals = _hex_int(results[1], "token decimals")
                self.store.save_token_metadata(address, symbol, decimals)
                with self._cache_lock:
                    self._pool_cache.clear()
                    self._pool_misses.discard(address)
                if callable(register):
                    try:
                        self._publish_token_pools(address, register)
                    except BaseException:
                        self._publish_after_id = ""
                        raise
            except Exception as exc:
                attempts = int(row.get("attempts", 0)) + 1
                self.store.mark_token_metadata_error(
                    address, str(exc), delay=min(300.0, 2.0 ** min(attempts, 8)),
                )
                self._set_runtime("metadata", error=exc)
            else:
                self._set_runtime("metadata")
        return True

    def _reproject_once(self) -> bool:
        pending = self.store.pending_reprojections(REPROJECT_BATCH)
        if not pending:
            return False
        event_ids = [int(event["id"]) for event in pending]
        started = time.monotonic()
        try:
            self.store.reproject(event_ids)
        except Exception as exc:
            attempts = max(int(event.get("reprojection_attempts", 0)) for event in pending) + 1
            self.store.mark_reprojection_error(
                event_ids, str(exc), delay=min(300.0, 2.0 ** min(attempts, 8)),
            )
            self._set_runtime(
                "reprojection", error=exc, latency=time.monotonic() - started,
                reprojection_batch=len(event_ids),
            )
        else:
            self._set_runtime(
                "reprojection", latency=time.monotonic() - started,
                reprojection_batch=len(event_ids),
            )
        return True

    @staticmethod
    def _balance_call(token: str, pool_address: str, block: int) -> tuple[str, list[Any]]:
        if len(token) != 42 or len(pool_address) != 42:
            raise ValueError("V3 balance request requires token and pool addresses")
        data = BALANCE_OF_SELECTOR + pool_address[2:].rjust(64, "0")
        return "eth_call", [{"to": token, "data": data}, hex(block)]

    def _balances_once(self) -> bool:
        if not self.v3_balances:
            return False
        pending = self.store.pending_v3_balances(BALANCE_BATCH)
        if not pending:
            return False
        started = time.monotonic()
        completed = 0
        batch_error: Exception | None = None
        for row in pending:
            if self._stop.is_set():
                break
            try:
                results = self._rpc_batch("enrichment", [
                    self._balance_call(
                        str(row["token0"]), str(row["pool_id"]), int(row["block_number"]),
                    ),
                    self._balance_call(
                        str(row["token1"]), str(row["pool_id"]), int(row["block_number"]),
                    ),
                ])
                balance0 = _hex_int(results[0], "token0 balance")
                balance1 = _hex_int(results[1], "token1 balance")
                self.store.save_v3_balance(
                    str(row["pool_id"]), int(row["block_number"]), str(row["block_hash"]),
                    balance0, balance1,
                )
            except Exception as exc:
                batch_error = exc
                attempts = int(row.get("attempts", 0)) + 1
                self.store.mark_v3_balance_error(
                    str(row["pool_id"]), int(row["block_number"]), str(exc),
                    delay=min(300.0, 2.0 ** min(attempts, 8)),
                )
            else:
                completed += 1
        self._set_runtime(
            "balances", error=batch_error, latency=time.monotonic() - started,
            balances_batch=completed,
        )
        return True

    def _live_run(self) -> None:
        backoff = 0.25
        while not self._stop.is_set():
            try:
                if not self._initialized.is_set():
                    head_number, head, latency = self._bootstrap()
                    self._initialized.set()
                    self._set_runtime(
                        "startup", latency=latency, startup=None,
                        head=head_number, head_hash=head["hash"],
                        head_timestamp=_timestamp(head),
                    )
                worked = self._scan_live_once()
            except Exception as exc:
                lane = "live" if self._initialized.is_set() else "startup"
                self._set_runtime(lane, error=exc)
                self._stop.wait(backoff)
                backoff = min(10.0, backoff * 2.0)
            else:
                backoff = 0.25
                self._stop.wait(0.05 if worked else 0.35)

    def _history_run(self) -> None:
        backoff = 0.5
        while not self._stop.is_set():
            if not self._initialized.wait(0.5):
                continue
            try:
                worked = self._scan_history_once()
            except Exception as exc:
                self._set_runtime("history", error=exc)
                self._stop.wait(backoff)
                backoff = min(30.0, backoff * 2.0)
            else:
                backoff = 0.5
                self._stop.wait(0.05 if worked else 0.5)

    def _enrichment_run(self) -> None:
        backoff = 0.5
        while not self._stop.is_set():
            if not self._initialized.wait(0.5):
                continue
            try:
                if not self._enrichment_verified:
                    self._verify_chain("enrichment")
                    self._enrichment_verified = True
                worked = self._sync_market_reorg()
                worked = self._publish_stored_pool_page() or worked
                if not self._recent_catchup_pending():
                    worked = self._resolve_deferred_pool_identities_once() or worked
                    worked = self._enrich_once() or worked
                worked = self._metadata_once() or worked
                worked = self._recover_legacy_v4_pool_page() or worked
            except Exception as exc:
                self._set_runtime("enrichment", error=exc)
                self._stop.wait(backoff)
                backoff = min(30.0, backoff * 2.0)
            else:
                backoff = 0.5
                self._stop.wait(0.05 if worked else 0.5)

    def _projection_run(self) -> None:
        backoff = 0.5
        while not self._stop.is_set():
            if not self._initialized.wait(0.5):
                continue
            try:
                self.store.checkpoint()
                if self._recent_catchup_pending():
                    self._stop.wait(0.25)
                    continue
                if not self._projection_verified:
                    self._verify_chain("enrichment")
                    self._projection_verified = True
                worked = self._reproject_once()
                worked = self._balances_once() or worked
            except Exception as exc:
                self._set_runtime("projection", error=exc)
                self._stop.wait(backoff)
                backoff = min(30.0, backoff * 2.0)
            else:
                backoff = 0.5
                with self._status_lock:
                    recovered = "projection" in self._errors
                if recovered:
                    self._set_runtime("projection")
                self._stop.wait(0.05 if worked else 0.5)

    def close(self) -> None:
        with self._lifecycle_lock:
            if self._stop.is_set():
                return
            was_started = self._started
            self._stop.set()
            try:
                current = threading.current_thread()
                for thread in self._threads:
                    if thread is not current:
                        thread.join()
                self._current_receipt_executor.shutdown(wait=True, cancel_futures=True)
                self._current_pool_executor.shutdown(wait=True, cancel_futures=True)
                self._enrichment_executor.shutdown(wait=True, cancel_futures=True)
                self._header_executor.shutdown(
                    wait=True, cancel_futures=True,
                )
                self._market_observer_executor.shutdown(
                    wait=True, cancel_futures=True,
                )
                self._close_owned_rpc_clients()
                if was_started:
                    self._set_runtime("lifecycle", state="stopped")
            finally:
                self._release_process_lock()

    def __enter__(self) -> "MarketIndexer":
        return self.start()

    def __exit__(self, *_args: Any) -> None:
        self.close()


__all__ = ["MarketIndexer", "RpcError"]
