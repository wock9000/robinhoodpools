"""Bounded, read-only consumers for approved anonymous public Fomo flow."""
from __future__ import annotations

from collections import Counter, OrderedDict
from collections.abc import Mapping
from concurrent.futures import Future, TimeoutError as FutureTimeoutError
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import ipaddress
import json
import os
import re
import threading
import time
from typing import Any
from urllib.parse import urlsplit

import requests

PUBLIC_APOLLO_ORIGIN = "https://apollo-copy.xylem-group.org"
PUBLIC_RHTRENCHES_ORIGIN = "https://rhtrenches.com"
APOLLO_FLOW_PATH = "/v1/copy/flow"
RHTRENCHES_TAPE_PATH = "/api/tape"
RHTRENCHES_STATUS_PATH = "/api/status"
SUPPORTED_CHAINS = ("solana", "base", "robinhood")
ENABLED_SOURCES = ("apollo", "rhtrenches")

_CACHE_TTL_SECONDS = 10.0
_RHTRENCHES_FETCH_LIMIT = 100
_MAX_CACHE_ENTRIES = 64
_MAX_RESPONSE_BYTES = 2 * 1024 * 1024
_CONNECT_TIMEOUT_SECONDS = 2.5
_READ_TIMEOUT_SECONDS = 6.0
_CURSOR_RE = re.compile(r"[A-Za-z0-9_-]{1,4096}\Z")
_DECIMAL_RE = re.compile(r"(?:0|[1-9][0-9]*)(?:\.[0-9]+)?\Z")
_EVM_ADDRESS_RE = re.compile(r"0x[0-9a-fA-F]{40}\Z")
_EVM_HASH_RE = re.compile(r"0x[0-9a-fA-F]{64}\Z")
_HOST_RE = re.compile(
    r"(?=.{1,253}\Z)(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+"
    r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\Z"
)
_BLOCKED_HOST_SUFFIXES = (".internal", ".lan", ".local", ".localhost")
_APOLLO_ACTIONS = frozenset({"open", "increase", "decrease", "close"})
_RHTRENCHES_ACTIONS = frozenset({"buy", "sell"})
_IDENTITY_KINDS = frozenset({"verified-fomo", "observed-wallet"})
_CONFIRMATIONS = frozenset({"processed", "confirmed", "finalized"})
_SOURCE_KINDS = frozenset({"chain-observed", "fomo-official"})
_QUERY_KEYS = frozenset({"chain", "cursor", "limit", "source", "verified"})


class FomoFlowError(RuntimeError):
    """Base class for failures that public routing should return as HTTP 503."""


class FomoFlowUnavailable(FomoFlowError):
    """A selected public flow endpoint cannot provide a usable page."""

    def __init__(
        self,
        message: str,
        *,
        upstream_status: int | None = None,
        retry_after: str | None = None,
    ) -> None:
        super().__init__(message)
        self.upstream_status = upstream_status
        self.retry_after = retry_after


class FomoFlowContractError(FomoFlowUnavailable):
    """A publisher returned a response outside its selected public contract."""


@dataclass(frozen=True, slots=True)
class _FlowQuery:
    source: str
    chain: str | None
    cursor: str | None
    limit: int
    verified: bool


@dataclass(frozen=True, slots=True)
class _UpstreamPage:
    origin: str
    payload: Any
    status: Mapping[str, Any] | None
    retrieved_at: str


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _safe_origin(value: str) -> str:
    if not isinstance(value, str):
        raise ValueError("Fomo flow origin must be a valid HTTPS origin")
    candidate = value.strip()
    try:
        parsed = urlsplit(candidate)
        port = parsed.port
    except ValueError as exc:
        raise ValueError("Fomo flow origin must be a valid HTTPS origin") from exc
    hostname = (parsed.hostname or "").lower().rstrip(".")
    if (
        parsed.scheme.lower() != "https"
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or parsed.path not in {"", "/"}
        or port not in {None, 443}
        or not _HOST_RE.fullmatch(hostname)
    ):
        raise ValueError(
            "Fomo flow origin must be an HTTPS DNS origin without credentials, "
            "a non-default port, path, query, or fragment"
        )
    try:
        ipaddress.ip_address(hostname)
    except ValueError:
        pass
    else:
        raise ValueError("Fomo flow origin must use a public DNS hostname, not an IP literal")
    if hostname == "localhost" or hostname.endswith(_BLOCKED_HOST_SUFFIXES):
        raise ValueError("Fomo flow origin must not use a local or private hostname")
    return f"https://{hostname}"


def _scalar(params: Mapping[str, Any], name: str) -> Any:
    value = params.get(name)
    if isinstance(value, (list, tuple)):
        if len(value) != 1:
            raise ValueError(f"{name} may only be provided once")
        return value[0]
    return value


def _query(params: Mapping[str, Any]) -> _FlowQuery:
    unknown = sorted(str(key) for key in params if key not in _QUERY_KEYS)
    if unknown:
        raise ValueError(f"unsupported Fomo flow query parameter: {unknown[0]}")

    raw_source = _scalar(params, "source")
    source = "apollo" if raw_source is None else str(raw_source).strip().lower()
    if source not in ENABLED_SOURCES:
        raise ValueError("source must be apollo or rhtrenches")

    raw_chain = _scalar(params, "chain")
    chain = None if raw_chain is None or str(raw_chain).strip() == "" else str(raw_chain).strip().lower()
    if chain is not None and chain not in SUPPORTED_CHAINS:
        raise ValueError("chain must be solana, base, or robinhood; all-chain scope is not available")

    raw_cursor = _scalar(params, "cursor")
    cursor = None if raw_cursor is None else str(raw_cursor).strip()
    if cursor == "" or (cursor is not None and not _CURSOR_RE.fullmatch(cursor)):
        raise ValueError("cursor must be a non-empty Apollo base64url cursor of at most 4096 characters")

    raw_limit = _scalar(params, "limit")
    if raw_limit is None:
        limit = 50
    elif isinstance(raw_limit, bool) or not re.fullmatch(r"[0-9]+", str(raw_limit)):
        raise ValueError("limit must be an integer from 1 through 100")
    else:
        limit = int(raw_limit)
    if limit < 1 or limit > 100:
        raise ValueError("limit must be an integer from 1 through 100")

    raw_verified = _scalar(params, "verified")
    if raw_verified is None or raw_verified is False or raw_verified == "false":
        verified = False
    elif raw_verified is True or raw_verified == "true":
        verified = True
    else:
        raise ValueError("verified must be true or false")

    if source == "rhtrenches":
        if chain not in {None, "robinhood"}:
            raise ValueError("rhtrenches publishes Robinhood Chain only")
        if verified:
            raise ValueError("rhtrenches publishes observed-wallet identity only; verified must be false")
        if cursor is not None:
            raise ValueError("rhtrenches publishes a current bounded tape snapshot and does not accept cursor")
        chain = "robinhood"
    return _FlowQuery(source, chain, cursor, limit, verified)


def _object(value: Any, field: str, *, publisher: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise FomoFlowContractError(f"{publisher} flow response has an invalid {field}")
    return value


def _text(
    value: Any,
    field: str,
    *,
    publisher: str,
    maximum: int = 4096,
    nullable: bool = False,
) -> str | None:
    if nullable and value is None:
        return None
    if not isinstance(value, str) or not value or len(value) > maximum:
        raise FomoFlowContractError(f"{publisher} flow response has an invalid {field}")
    return value


def _integer(
    value: Any,
    field: str,
    *,
    publisher: str,
    minimum: int = 0,
) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise FomoFlowContractError(f"{publisher} flow response has an invalid {field}")
    return value


def _apollo_decimal(value: Any, field: str, *, nullable: bool = False) -> str | None:
    text = _text(value, field, publisher="Apollo", maximum=512, nullable=nullable)
    if text is None:
        return None
    if not _DECIMAL_RE.fullmatch(text):
        raise FomoFlowContractError(f"Apollo flow response has an invalid {field}")
    return text


def _reported_decimal(
    value: Any,
    field: str,
    *,
    nullable: bool = False,
) -> str | None:
    if nullable and value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (str, int, float)):
        raise FomoFlowContractError(f"RH Trenches flow response has an invalid {field}")
    try:
        number = Decimal(str(value))
    except InvalidOperation as exc:
        raise FomoFlowContractError(f"RH Trenches flow response has an invalid {field}") from exc
    if not number.is_finite() or number < 0:
        raise FomoFlowContractError(f"RH Trenches flow response has an invalid {field}")
    return format(number, "f")


def _iso(
    value: Any,
    field: str,
    *,
    publisher: str,
    nullable: bool = False,
) -> str | None:
    text = _text(value, field, publisher=publisher, maximum=64, nullable=nullable)
    if text is None:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise FomoFlowContractError(f"{publisher} flow response has an invalid {field}") from exc
    if parsed.tzinfo is None:
        raise FomoFlowContractError(f"{publisher} flow response has an invalid {field}")
    return text


def _epoch(value: Any, field: str, *, nullable: bool = False) -> tuple[str | None, str | None]:
    if nullable and value is None:
        return None, None
    seconds = _integer(value, field, publisher="RH Trenches")
    if seconds > 4_102_444_800:
        raise FomoFlowContractError(f"RH Trenches flow response has an invalid {field}")
    rendered = datetime.fromtimestamp(seconds, timezone.utc).isoformat(timespec="milliseconds").replace(
        "+00:00", "Z"
    )
    return rendered, str(seconds)


def _evm_address(value: Any, field: str, *, nullable: bool = False) -> str | None:
    text = _text(value, field, publisher="RH Trenches", maximum=42, nullable=nullable)
    if text is None:
        return None
    if not _EVM_ADDRESS_RE.fullmatch(text):
        raise FomoFlowContractError(f"RH Trenches flow response has an invalid {field}")
    return text.lower()


def _apollo_identity(value: Any) -> dict[str, Any]:
    identity = _object(value, "identity", publisher="Apollo")
    kind = _text(identity.get("kind"), "identity.kind", publisher="Apollo", maximum=32)
    if kind not in _IDENTITY_KINDS:
        raise FomoFlowContractError("Apollo flow response has an unsupported identity kind")
    return {
        "kind": kind,
        "trader_id": _text(
            identity.get("traderId"), "identity.traderId", publisher="Apollo", maximum=256,
        ),
        "handle": _text(identity.get("handle"), "identity.handle", publisher="Apollo", maximum=256),
        "wallet": _text(identity.get("wallet"), "identity.wallet", publisher="Apollo", maximum=256),
    }


def _apollo_source(value: Any) -> dict[str, Any]:
    source = _object(value, "sourceRef", publisher="Apollo")
    kind = _text(source.get("kind"), "sourceRef.kind", publisher="Apollo", maximum=32)
    if kind not in _SOURCE_KINDS:
        raise FomoFlowContractError("Apollo flow response has an unsupported source kind")
    if kind == "chain-observed":
        confirmation = _text(
            source.get("confirmationStatus"),
            "sourceRef.confirmationStatus",
            publisher="Apollo",
            maximum=16,
        )
        if confirmation not in _CONFIRMATIONS:
            raise FomoFlowContractError("Apollo flow response has an invalid confirmation status")
        block = _text(
            source.get("blockNumberOrSlot"),
            "sourceRef.blockNumberOrSlot",
            publisher="Apollo",
            maximum=128,
        )
        if not re.fullmatch(r"(?:0|[1-9][0-9]*)", block or ""):
            raise FomoFlowContractError("Apollo flow response has an invalid block or slot")
        return {
            "kind": kind,
            "transaction_hash": _text(
                source.get("txHash"), "sourceRef.txHash", publisher="Apollo", maximum=256,
            ),
            "instruction_or_log_index": _integer(
                source.get("instructionOrLogIndex"),
                "sourceRef.instructionOrLogIndex",
                publisher="Apollo",
            ),
            "block_hash": _text(
                source.get("blockHash"), "sourceRef.blockHash", publisher="Apollo", maximum=256,
            ),
            "block_number_or_slot": block,
            "confirmation_status": confirmation,
        }
    return {
        "kind": kind,
        "event_id": _text(
            source.get("eventId"), "sourceRef.eventId", publisher="Apollo", maximum=512,
        ),
        "sequence": _text(
            source.get("sequence"), "sourceRef.sequence", publisher="Apollo", maximum=256,
        ),
    }


def _apollo_event(value: Any) -> dict[str, Any]:
    event = _object(value, "items[]", publisher="Apollo")
    asset = _object(event.get("asset"), "asset", publisher="Apollo")
    chain = _text(asset.get("chain"), "asset.chain", publisher="Apollo", maximum=32)
    if chain not in SUPPORTED_CHAINS:
        raise FomoFlowContractError("Apollo flow response contains a chain outside the supported set")
    action = _text(event.get("action"), "action", publisher="Apollo", maximum=16)
    if action not in _APOLLO_ACTIONS:
        raise FomoFlowContractError("Apollo flow response has an unsupported action")
    occurred_at = _iso(event.get("occurredAt"), "occurredAt", publisher="Apollo")
    observed_at = _iso(event.get("observedAt"), "observedAt", publisher="Apollo")
    return {
        "publisher": "apollo",
        "canonical_action_id": _text(
            event.get("canonicalActionId"), "canonicalActionId", publisher="Apollo", maximum=1024,
        ),
        "source_event_id": None,
        "trader_strategy_id": _text(
            event.get("traderStrategyId"), "traderStrategyId", publisher="Apollo", maximum=256,
        ),
        "identity": _apollo_identity(event.get("identity")),
        "source": _apollo_source(event.get("sourceRef")),
        "asset": {
            "chain": chain,
            "address": _text(
                asset.get("address"), "asset.address", publisher="Apollo", maximum=256,
            ),
            "symbol": _text(
                event.get("assetSymbol"), "assetSymbol", publisher="Apollo", maximum=128, nullable=True,
            ),
            "name": _text(
                event.get("assetName"), "assetName", publisher="Apollo", maximum=256, nullable=True,
            ),
        },
        "action": action,
        "action_basis": "apollo-normalized-leader-action",
        "economics": {
            "leader_position_before_usd": _apollo_decimal(
                event.get("leaderPositionBeforeUsd"), "leaderPositionBeforeUsd",
            ),
            "leader_position_after_usd": _apollo_decimal(
                event.get("leaderPositionAfterUsd"), "leaderPositionAfterUsd",
            ),
            "trade_size_usd": _apollo_decimal(event.get("tradeSizeUsd"), "tradeSizeUsd"),
            "leader_fill_price_usd": _apollo_decimal(
                event.get("leaderFillPriceUsd"), "leaderFillPriceUsd",
            ),
            "trade_quantity": _apollo_decimal(
                event.get("tradeQuantity"), "tradeQuantity", nullable=True,
            ),
            "quote_token": None,
            "value_basis": "apollo-published-decimal",
        },
        "evidence": {
            "occurred_at": occurred_at,
            "occurred_at_unix": None,
            "observed_at": observed_at,
        },
    }


def _rh_event(value: Any) -> tuple[dict[str, Any] | None, str | None]:
    event = _object(value, "tape[]", publisher="RH Trenches")
    record_id = _integer(event.get("id"), "id", publisher="RH Trenches", minimum=1)
    occurred_at, occurred_at_unix = _epoch(event.get("ts"), "ts")
    transaction_hash = _text(event.get("tx"), "tx", publisher="RH Trenches", maximum=66)
    if not _EVM_HASH_RE.fullmatch(transaction_hash or ""):
        raise FomoFlowContractError("RH Trenches flow response has an invalid tx")
    block = _integer(event.get("block"), "block", publisher="RH Trenches")
    action = _text(event.get("side"), "side", publisher="RH Trenches", maximum=8)
    if action not in _RHTRENCHES_ACTIONS:
        raise FomoFlowContractError("RH Trenches flow response has an unsupported side")
    wallet = _evm_address(event.get("wallet"), "wallet")
    token = _evm_address(event.get("token"), "token")
    handle = _text(event.get("handle"), "handle", publisher="RH Trenches", maximum=256)
    flags = event.get("flags")
    if not isinstance(flags, list) or any(not isinstance(flag, str) or len(flag) > 256 for flag in flags):
        raise FomoFlowContractError("RH Trenches flow response has invalid flags")
    priced = event.get("priced")
    if priced is not None and (not isinstance(priced, str) or len(priced) > 64):
        raise FomoFlowContractError("RH Trenches flow response has an invalid priced basis")

    # RH Trenches publishes derived warning labels for planted/gifted/spoofed
    # transfers. They are useful on its own surface but are not licensed raw
    # evidence for this projection. Exclude those rows rather than stripping a
    # warning and then presenting its side as an unqualified action.
    if flags:
        return None, "publisher-warning"
    # Its own client marks non-cash-leg USD as an estimate. This consumer is a
    # facts-only lane, so it excludes those estimates instead of republishing
    # them as observed USD size.
    if priced not in {None, "cash_leg"}:
        return None, "estimated-value"

    quote_token = _evm_address(event.get("quote_token"), "quote_token", nullable=True)
    return {
        "publisher": "rhtrenches",
        "canonical_action_id": None,
        "source_event_id": str(record_id),
        "trader_strategy_id": None,
        "identity": {
            "kind": "observed-wallet",
            "trader_id": None,
            "handle": handle,
            "wallet": wallet,
        },
        "source": {
            "kind": "chain-observed",
            "transaction_hash": transaction_hash.lower(),
            "instruction_or_log_index": None,
            "block_hash": None,
            "block_number_or_slot": str(block),
            "confirmation_status": None,
        },
        "asset": {
            "chain": "robinhood",
            "address": token,
            "symbol": _text(
                event.get("symbol"), "symbol", publisher="RH Trenches", maximum=128, nullable=True,
            ),
            "name": _text(
                event.get("name"), "name", publisher="RH Trenches", maximum=256, nullable=True,
            ),
        },
        "action": action,
        "action_basis": "rhtrenches-published-side",
        "economics": {
            "leader_position_before_usd": None,
            "leader_position_after_usd": None,
            "trade_size_usd": _reported_decimal(event.get("usd"), "usd", nullable=True),
            "leader_fill_price_usd": _reported_decimal(event.get("price"), "price", nullable=True),
            "trade_quantity": _reported_decimal(event.get("amount"), "amount", nullable=True),
            "quote_token": quote_token,
            "value_basis": "rhtrenches-cash-leg" if priced == "cash_leg" else "rhtrenches-published",
        },
        "evidence": {
            "occurred_at": occurred_at,
            "occurred_at_unix": occurred_at_unix,
            "observed_at": None,
        },
    }, None


def _latest(items: list[dict[str, Any]], field: str) -> str | None:
    return max(
        (
            str(item["evidence"][field])
            for item in items
            if item["evidence"].get(field) is not None
        ),
        key=lambda value: datetime.fromisoformat(value.replace("Z", "+00:00")),
        default=None,
    )


def _source_catalog(selected: str) -> dict[str, Any]:
    return {
        "selected": selected,
        "enabled": [
            {
                "id": "apollo",
                "publisher": "Apollo",
                "origin": PUBLIC_APOLLO_ORIGIN,
                "chains": list(SUPPORTED_CHAINS),
                "identity": "published observed-wallet or verified-fomo",
                "scope": "anonymous Apollo FlowPage",
            },
            {
                "id": "rhtrenches",
                "publisher": "RH Trenches / @Degenerate_DeFi",
                "origin": PUBLIC_RHTRENCHES_ORIGIN,
                "chains": ["robinhood"],
                "identity": "observed-wallet only",
                "scope": "bounded public tape; warning and estimated-value rows omitted",
            },
        ],
        "not_enabled": [
            {
                "id": "wochovy",
                "origin": "https://wochovy.org",
                "reason": "anonymous proxy of keyed FomoAPI with derived signals, reputation, and theses; no separate redistribution grant",
            },
            {
                "id": "fomoapi",
                "origin": "https://api.fomoapi.io",
                "reason": "data requires a bearer key; public product use requires an explicit redistribution/license agreement",
            },
        ],
    }


class FomoFlowService:
    """Consume strictly projected pages from fixed anonymous public sources.

    Origins are read once at construction and can never come from request
    parameters. The service owns its default HTTP session and must be closed by
    its application owner.
    """

    def __init__(
        self,
        *,
        origin: str | None = None,
        rhtrenches_origin: str | None = None,
        cache_ttl: float = _CACHE_TTL_SECONDS,
        max_cache_entries: int = _MAX_CACHE_ENTRIES,
        max_response_bytes: int = _MAX_RESPONSE_BYTES,
        connect_timeout: float = _CONNECT_TIMEOUT_SECONDS,
        read_timeout: float = _READ_TIMEOUT_SECONDS,
        session: requests.Session | None = None,
    ) -> None:
        apollo_origin = origin
        if apollo_origin is None:
            apollo_origin = os.environ.get("RHP_APOLLO_FLOW_ORIGIN", PUBLIC_APOLLO_ORIGIN)
        rh_origin = rhtrenches_origin
        if rh_origin is None:
            rh_origin = os.environ.get("RHP_RHTRENCHES_ORIGIN", PUBLIC_RHTRENCHES_ORIGIN)
        self.origins = {
            "apollo": _safe_origin(apollo_origin),
            "rhtrenches": _safe_origin(rh_origin),
        }
        # Retained for callers that reported the original single-source origin.
        self.origin = self.origins["apollo"]
        if cache_ttl < 10 or cache_ttl > 60:
            raise ValueError("cache_ttl must be between 10 and 60 seconds")
        if not 1 <= int(max_cache_entries) <= 256:
            raise ValueError("max_cache_entries must be between 1 and 256")
        if not 65_536 <= int(max_response_bytes) <= 16 * 1024 * 1024:
            raise ValueError("max_response_bytes must be between 65536 and 16777216")
        if not 0 < connect_timeout <= 30 or not 0 < read_timeout <= 30:
            raise ValueError("HTTP timeouts must be greater than zero and at most 30 seconds")

        self.cache_ttl = float(cache_ttl)
        self.max_cache_entries = int(max_cache_entries)
        self.max_response_bytes = int(max_response_bytes)
        self.timeout = (float(connect_timeout), float(read_timeout))
        self._wait_timeout = float(connect_timeout + read_timeout + 1)
        self._session = session if session is not None else requests.Session()
        self._owns_session = session is None
        if self._owns_session:
            self._session.trust_env = False
            self._session.headers.update({
                "Accept": "application/json",
                "User-Agent": "rhpools-public-flow/1",
            })
        self._cache: OrderedDict[
            tuple[str, str | None, int, bool], tuple[float, _UpstreamPage]
        ] = OrderedDict()
        self._pending: dict[
            tuple[str, str | None, int, bool], Future[_UpstreamPage]
        ] = {}
        self._lock = threading.Lock()
        self._network_lock = threading.Lock()
        self._closed = False

    def close(self) -> None:
        """Reject new reads, clear cached pages, and close the owned HTTP session."""
        with self._lock:
            if self._closed:
                return
            self._closed = True
            self._cache.clear()
        if self._owns_session:
            with self._network_lock:
                self._session.close()

    @staticmethod
    def _bounded_body(response: requests.Response, maximum: int, publisher: str) -> bytes:
        declared = response.headers.get("content-length")
        if declared is not None:
            try:
                declared_size = int(declared)
                if declared_size < 0:
                    raise ValueError
                if declared_size > maximum:
                    raise FomoFlowUnavailable(f"{publisher} public flow response exceeded the size limit")
            except ValueError as exc:
                raise FomoFlowContractError(
                    f"{publisher} public flow response has an invalid Content-Length"
                ) from exc
        chunks: list[bytes] = []
        size = 0
        for chunk in response.iter_content(chunk_size=65_536):
            if not chunk:
                continue
            size += len(chunk)
            if size > maximum:
                raise FomoFlowUnavailable(f"{publisher} public flow response exceeded the size limit")
            chunks.append(chunk)
        return b"".join(chunks)

    def _request_json(
        self,
        *,
        source: str,
        path: str,
        params: Mapping[str, str],
        bad_request_is_query: bool = False,
    ) -> Any:
        publisher = "Apollo" if source == "apollo" else "RH Trenches"
        response: requests.Response | None = None
        try:
            response = self._session.get(
                f"{self.origins[source]}{path}",
                params=dict(params),
                timeout=self.timeout,
                allow_redirects=False,
                stream=True,
            )
            status = response.status_code
            if bad_request_is_query and status == 400:
                raise ValueError("cursor was rejected by the Apollo public flow endpoint")
            if status != 200:
                retry_after = response.headers.get("retry-after")
                raise FomoFlowUnavailable(
                    f"{publisher} public flow endpoint returned HTTP {status}",
                    upstream_status=status,
                    retry_after=retry_after if retry_after and len(retry_after) <= 64 else None,
                )
            media_type = response.headers.get("content-type", "").split(";", 1)[0].strip().lower()
            if media_type != "application/json":
                raise FomoFlowContractError(
                    f"{publisher} public flow endpoint returned a non-JSON response"
                )
            body = self._bounded_body(response, self.max_response_bytes, publisher)
        except (FomoFlowError, ValueError):
            raise
        except requests.RequestException as exc:
            raise FomoFlowUnavailable(f"{publisher} public flow endpoint is unavailable") from exc
        finally:
            if response is not None:
                response.close()
        try:
            return json.loads(body.decode("utf-8"), parse_float=str)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise FomoFlowContractError(f"{publisher} public flow endpoint returned invalid JSON") from exc

    def _fetch(self, query: _FlowQuery) -> _UpstreamPage:
        with self._network_lock:
            with self._lock:
                if self._closed:
                    raise FomoFlowUnavailable("Fomo flow service is closed")
            try:
                if query.source == "apollo":
                    params: dict[str, str] = {"limit": str(query.limit)}
                    if query.cursor is not None:
                        params["cursor"] = query.cursor
                    if query.verified:
                        params["verified"] = "true"
                    payload = self._request_json(
                        source="apollo",
                        path=APOLLO_FLOW_PATH,
                        params=params,
                        bad_request_is_query=True,
                    )
                    return _UpstreamPage(
                        self.origins["apollo"], payload, None, _utc_now(),
                    )

                tape = self._request_json(
                    source="rhtrenches",
                    path=RHTRENCHES_TAPE_PATH,
                    params={"limit": str(_RHTRENCHES_FETCH_LIMIT), "stocks": "true"},
                )
                status = self._request_json(
                    source="rhtrenches",
                    path=RHTRENCHES_STATUS_PATH,
                    params={},
                )
                return _UpstreamPage(
                    self.origins["rhtrenches"],
                    tape,
                    _object(status, "status", publisher="RH Trenches"),
                    _utc_now(),
                )
            finally:
                if self._owns_session:
                    self._session.cookies.clear()

    def _page(self, query: _FlowQuery) -> _UpstreamPage:
        key = (
            (query.source, None, _RHTRENCHES_FETCH_LIMIT, False)
            if query.source == "rhtrenches"
            else (query.source, query.cursor, query.limit, query.verified)
        )
        now = time.monotonic()
        leader = False
        with self._lock:
            if self._closed:
                raise FomoFlowUnavailable("Fomo flow service is closed")
            cached = self._cache.get(key)
            if cached is not None and now - cached[0] < self.cache_ttl:
                self._cache.move_to_end(key)
                return cached[1]
            future = self._pending.get(key)
            if future is None:
                if len(self._pending) >= self.max_cache_entries:
                    raise FomoFlowUnavailable("public flow read capacity is exhausted")
                future = Future()
                self._pending[key] = future
                leader = True
        if not leader:
            try:
                return future.result(timeout=self._wait_timeout)
            except FutureTimeoutError as exc:
                raise FomoFlowUnavailable("timed out waiting for the shared public flow read") from exc

        try:
            page = self._fetch(query)
        except BaseException as exc:
            with self._lock:
                self._pending.pop(key, None)
            future.set_exception(exc)
            raise
        with self._lock:
            self._pending.pop(key, None)
            if not self._closed:
                self._cache[key] = (time.monotonic(), page)
                self._cache.move_to_end(key)
                while len(self._cache) > self.max_cache_entries:
                    self._cache.popitem(last=False)
        future.set_result(page)
        return page

    @staticmethod
    def _apollo_response(query: _FlowQuery, upstream: _UpstreamPage) -> dict[str, Any]:
        page = _object(upstream.payload, "page", publisher="Apollo")
        raw_items = page.get("items")
        if not isinstance(raw_items, list) or len(raw_items) > query.limit:
            raise FomoFlowContractError("Apollo flow response has an invalid bounded items page")
        items = [_apollo_event(item) for item in raw_items]
        if query.verified and any(item["identity"]["kind"] != "verified-fomo" for item in items):
            raise FomoFlowContractError("Apollo verified flow included a non-verified identity")

        upstream_read_at = _iso(page.get("readAt"), "readAt", publisher="Apollo")
        upstream_evidence = _iso(
            page.get("evidenceThrough"), "evidenceThrough", publisher="Apollo", nullable=True,
        )
        if upstream_evidence != _latest(items, "observed_at"):
            raise FomoFlowContractError(
                "Apollo flow evidenceThrough does not match the newest returned observation"
            )
        raw_next_cursor = page.get("nextCursor")
        if raw_next_cursor is None:
            next_cursor = None
        elif isinstance(raw_next_cursor, str) and _CURSOR_RE.fullmatch(raw_next_cursor):
            next_cursor = raw_next_cursor
        else:
            raise FomoFlowContractError("Apollo flow response has an invalid nextCursor")

        selected = [
            item for item in items
            if query.chain is None or item["asset"]["chain"] == query.chain
        ]
        return {
            "items": selected,
            "next_cursor": next_cursor,
            "read_at": upstream_read_at,
            "evidence_through": _latest(selected, "observed_at"),
            "provenance": {
                "publisher": "Apollo",
                "source_id": "apollo",
                "upstream_origin": upstream.origin,
                "canonical_origin": PUBLIC_APOLLO_ORIGIN,
                "upstream_endpoint": APOLLO_FLOW_PATH,
                "upstream_contract": "FlowPage",
                "access": "anonymous-public-read",
                "retrieved_at": upstream.retrieved_at,
                "upstream_read_at": upstream_read_at,
                "upstream_evidence_through": upstream_evidence,
                "evidence_time_basis": "observed_at",
            },
            "coverage": {
                "supported_chains": list(SUPPORTED_CHAINS),
                "full_chain_complete": False,
                "requested_chain": query.chain,
                "upstream_page_items": len(items),
                "returned_items": len(selected),
                "items_filtered_by_chain": len(items) - len(selected),
                "identity_scope": (
                    "verified-fomo" if query.verified else "published observed-wallet and verified-fomo"
                ),
                "cursor_scope": "one bounded Apollo page; continue with next_cursor",
                "publisher_status": None,
                "rows_omitted": {},
            },
            "limitations": [
                "Coverage is the Apollo verified three-chain set, not every chain or every wallet.",
                "Identity kind is preserved; an observed wallet is not upgraded to a verified Fomo identity.",
                "Actions are published observations, not intent or recommendations.",
                "No pool route or LP behavior is inferred from a flow event.",
                "Server read time is not evidence time; use occurred_at and observed_at.",
            ],
        }

    @staticmethod
    def _rh_status(value: Mapping[str, Any]) -> dict[str, Any]:
        if value.get("ok") is not True or value.get("chain") != "robinhood" or value.get("chain_id") != 4663:
            raise FomoFlowContractError("RH Trenches status did not identify live Robinhood Chain 4663")
        last_event_at, last_event_unix = _epoch(value.get("last_ts"), "status.last_ts", nullable=True)
        server_at, server_unix = _epoch(value.get("server_ts"), "status.server_ts")
        latency = _object(value.get("latency"), "status.latency", publisher="RH Trenches")
        latency_since, latency_since_unix = _epoch(
            latency.get("since"), "status.latency.since", nullable=True,
        )
        source_transport = _text(
            value.get("source"), "status.source", publisher="RH Trenches", maximum=64,
        )
        return {
            "ok": True,
            "chain": "robinhood",
            "chain_id": 4663,
            "tracked_wallets": _integer(
                value.get("wallets"), "status.wallets", publisher="RH Trenches",
            ),
            "source_transport": source_transport,
            "last_block": str(_integer(
                value.get("last_block"), "status.last_block", publisher="RH Trenches",
            )),
            "last_event_at": last_event_at,
            "last_event_at_unix": last_event_unix,
            "server_at": server_at,
            "server_at_unix": server_unix,
            "lag_seconds_reported": _reported_decimal(
                value.get("lag_seconds"), "status.lag_seconds", nullable=True,
            ),
            "indexer_age_seconds_reported": _reported_decimal(
                value.get("indexer_age"), "status.indexer_age", nullable=True,
            ),
            "latency_seconds_reported": {
                "sample_count": _integer(
                    latency.get("n"), "status.latency.n", publisher="RH Trenches",
                ),
                "median": _reported_decimal(
                    latency.get("median"), "status.latency.median", nullable=True,
                ),
                "p90": _reported_decimal(
                    latency.get("p90"), "status.latency.p90", nullable=True,
                ),
                "since": latency_since,
                "since_unix": latency_since_unix,
            },
        }

    @classmethod
    def _rh_response(cls, query: _FlowQuery, upstream: _UpstreamPage) -> dict[str, Any]:
        if not isinstance(upstream.payload, list) or len(upstream.payload) > _RHTRENCHES_FETCH_LIMIT:
            raise FomoFlowContractError("RH Trenches flow response has an invalid bounded tape")
        raw_page = upstream.payload[:query.limit]
        selected: list[dict[str, Any]] = []
        omissions: Counter[str] = Counter()
        for raw in raw_page:
            item, omitted = _rh_event(raw)
            if item is not None:
                selected.append(item)
            elif omitted is not None:
                omissions[omitted] += 1
        if upstream.status is None:
            raise FomoFlowContractError("RH Trenches status response is absent")
        publisher_status = cls._rh_status(upstream.status)
        return {
            "items": selected,
            "next_cursor": None,
            "read_at": upstream.retrieved_at,
            "evidence_through": _latest(selected, "occurred_at"),
            "provenance": {
                "publisher": "RH Trenches / @Degenerate_DeFi",
                "source_id": "rhtrenches",
                "upstream_origin": upstream.origin,
                "canonical_origin": PUBLIC_RHTRENCHES_ORIGIN,
                "upstream_endpoint": RHTRENCHES_TAPE_PATH,
                "status_endpoint": RHTRENCHES_STATUS_PATH,
                "upstream_contract": "public tape and status JSON",
                "access": "anonymous-public-read",
                "retrieved_at": upstream.retrieved_at,
                "upstream_read_at": publisher_status["server_at"],
                "upstream_evidence_through": publisher_status["last_event_at"],
                "evidence_time_basis": "occurred_at; publisher provides no per-row observed_at",
                "attribution": "Unofficial RH Trenches public tape, made by @Degenerate_DeFi",
            },
            "coverage": {
                "supported_chains": ["robinhood"],
                "full_chain_complete": False,
                "requested_chain": "robinhood",
                "upstream_page_items": len(raw_page),
                "upstream_fetch_items": len(upstream.payload),
                "returned_items": len(selected),
                "items_filtered_by_chain": 0,
                "identity_scope": "publisher-associated handles with observed-wallet status only",
                "cursor_scope": "current bounded RH Trenches tape snapshot; no older cursor contract",
                "publisher_status": publisher_status,
                "rows_omitted": dict(sorted(omissions.items())),
            },
            "limitations": [
                "The live publisher status reports its tracked-wallet count; this is not all Fomo wallets or all chains.",
                "Handles remain observed-wallet associations; this adapter does not upgrade them to verified Fomo identity.",
                "Rows carrying publisher warning labels are omitted rather than republished without their qualification.",
                "Rows labeled non-cash-leg by RH Trenches are omitted because the publisher treats their USD value as estimated.",
                "P/L, followers, reputation, flags, market cap, liquidity, pair URLs, and lead/follower inference are not republished.",
                "The publisher supplies transaction and block but no instruction/log index, block hash, confirmation, or per-row observation time.",
                "No pool route or LP behavior is inferred from a tape row.",
            ],
        }

    def flow(self, params: Mapping[str, Any]) -> dict[str, Any]:
        """Return one bounded, evidence-qualified public Fomo flow projection."""
        if not isinstance(params, Mapping):
            raise ValueError("Fomo flow query must be a parameter mapping")
        query = _query(params)
        upstream = self._page(query)
        projected = (
            self._apollo_response(query, upstream)
            if query.source == "apollo"
            else self._rh_response(query, upstream)
        )
        return {
            "schema_version": "fomo-flow.v1",
            "query": {
                "source": query.source,
                "chain": query.chain,
                "chain_scope": (
                    query.chain if query.chain is not None else "supported-set"
                ),
                "verified_only": query.verified,
                "limit": query.limit,
            },
            "sources": _source_catalog(query.source),
            **projected,
            "coverage": {
                **projected["coverage"],
                "pool_attribution": {
                    "state": "not-provided",
                    "reason": (
                        "Selected public flow contracts do not provide an approved exact "
                        "same-transaction pool route; trades are not treated as LP activity"
                    ),
                },
            },
        }


__all__ = [
    "APOLLO_FLOW_PATH",
    "ENABLED_SOURCES",
    "PUBLIC_APOLLO_ORIGIN",
    "PUBLIC_RHTRENCHES_ORIGIN",
    "RHTRENCHES_STATUS_PATH",
    "RHTRENCHES_TAPE_PATH",
    "SUPPORTED_CHAINS",
    "FomoFlowContractError",
    "FomoFlowError",
    "FomoFlowService",
    "FomoFlowUnavailable",
]
