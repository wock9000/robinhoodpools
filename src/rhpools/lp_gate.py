"""Holder gate: owner-signed policy, balance oracle, one credential type, entitlement."""
from __future__ import annotations

import base64
import hashlib
import ipaddress
import json
import logging
import secrets
import sqlite3
import threading
import time
from collections import OrderedDict, deque
from contextlib import contextmanager
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Callable, Iterator, Literal, Mapping
from urllib.request import Request, urlopen

from eth_abi import encode as abi_encode
from eth_utils import keccak, to_checksum_address

from .lp_gate_siwe import host_of, parse_message, personal_sign_hash, recover_signer, verify_signer

Feature = Literal["trade", "lp", "api", "flags"]
FEATURES: tuple[Feature, ...] = ("trade", "lp", "api", "flags")
CHAIN_ID = 4663
COOKIE_NAME = "__Host-rhp_session"
SECRET_PREFIX = "rhp_"
POLICY_SKEW_S = 600
ORACLE_TTL_S = 30.0
QUALIFIED_WRITE_STEP_S = 5.0
LAST_USED_STEP_S = 60
_ZERO_ADDRESS = "0x" + "00" * 20
_UINT256_MAX = 2**256 - 1
_POLICY_TYPE = (
    "GatePolicy(uint64 version,address token,uint8 decimals,uint256 trade,uint256 lp,"
    "uint256 api,uint256 flags,uint32 graceSeconds,uint64 issuedAt,uint16 baseFeeBps,FeeTier[] feeTiers)"
    "FeeTier(uint16 minSupplyBps,uint16 feeBps)"
)
_TIER_TYPE = "FeeTier(uint16 minSupplyBps,uint16 feeBps)"
_DOMAIN_TYPE = "EIP712Domain(string name,string version,uint256 chainId)"
_DOMAIN_NAME = "rhpools gate"
_DOMAIN_VERSION = "2"

Via = Literal["cookie", "bearer"]
Kind = Literal["session", "key"]


class GateRefusal(Exception):
    def __init__(self, status: int, error: str, *, retry_after: int | None = None, **gate: Any) -> None:
        super().__init__(error)
        self.status = status
        self.retry_after = retry_after
        self.payload = {"error": error, "gate": gate}


def _uint(value: Any, bits: int, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        raise ValueError(f"{name} must be an integer")
    number = int(value, 10) if isinstance(value, str) else value
    if not 0 <= number < 2**bits:
        raise ValueError(f"{name} out of range")
    return number


@dataclass(frozen=True)
class FeeTier:
    min_supply_bps: int
    fee_bps: int

    def public(self) -> dict:
        return {"min_supply_bps": self.min_supply_bps, "fee_bps": self.fee_bps}


DEFAULT_FEE_TIERS = (FeeTier(10, 75), FeeTier(50, 50))


@dataclass(frozen=True)
class GatePolicy:
    version: int
    token: str | None
    decimals: int
    threshold: Mapping[Feature, int]
    grace_s: int
    issued_at: int
    base_fee_bps: int
    fee_tiers: tuple[FeeTier, ...]

    @staticmethod
    def parse(obj: Any) -> "GatePolicy":
        if not isinstance(obj, Mapping):
            raise ValueError("policy must be an object")
        unknown = set(obj) - {"version", "token", "decimals", "threshold", "grace_s", "issued_at", "base_fee_bps", "fee_tiers"}
        if unknown:
            raise ValueError("unknown policy fields: " + ", ".join(sorted(unknown)))
        token = obj.get("token")
        if token is not None:
            try:
                token = to_checksum_address(token)
            except (ValueError, TypeError) as exc:
                raise ValueError("token must be an address") from exc
            if token == _ZERO_ADDRESS:
                token = None
        thresholds = obj.get("threshold")
        if not isinstance(thresholds, Mapping) or set(thresholds) != set(FEATURES):
            raise ValueError("threshold must name exactly " + ", ".join(FEATURES))
        base_fee_bps = _uint(obj.get("base_fee_bps", 100), 16, "base_fee_bps")
        if base_fee_bps > 100:
            raise ValueError("base_fee_bps out of range")
        entries = obj.get("fee_tiers", [tier.public() for tier in DEFAULT_FEE_TIERS])
        if not isinstance(entries, list) or len(entries) > 4:
            raise ValueError("fee_tiers must be a list of at most four tiers")
        tiers = []
        previous_supply, previous_fee = -1, base_fee_bps + 1
        for entry in entries:
            if not isinstance(entry, Mapping) or set(entry) != {"min_supply_bps", "fee_bps"}:
                raise ValueError("fee tier must name min_supply_bps and fee_bps")
            supply = _uint(entry["min_supply_bps"], 16, "min_supply_bps")
            fee = _uint(entry["fee_bps"], 16, "fee_bps")
            if supply > 10000 or fee > 100 or supply <= previous_supply or fee >= previous_fee:
                raise ValueError("fee tiers require increasing supply and strictly decreasing fees within 0..100 bps")
            tiers.append(FeeTier(supply, fee))
            previous_supply, previous_fee = supply, fee
        return GatePolicy(
            version=_uint(obj.get("version"), 64, "version"), token=token,
            decimals=_uint(obj.get("decimals", 18), 8, "decimals"),
            threshold={feature: _uint(thresholds[feature], 256, feature) for feature in FEATURES},
            grace_s=_uint(obj.get("grace_s", 0), 32, "grace_s"),
            issued_at=_uint(obj.get("issued_at"), 64, "issued_at"),
            base_fee_bps=base_fee_bps, fee_tiers=tuple(tiers),
        )

    def public(self) -> dict:
        return {
            "version": self.version, "token": self.token, "decimals": self.decimals,
            "threshold": {feature: str(self.threshold[feature]) for feature in FEATURES},
            "grace_s": self.grace_s, "issued_at": self.issued_at,
            "base_fee_bps": self.base_fee_bps,
            "fee_tiers": [tier.public() for tier in self.fee_tiers],
        }

    def typed_data(self, chain_id: int = CHAIN_ID) -> dict:
        return {
            "types": {
                "EIP712Domain": [
                    {"name": "name", "type": "string"}, {"name": "version", "type": "string"},
                    {"name": "chainId", "type": "uint256"},
                ],
                "GatePolicy": [
                    {"name": "version", "type": "uint64"}, {"name": "token", "type": "address"},
                    {"name": "decimals", "type": "uint8"}, {"name": "trade", "type": "uint256"},
                    {"name": "lp", "type": "uint256"}, {"name": "api", "type": "uint256"},
                    {"name": "flags", "type": "uint256"}, {"name": "graceSeconds", "type": "uint32"},
                    {"name": "issuedAt", "type": "uint64"}, {"name": "baseFeeBps", "type": "uint16"},
                    {"name": "feeTiers", "type": "FeeTier[]"},
                ],
                "FeeTier": [
                    {"name": "minSupplyBps", "type": "uint16"}, {"name": "feeBps", "type": "uint16"},
                ],
            },
            "primaryType": "GatePolicy",
            "domain": {"name": _DOMAIN_NAME, "version": _DOMAIN_VERSION, "chainId": chain_id},
            "message": {
                "version": str(self.version), "token": self.token or _ZERO_ADDRESS,
                "decimals": str(self.decimals),
                **{feature: str(self.threshold[feature]) for feature in FEATURES},
                "graceSeconds": str(self.grace_s), "issuedAt": str(self.issued_at),
                "baseFeeBps": str(self.base_fee_bps),
                "feeTiers": [
                    {"minSupplyBps": str(tier.min_supply_bps), "feeBps": str(tier.fee_bps)}
                    for tier in self.fee_tiers
                ],
            },
        }

    def digest(self, chain_id: int = CHAIN_ID) -> bytes:
        domain = keccak(abi_encode(
            ["bytes32", "bytes32", "bytes32", "uint256"],
            [keccak(text=_DOMAIN_TYPE), keccak(text=_DOMAIN_NAME), keccak(text=_DOMAIN_VERSION), chain_id],
        ))
        tiers_hash = keccak(b"".join(
            keccak(abi_encode(
                ["bytes32", "uint16", "uint16"],
                [keccak(text=_TIER_TYPE), tier.min_supply_bps, tier.fee_bps],
            )) for tier in self.fee_tiers
        ))
        struct = keccak(abi_encode(
            ["bytes32", "uint64", "address", "uint8", "uint256", "uint256", "uint256", "uint256",
             "uint32", "uint64", "uint16", "bytes32"],
            [keccak(text=_POLICY_TYPE), self.version, self.token or _ZERO_ADDRESS, self.decimals,
             *(self.threshold[feature] for feature in FEATURES), self.grace_s, self.issued_at,
             self.base_fee_bps, tiers_hash],
        ))
        return keccak(b"\x19\x01" + domain + struct)


UNSET_POLICY = GatePolicy(version=0, token=None, decimals=18, threshold=dict.fromkeys(FEATURES, 0), grace_s=0, issued_at=0,
                          base_fee_bps=100, fee_tiers=DEFAULT_FEE_TIERS)


@dataclass(frozen=True)
class Holding:
    wallet: str
    balance_raw: int
    block: int
    observed_at: float
    total_supply_raw: int | None = None

    def public(self) -> dict:
        return {"balance_raw": str(self.balance_raw), "block": self.block, "observed_at": self.observed_at,
                "total_supply_raw": None if self.total_supply_raw is None else str(self.total_supply_raw)}


@dataclass(frozen=True)
class HoldingState:
    wallet: str
    holding: Holding | None
    last_ok: Mapping[Feature, tuple[float, int]] = field(default_factory=dict)


@dataclass(frozen=True)
class Entitlement:
    wallet: str
    features: frozenset[Feature]
    holding: Holding | None
    grace_until: Mapping[Feature, float]
    policy_version: int
    fee_bps: int
    tier: int

    def has(self, feature: Feature) -> bool:
        return feature in self.features

    @property
    def qualified(self) -> frozenset[Feature]:
        return self.features - frozenset(self.grace_until)

    def state(self, policy: GatePolicy) -> str:
        if policy.token is None:
            return "unset"
        if any(policy.threshold[feature] > 0 for feature in self.qualified):
            return "holder"
        if self.grace_until:
            return "grace"
        return "holder" if self.features == frozenset(FEATURES) else "below"

    def public(self) -> dict:
        return {
            "wallet": self.wallet,
            "features": [feature for feature in FEATURES if feature in self.features],
            "holding": None if self.holding is None else self.holding.public(),
            "grace_until": dict(self.grace_until),
            "policy_version": self.policy_version,
            "fee_bps": self.fee_bps, "tier": self.tier,
        }


def entitle(policy: GatePolicy, state: HoldingState, now: float) -> Entitlement:
    features: set[Feature] = set()
    grace_until: dict[Feature, float] = {}
    if policy.token is not None:
        for feature in FEATURES:
            threshold = policy.threshold[feature]
            fresh = state.holding is not None and now - state.holding.observed_at < ORACLE_TTL_S
            balance_ok = fresh and state.holding.balance_raw >= threshold
            if threshold == 0 or balance_ok:
                features.add(feature)
                continue
            anchor = state.last_ok.get(feature)
            if not fresh and anchor is not None and anchor[1] == policy.version and now - anchor[0] <= policy.grace_s:
                features.add(feature)
                grace_until[feature] = anchor[0] + policy.grace_s
    tier, fee_bps = 0, policy.base_fee_bps
    holding = state.holding
    if (holding is not None and now - holding.observed_at < ORACLE_TTL_S
            and holding.total_supply_raw is not None and holding.total_supply_raw > 0):
        for index, candidate in enumerate(policy.fee_tiers, 1):
            if holding.balance_raw * 10000 >= candidate.min_supply_bps * holding.total_supply_raw:
                tier, fee_bps = index, candidate.fee_bps
    return Entitlement(state.wallet, frozenset(features), holding, grace_until, policy.version, fee_bps, tier)


@dataclass(frozen=True)
class Principal:
    wallet: str
    key_id: str
    label: str
    via: Via
    kind: Kind
    expires_at: int


@dataclass(frozen=True)
class KeyRecord:
    key_id: str
    wallet: str
    kind: Kind
    label: str
    created_at: int
    expires_at: int
    revoked_at: int | None
    last_used_at: int | None

    def public(self) -> dict:
        return self.__dict__.copy()


@dataclass(frozen=True)
class Quota:
    limit: int
    remaining: int
    reset_at: float


@dataclass(frozen=True)
class Limits:
    session_ttl_s: int = 7 * 86400
    key_ttl_max_s: int = 365 * 86400
    keys_per_wallet: int = 8
    streams_per_wallet: int = 4
    key_rps: float = 10.0
    key_burst: int = 40
    nonce_ttl_s: int = 600
    signin_per_ip_per_min: int = 5
    nonce_per_ip_per_min: int = 20
    policy_per_ip_per_min: int = 10


def json_rpc(url: str, timeout: float = 5.0) -> Callable[[str, list], Any]:
    def call(method: str, params: list) -> Any:
        body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode()
        request = Request(url, data=body, headers={"Content-Type": "application/json"})
        with urlopen(request, timeout=timeout) as response:
            reply = json.loads(response.read())
        if not isinstance(reply, dict) or "result" not in reply:
            raise ValueError(str((reply or {}).get("error") if isinstance(reply, dict) else reply))
        return reply["result"]
    return call


class _Oracle:
    def __init__(self, rpc: Callable[[str, list], Any], *, clock: Callable[[], float],
                 ttl_s: float = ORACLE_TTL_S, max_wallets: int = 4096) -> None:
        self._rpc = rpc
        self._clock = clock
        self._ttl = ttl_s
        self._max = max_wallets
        self._lock = threading.Lock()
        self._cache: OrderedDict[str, Holding] = OrderedDict()
        self._inflight: dict[str, threading.Event] = {}
        self._supplies: dict[str, tuple[int | None, float]] = {}
        self._supply_inflight: dict[str, threading.Event] = {}

    def forget_all(self) -> None:
        with self._lock:
            self._cache.clear()
            self._supplies.clear()

    def observe(self, token: str, wallet: str, grace_s: float) -> Holding | None:
        now = self._clock()
        with self._lock:
            cached = self._cache.get(wallet)
            fresh = cached is not None and now - cached.observed_at < self._ttl
            if fresh:
                self._cache.move_to_end(wallet)
            else:
                pending = self._inflight.get(wallet)
                leader = pending is None
                if leader:
                    pending = self._inflight[wallet] = threading.Event()
        if fresh:
            return replace(cached, total_supply_raw=self._supply(token, cached.block, now))
        if not leader:
            pending.wait(10)
            with self._lock:
                cached = self._cache.get(wallet)
            result = self._usable(cached, now, grace_s)
            return None if result is None else replace(result, total_supply_raw=self._supply(token, result.block, now))
        holding = None
        try:
            holding = self._fetch(token, wallet, now)
        except (OSError, ValueError, TypeError, KeyError):
            pass
        finally:
            with self._lock:
                if holding is not None:
                    self._cache[wallet] = holding
                    self._cache.move_to_end(wallet)
                    while len(self._cache) > self._max:
                        self._cache.popitem(last=False)
                del self._inflight[wallet]
                pending.set()
        result = holding if holding is not None else self._usable(cached, now, grace_s)
        return None if result is None else replace(result, total_supply_raw=self._supply(token, result.block, now))

    @staticmethod
    def _usable(cached: Holding | None, now: float, grace_s: float) -> Holding | None:
        if cached is not None and now - cached.observed_at <= grace_s:
            return cached
        return None

    def _fetch(self, token: str, wallet: str, now: float) -> Holding:
        block = int(self._rpc("eth_blockNumber", []), 16)
        data = "0x70a08231" + wallet[2:].lower().rjust(64, "0")
        raw = self._rpc("eth_call", [{"to": token, "data": data}, hex(block)])
        if not isinstance(raw, str) or len(raw) != 66:
            raise ValueError("balanceOf returned no uint256")
        return Holding(wallet, int(raw, 16), block, now)

    def _supply(self, token: str, block: int, now: float) -> int | None:
        with self._lock:
            cached = self._supplies.get(token)
            if cached is not None and now - cached[1] < self._ttl:
                return cached[0]
            pending = self._supply_inflight.get(token)
            leader = pending is None
            if leader:
                pending = self._supply_inflight[token] = threading.Event()
        if not leader:
            pending.wait(10)
            with self._lock:
                cached = self._supplies.get(token)
            return cached[0] if cached is not None and now - cached[1] < self._ttl else None
        supply = None
        try:
            raw = self._rpc("eth_call", [{"to": token, "data": "0x18160ddd"}, hex(block)])
            if not isinstance(raw, str) or len(raw) != 66:
                raise ValueError("totalSupply returned no uint256")
            supply = int(raw, 16)
        except (OSError, ValueError, TypeError, KeyError):
            pass
        finally:
            with self._lock:
                self._supplies[token] = (supply, now)
                del self._supply_inflight[token]
                pending.set()
        return supply



class _Window:
    def __init__(self, per_key: int, *, clock: Callable[[], float]) -> None:
        self._per_key, self._clock = per_key, clock
        self._lock = threading.Lock()
        self._keys: OrderedDict[str, deque] = OrderedDict()

    def allowed(self, key: str) -> bool:
        now = self._clock()
        with self._lock:
            bucket = self._keys.get(key)
            if bucket is None:
                return True
            while bucket and bucket[0] <= now - 60:
                bucket.popleft()
            return len(bucket) < self._per_key

    def admit(self, key: str) -> bool:
        now = self._clock()
        with self._lock:
            bucket = self._keys.setdefault(key, deque())
            self._keys.move_to_end(key)
            while bucket and bucket[0] <= now - 60:
                bucket.popleft()
            if len(bucket) >= self._per_key:
                return False
            bucket.append(now)
            while len(self._keys) > 4096:
                self._keys.popitem(last=False)
            return True


def _ip_bucket(address: str) -> str:
    try:
        parsed = ipaddress.ip_address(address)
        if parsed.version == 6:
            return str(ipaddress.ip_network((parsed, 64), strict=False))
        return str(parsed)
    except ValueError:
        return address[:64]


class Gate:
    def __init__(self, db_path: Path | str, *, owner: str | None, rpc_url: str, hosts: frozenset[str],
                 chain_id: int = CHAIN_ID, limits: Limits = Limits(), clock: Callable[[], float] = time.time,
                 rpc: Callable[[str, list], Any] | None = None) -> None:
        self.owner = to_checksum_address(owner) if owner else None
        self.chain_id = chain_id
        self.hosts = frozenset(host.lower() for host in hosts)
        self.limits = limits
        self._clock = clock
        self._rpc = rpc or json_rpc(rpc_url)
        self._oracle = _Oracle(self._rpc, clock=clock)
        self._lock = threading.RLock()
        self._db = sqlite3.connect(str(db_path), check_same_thread=False, isolation_level=None, timeout=5.0)
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA busy_timeout=5000")
        self._db.executescript(_SCHEMA)
        self._policy: GatePolicy = UNSET_POLICY
        self._policy_read_at = float("-inf")
        self._nonces: OrderedDict[str, float] = OrderedDict()
        self._buckets: OrderedDict[str, tuple[float, float]] = OrderedDict()
        self._streams: dict[str, int] = {}
        self._signins = _Window(limits.signin_per_ip_per_min, clock=clock)
        self._nonce_requests = _Window(limits.nonce_per_ip_per_min, clock=clock)
        self._policy_requests = _Window(limits.policy_per_ip_per_min, clock=clock)
        self._policy_slots = threading.BoundedSemaphore(2)
        self._policy_refusals = 0
        self._policy_log_at = float("-inf")

    def close(self) -> None:
        with self._lock:
            self._db.close()

    def resolve(self, headers: Mapping[str, str]) -> Principal | None:
        scheme, _, value = str(headers.get("Authorization") or "").partition(" ")
        value = value.strip()
        if scheme.lower() == "bearer" and value.startswith(SECRET_PREFIX):
            principal = self._lookup(value, "bearer")
            if principal is None:
                raise GateRefusal(401, "credential invalid", state="invalid")
            return principal
        for part in str(headers.get("Cookie") or "").split(";"):
            name, _, secret = part.strip().partition("=")
            if name == COOKIE_NAME and secret.startswith(SECRET_PREFIX):
                return self._lookup(secret, "cookie")
        return None

    def entitlement(self, wallet: str) -> Entitlement:
        policy = self.policy()
        now = self._clock()
        holding = self._oracle.observe(policy.token, wallet, policy.grace_s) if policy.token else None
        with self._lock:
            rows = self._db.execute(
                "SELECT feature, last_ok_at, policy_version FROM qualified WHERE wallet = ?", (wallet,),
            ).fetchall()
        anchors = {feature: (float(at), int(version)) for feature, at, version in rows}
        if holding is not None and now - holding.observed_at < ORACLE_TTL_S:
            dropped = [feature for feature in anchors if policy.threshold[feature] > 0
                       and holding.balance_raw < policy.threshold[feature]]
            if dropped:
                with self._lock:
                    self._db.executemany(
                        "DELETE FROM qualified WHERE wallet = ? AND feature = ?",
                        [(wallet, feature) for feature in dropped],
                    )
                for feature in dropped:
                    del anchors[feature]
        ent = entitle(policy, HoldingState(wallet, holding, anchors), now)
        moved = []
        for feature in ent.qualified:
            anchor = anchors.get(feature)
            if policy.threshold[feature] > 0 and (
                anchor is None or anchor[1] != policy.version or holding.observed_at - anchor[0] >= QUALIFIED_WRITE_STEP_S
            ):
                moved.append(feature)
        if moved:
            with self._lock:
                self._db.executemany(
                    "INSERT INTO qualified(wallet, feature, last_ok_at, policy_version) VALUES (?, ?, ?, ?) "
                    "ON CONFLICT(wallet, feature) DO UPDATE SET last_ok_at = excluded.last_ok_at, "
                    "policy_version = excluded.policy_version",
                    [(wallet, feature, holding.observed_at, policy.version) for feature in moved],
                )
        return ent

    def require(self, headers: Mapping[str, str], feature: Feature) -> tuple[Principal, Entitlement]:
        principal = self.resolve(headers)
        if principal is None:
            raise GateRefusal(401, "credential required", state="anonymous")
        return principal, self._entitled(principal.wallet, feature)

    def recheck(self, principal: Principal, feature: Feature) -> Entitlement:
        if self._lookup_id(principal.key_id) is None:
            raise GateRefusal(401, "credential revoked", state="revoked")
        return self._entitled(principal.wallet, feature)

    def _entitled(self, wallet: str, feature: Feature) -> Entitlement:
        ent = self.entitlement(wallet)
        if ent.has(feature):
            return ent
        policy = self.policy()
        raise GateRefusal(
            403, "not entitled", state=ent.state(policy), feature=feature,
            need=str(policy.threshold[feature]),
            have=None if ent.holding is None else str(ent.holding.balance_raw),
            grace_until=None,
        )

    def admit(self, principal: Principal, cost: int = 1) -> Quota:
        limits = self.limits
        now = self._clock()
        with self._lock:
            tokens, last = self._buckets.get(principal.key_id, (float(limits.key_burst), now))
            tokens = min(float(limits.key_burst), tokens + (now - last) * limits.key_rps)
            if tokens < cost:
                self._buckets[principal.key_id] = (tokens, now)
                wait_ms = int((cost - tokens) / limits.key_rps * 1000) + 1
                raise GateRefusal(429, "key rate limit", retry_after=max(1, -(-wait_ms // 1000)), retry_after_ms=wait_ms)
            tokens -= cost
            self._buckets[principal.key_id] = (tokens, now)
            self._buckets.move_to_end(principal.key_id)
            while len(self._buckets) > 4096:
                self._buckets.popitem(last=False)
        return Quota(limits.key_burst, int(tokens), now + (limits.key_burst - tokens) / limits.key_rps)

    @contextmanager
    def stream_slot(self, principal: Principal) -> Iterator[None]:
        with self._lock:
            held = self._streams.get(principal.wallet, 0)
            if held >= self.limits.streams_per_wallet:
                raise GateRefusal(429, "stream limit per wallet", retry_after=5, limit=self.limits.streams_per_wallet)
            self._streams[principal.wallet] = held + 1
        try:
            yield
        finally:
            with self._lock:
                remaining = self._streams[principal.wallet] - 1
                if remaining:
                    self._streams[principal.wallet] = remaining
                else:
                    del self._streams[principal.wallet]

    def nonce(self, wallet: str | None = None, *, client_ip: str | None = None) -> dict:
        if wallet:
            try:
                wallet = to_checksum_address(wallet)
            except (ValueError, TypeError) as exc:
                raise GateRefusal(400, "wallet must be an address") from exc
        if client_ip is not None and not self._nonce_requests.admit(_ip_bucket(client_ip)):
            raise GateRefusal(429, "nonce rate limit", retry_after=60)
        now = self._clock()
        value = secrets.token_hex(16)
        with self._lock:
            while self._nonces and next(iter(self._nonces.values())) <= now:
                self._nonces.popitem(last=False)
            if len(self._nonces) >= 4096:
                raise GateRefusal(503, "nonce capacity reached", retry_after=30)
            self._nonces[value] = now + self.limits.nonce_ttl_s
        reply = {
            "nonce": value, "issued_at": _iso(now), "expires_at": _iso(now + self.limits.nonce_ttl_s),
            "chain_id": self.chain_id, "statement": "Sign in to rhpools. No transaction, no fee.",
        }
        if wallet:
            reply["address"] = wallet
        return reply

    def sign_in(self, message: str, signature: str, *, label: str, client_ip: str,
                host: str | None = None) -> tuple[Principal, str]:
        bucket = _ip_bucket(client_ip)
        now = self._clock()
        try:
            siwe = parse_message(message)
            domain = siwe.domain.lower()
            allowed = domain in self.hosts or (host is not None and domain == host.lower())
            if not allowed or host_of(siwe.uri) != domain:
                raise ValueError("domain not served here")
            if siwe.chain_id != self.chain_id:
                raise ValueError(f"chain id must be {self.chain_id}")
            if siwe.expiration_time is not None and siwe.expiration_time <= now:
                raise ValueError("message expired")
            if siwe.not_before is not None and siwe.not_before > now:
                raise ValueError("message not yet valid")
            if siwe.issued_at > now + 60:
                raise ValueError("issued in the future")
            with self._lock:
                fresh = self._nonces.get(siwe.nonce)
            if fresh is None or fresh <= now:
                raise ValueError("nonce unknown, used or expired")
            if not self._signins.allowed(bucket):
                raise GateRefusal(429, "sign-in rate limit", retry_after=60)
            if not verify_signer(siwe.address, personal_sign_hash(message), signature, self._rpc):
                self._signins.admit(bucket)
                raise ValueError("signature does not match the address")
            with self._lock:
                if self._nonces.pop(siwe.nonce, None) is None:
                    raise ValueError("nonce unknown, used or expired")
        except GateRefusal:
            raise
        except ValueError as exc:
            raise GateRefusal(401, "sign-in refused", state="refused", reason=str(exc)) from exc
        principal, secret = self._mint(siwe.address, "session", label, self.limits.session_ttl_s, "cookie")
        self._audit(siwe.address, "web", "session.sign_in", {"key_id": principal.key_id, "label": label, "ip": client_ip})
        return principal, secret

    def mint_key(self, principal: Principal, *, label: str, ttl_s: int) -> tuple[Principal, str]:
        if principal.kind != "session" or principal.via != "cookie":
            raise GateRefusal(403, "keys are minted from a browser session only", state="forbidden")
        self._entitled(principal.wallet, "api")
        ttl = max(60, min(int(ttl_s), self.limits.key_ttl_max_s))
        now = int(self._clock())
        with self._lock:
            live = self._db.execute(
                "SELECT COUNT(*) FROM credential WHERE wallet = ? AND kind = 'key' AND revoked_at IS NULL AND expires_at > ?",
                (principal.wallet, now),
            ).fetchone()[0]
            if live >= self.limits.keys_per_wallet:
                raise GateRefusal(403, "key limit reached", state="forbidden", limit=self.limits.keys_per_wallet)
            minted, secret = self._mint(principal.wallet, "key", label, ttl, "bearer")
        self._audit(principal.wallet, "web", "key.mint", {"key_id": minted.key_id, "label": label, "by": principal.key_id})
        return minted, secret

    def keys(self, wallet: str) -> list[KeyRecord]:
        with self._lock:
            rows = self._db.execute(
                "SELECT key_id, wallet, kind, label, created_at, expires_at, revoked_at, last_used_at "
                "FROM credential WHERE wallet = ? ORDER BY created_at DESC, key_id", (wallet,),
            ).fetchall()
        return [KeyRecord(*row) for row in rows]

    def revoke(self, key_id: str, *, wallet: str | None = None, via: str = "web") -> bool:
        now = int(self._clock())
        with self._lock:
            params = (now, key_id) if wallet is None else (now, key_id, wallet)
            changed = self._db.execute(
                "UPDATE credential SET revoked_at = ? WHERE key_id = ? AND revoked_at IS NULL"
                + ("" if wallet is None else " AND wallet = ?"), params,
            ).rowcount
        if changed:
            self._audit(wallet or "owner", via, "key.revoke", {"key_id": key_id})
        return bool(changed)

    def revoke_all(self, wallet: str) -> int:
        now = int(self._clock())
        with self._lock:
            changed = self._db.execute(
                "UPDATE credential SET revoked_at = ? WHERE wallet = ? AND revoked_at IS NULL", (now, wallet),
            ).rowcount
        if changed:
            self._audit(wallet, "web", "key.revoke_all", {"count": changed})
        return changed

    def _mint(self, wallet: str, kind: Kind, label: str, ttl_s: int, via: Via) -> tuple[Principal, str]:
        secret = SECRET_PREFIX + base64.urlsafe_b64encode(secrets.token_bytes(32)).rstrip(b"=").decode()
        key_hash = hashlib.sha256(secret.encode()).hexdigest()
        key_id = key_hash[:16]
        now = int(self._clock())
        label = str(label or kind)[:64]
        with self._lock:
            self._db.execute(
                "INSERT INTO credential(key_hash, key_id, wallet, kind, label, created_at, expires_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (key_hash, key_id, wallet, kind, label, now, now + ttl_s),
            )
        return Principal(wallet, key_id, label, via, kind, now + ttl_s), secret

    def _lookup(self, secret: str, via: Via) -> Principal | None:
        key_hash = hashlib.sha256(secret.encode()).hexdigest()
        now = int(self._clock())
        with self._lock:
            row = self._db.execute(
                "SELECT key_id, wallet, kind, label, expires_at, last_used_at FROM credential "
                "WHERE key_hash = ? AND revoked_at IS NULL AND expires_at > ?", (key_hash, now),
            ).fetchone()
            if row is None:
                return None
            key_id, wallet, kind, label, expires_at, last_used_at = row
            if last_used_at is None or now - last_used_at >= LAST_USED_STEP_S:
                self._db.execute("UPDATE credential SET last_used_at = ? WHERE key_id = ?", (now, key_id))
        return Principal(wallet, key_id, label, via, kind, expires_at)

    def _lookup_id(self, key_id: str) -> str | None:
        with self._lock:
            row = self._db.execute(
                "SELECT wallet FROM credential WHERE key_id = ? AND revoked_at IS NULL AND expires_at > ?",
                (key_id, int(self._clock())),
            ).fetchone()
        return None if row is None else row[0]

    def policy(self) -> GatePolicy:
        now = self._clock()
        with self._lock:
            if now - self._policy_read_at >= 5.0 or self._policy_read_at > now:
                row = self._db.execute("SELECT body FROM policy ORDER BY version DESC LIMIT 1").fetchone()
                loaded = UNSET_POLICY if row is None else GatePolicy.parse(json.loads(row[0]))
                if loaded.token != self._policy.token:
                    self._oracle.forget_all()
                self._policy = loaded
                self._policy_read_at = now
            return self._policy

    def apply_policy(self, policy: GatePolicy, signature: str, *, via: Literal["web", "cli"]) -> GatePolicy:
        now = self._clock()
        try:
            signer = recover_signer(policy.digest(self.chain_id), signature)
        except ValueError as exc:
            self._record_policy_refusal()
            raise GateRefusal(400, "policy signature malformed", state="refused", reason=str(exc)) from exc
        if self.owner is None or signer.lower() != self.owner.lower():
            self._record_policy_refusal()
            reason = "gate owner not configured" if self.owner is None else "signer is not the gate owner"
            raise GateRefusal(403, "policy refused", state="refused", reason=reason, signer=signer)
        current = self.policy()
        with self._lock:
            row = self._db.execute("SELECT signature FROM policy WHERE version = ?", (policy.version,)).fetchone()
        if row is not None and row[0] == signature and policy.version == current.version:
            return current
        reason = None
        if policy.version <= current.version:
            reason = f"version must exceed {current.version}"
        elif abs(policy.issued_at - now) > POLICY_SKEW_S:
            reason = f"issued_at outside +/-{POLICY_SKEW_S} s"
        if reason is not None:
            self._audit(signer, via, "policy.refused", {"reason": reason, "version": policy.version})
            status = 409 if reason.startswith("version") else 400 if reason.startswith("issued_at") else 403
            raise GateRefusal(status, "policy refused", state="refused", reason=reason, signer=signer)
        with self._lock:
            self._db.execute("BEGIN")
            try:
                self._db.execute(
                    "INSERT INTO policy(version, body, signature, signer, via, applied_at) VALUES (?, ?, ?, ?, ?, ?)",
                    (policy.version, json.dumps(policy.public(), sort_keys=True), signature, signer, via, now),
                )
                self._db.execute(
                    "INSERT INTO audit(at, actor, via, action, detail) VALUES (?, ?, ?, ?, ?)",
                    (now, signer, via, "policy.apply", json.dumps(policy.public(), sort_keys=True)),
                )
                self._db.execute("COMMIT")
            except BaseException:
                self._db.execute("ROLLBACK")
                raise
            self._policy = policy
            self._policy_read_at = now
            self._oracle.forget_all()
        return policy

    def policy_admit(self, client_ip: str) -> None:
        if not self._policy_requests.admit(_ip_bucket(client_ip)):
            raise GateRefusal(429, "policy rate limit", retry_after=60)

    def _record_policy_refusal(self) -> None:
        with self._lock:
            self._policy_refusals += 1
            now = self._clock()
            if now - self._policy_log_at >= 60:
                logging.getLogger(__name__).warning("Refused %d non-owner or malformed policy signatures", self._policy_refusals)
                self._policy_refusals = 0
                self._policy_log_at = now

    def audit(self, limit: int = 50) -> list[dict]:
        with self._lock:
            rows = self._db.execute(
                "SELECT id, at, actor, via, action, detail FROM audit ORDER BY id DESC LIMIT ?", (max(1, min(int(limit), 500)),),
            ).fetchall()
        return [
            {"id": row[0], "at": row[1], "actor": row[2], "via": row[3], "action": row[4], "detail": json.loads(row[5])}
            for row in rows
        ]

    def status(self) -> dict:
        return {"policy": self.policy().public(), "owner": self.owner, "chain_id": self.chain_id, "typed_data": self.policy().typed_data(self.chain_id)}

    def is_owner(self, wallet: str) -> bool:
        return self.owner is not None and wallet.lower() == self.owner.lower()

    def _audit(self, actor: str, via: str, action: str, detail: dict) -> None:
        with self._lock:
            self._db.execute(
                "INSERT INTO audit(at, actor, via, action, detail) VALUES (?, ?, ?, ?, ?)",
                (self._clock(), actor, via, action, json.dumps(detail, sort_keys=True)),
            )


def _iso(timestamp: float) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(timestamp))


_SCHEMA = """
CREATE TABLE IF NOT EXISTS policy(
    version INTEGER PRIMARY KEY, body TEXT NOT NULL, signature TEXT NOT NULL,
    signer TEXT NOT NULL, via TEXT NOT NULL, applied_at REAL NOT NULL);
CREATE TABLE IF NOT EXISTS credential(
    key_hash TEXT PRIMARY KEY, key_id TEXT NOT NULL UNIQUE, wallet TEXT NOT NULL, kind TEXT NOT NULL,
    label TEXT NOT NULL, created_at INTEGER NOT NULL, expires_at INTEGER NOT NULL,
    revoked_at INTEGER, last_used_at INTEGER);
CREATE INDEX IF NOT EXISTS credential_wallet ON credential(wallet);
CREATE TABLE IF NOT EXISTS qualified(
    wallet TEXT NOT NULL, feature TEXT NOT NULL, last_ok_at REAL NOT NULL, policy_version INTEGER NOT NULL,
    PRIMARY KEY(wallet, feature));
CREATE TABLE IF NOT EXISTS audit(
    id INTEGER PRIMARY KEY AUTOINCREMENT, at REAL NOT NULL, actor TEXT NOT NULL, via TEXT NOT NULL,
    action TEXT NOT NULL, detail TEXT NOT NULL);
"""
