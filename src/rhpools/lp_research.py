"""Read-only, coverage-qualified research views over canonical LP accounting."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping, Sequence
import math
import re
import statistics
import time
from typing import Any
from urllib.parse import urlencode

_ADDRESS_RE = re.compile(r"0x[0-9a-f]{40}")
_WINDOWS = {"1h": 3_600, "24h": 86_400, "7d": 604_800, "30d": 2_592_000, "all": None}
_OWNER_POSITION_LIMIT = 200
_V4_DYNAMIC_FEE_FLAG = 0x800000
_USDG_BASIS = "USDG quote (1 USDG = 1 quote dollar); not a fiat oracle"


def _finite(value: Any) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def _raw(value: Any) -> str | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        text = str(value).strip().lower()
        number = int(text, 16) if text.startswith("0x") else int(text)
        return str(number)
    except (TypeError, ValueError, OverflowError):
        return None


def _raw_nonzero(value: Any) -> bool:
    raw = _raw(value)
    return raw is not None and raw != "0"


def _known_sum(values: Sequence[float | None]) -> float | None:
    known = [value for value in values if value is not None]
    return sum(known) if known else None


def _pair(row: Mapping[str, Any], metadata: Mapping[str, Any]) -> str:
    if row.get("pair"):
        return str(row["pair"])
    left = metadata.get("symbol0") or str(metadata.get("token0") or "?")[:8]
    right = metadata.get("symbol1") or str(metadata.get("token1") or "?")[:8]
    return f"{left}/{right}"


def _fee(metadata: Mapping[str, Any], protocol: str) -> dict[str, Any]:
    raw_fee = _raw(metadata.get("fee_ppm"))
    current_fee = _raw(metadata.get("current_fee_ppm"))
    parsed = int(raw_fee) if raw_fee is not None else None
    if protocol == "v4" and parsed is not None and parsed & _V4_DYNAMIC_FEE_FLAG:
        mode = "dynamic"
        configured = None
    elif parsed is None:
        mode = "unknown"
        configured = None
    else:
        mode = "static"
        configured = raw_fee
    return {
        "mode": mode,
        "raw_configuration": raw_fee,
        "configured_ppm": configured,
        "current_ppm": current_fee if current_fee is not None else configured,
        "hook": metadata.get("hook"),
    }


def _is_current(row: Mapping[str, Any]) -> bool:
    if str(row.get("status") or "").lower() == "historical":
        return False
    episodes = row.get("episodes")
    if isinstance(episodes, Sequence) and not isinstance(episodes, (str, bytes)):
        if any(isinstance(item, Mapping) and item.get("closed_at") is None for item in episodes):
            return True
    if str(row.get("status") or "").lower() in {
        "open", "awaiting_claim", "partial_history", "ambiguous_reentry",
    }:
        return True
    return any(_raw_nonzero(row.get(field)) for field in (
        "liquidity", "pending_principal0", "pending_principal1",
        "tokens_owed0", "tokens_owed1",
    ))


def _ownership_match(row: Mapping[str, Any], owner: str) -> str:
    if str(row.get("owner") or "").lower() == owner:
        return "beneficial_owner"
    if str(row.get("custody") or "").lower() == owner:
        return "custody"
    return str(row.get("identity_match") or "unresolved")


def _window_complete(coverage: Mapping[str, Any], window: str) -> bool:
    seconds = _WINDOWS[window]
    if seconds is None:
        return False
    history_from = _finite(coverage.get("history_from"))
    return history_from is not None and history_from <= time.time() - seconds


class LPResearchService:
    """Build a bounded research response from the existing canonical LP model."""

    def __init__(self, lp_service: Any):
        self.lp_service = lp_service

    @staticmethod
    def _params(params: Mapping[str, Any]) -> tuple[str, str, str, str]:
        owner = str(params.get("owner") or "").strip().lower()
        if not _ADDRESS_RE.fullmatch(owner):
            raise ValueError("owner must be a 20-byte hexadecimal address")
        window = str(params.get("window") or "30d").strip().lower()
        if window not in _WINDOWS:
            raise ValueError("window must be 1h, 24h, 7d, 30d or all")
        protocol = str(params.get("protocol") or "").strip().lower()
        if protocol and protocol not in {"v2", "v3", "v4"}:
            raise ValueError("protocol must be v2, v3 or v4")
        pool_id = str(params.get("pool") or params.get("pool_id") or "").strip().lower()
        if len(pool_id) > 128:
            raise ValueError("pool id is too long")
        return owner, window, protocol, pool_id

    def _pool_metadata(self, pool_ids: Sequence[str]) -> dict[str, dict[str, Any]]:
        selected = sorted(set(pool_id for pool_id in pool_ids if pool_id))
        if not selected:
            return {}
        store = getattr(self.lp_service, "store", None)
        reader = getattr(store, "read", None)
        if not callable(reader):
            return {}
        marks = ",".join("?" for _ in selected)
        rows = reader().execute(
            "SELECT p.id,p.protocol,p.token0,p.token1,p.symbol0,p.symbol1,"
            "p.decimals0,p.decimals1,p.fee_ppm,p.tick_spacing,p.hook,"
            "s.fee_ppm AS current_fee_ppm,s.tick AS current_tick,"
            "s.block_number AS valuation_block,s.timestamp AS valuation_timestamp "
            "FROM pools p LEFT JOIN lp_pool_state s ON s.pool_id=p.id "
            f"WHERE p.id IN ({marks})",
            selected,
        ).fetchall()
        return {str(row["id"]).lower(): dict(row) for row in rows}

    def _latest_activity(
        self,
        owner: str,
        window: str,
        protocol: str,
        pool_id: str,
        positions: Sequence[Mapping[str, Any]],
    ) -> tuple[dict[str, Any] | None, dict[str, Any]]:
        current_method = getattr(self.lp_service, "_current_owner_snapshot", None)
        current_status: dict[str, Any] = {
            "qualification": "unavailable", "head": None, "observed_from": None,
        }
        if callable(current_method):
            snapshot = current_method({
                "window": window, "protocol": protocol, "pool": pool_id, "q": owner,
            })
            current_status = dict(snapshot.get("status") or {})
            matches = [
                row for row in snapshot.get("rows") or ()
                if str(row.get("owner") or row.get("custody") or "").lower() == owner
            ]
            beneficial = [row for row in matches if str(row.get("owner") or "").lower() == owner]
            selected = beneficial[0] if beneficial else (matches[0] if matches else None)
            if selected is not None and isinstance(selected.get("activity"), Mapping):
                return dict(selected["activity"]), current_status
        candidates: list[dict[str, Any]] = []
        for row in positions:
            timestamp = row.get("last_event_at")
            if timestamp is None:
                continue
            candidates.append({
                "block_number": None,
                "timestamp": timestamp,
                "pool_id": row.get("pool_id"),
                "pair": row.get("pair"),
                "kind": "position_update",
                "tx_hash": None,
                "event_count": None,
                "qualification": "durable_position_summary",
                "identity_match": _ownership_match(row, owner),
            })
        latest = max(candidates, key=lambda row: int(row.get("timestamp") or 0), default=None)
        return latest, current_status

    def owner(self, params: Mapping[str, Any]) -> dict[str, Any]:
        owner, window, protocol, pool_id = self._params(params)
        query = {"owner": owner, "window": window}
        if protocol:
            query["protocol"] = protocol
        if pool_id:
            query["pool"] = pool_id
        detail = self.lp_service.owner(query)
        source_positions = [
            dict(row) for row in detail.get("positions") or ()
            if isinstance(row, Mapping)
        ]
        positions = source_positions[:_OWNER_POSITION_LIMIT]
        metadata = self._pool_metadata([
            str(row.get("pool_id") or "").lower() for row in positions
        ])
        current_rows = [row for row in positions if _is_current(row)]
        direct = [row for row in current_rows if _ownership_match(row, owner) == "beneficial_owner"]
        custody = [row for row in current_rows if _ownership_match(row, owner) == "custody"]

        allocation_positions: list[dict[str, Any]] = []
        pool_groups: dict[str, dict[str, Any]] = {}
        position_values: list[float | None] = []
        claim_values: list[float | None] = []
        for row in direct:
            position_pool = str(row.get("pool_id") or "").lower()
            pool = metadata.get(position_pool, {})
            pair = _pair(row, pool)
            principal = _finite(row.get("principal_usd"))
            claim = _finite(row.get("claim_principal_usd"))
            known_value = _known_sum([principal, claim])
            valuation_state = (
                "complete" if principal is not None and claim is not None
                else "partial" if known_value is not None else "unknown"
            )
            position_values.append(known_value)
            claim_values.append(claim)
            token_amounts = []
            for side in (0, 1):
                token_amounts.append({
                    "address": pool.get(f"token{side}"),
                    "symbol": pool.get(f"symbol{side}"),
                    "decimals": pool.get(f"decimals{side}"),
                    "principal_raw": _raw(row.get(f"principal{side}")),
                    "pending_claim_raw": _raw(row.get(f"pending_principal{side}")),
                })
            allocation_positions.append({
                "position_key": row.get("position_key"),
                "token_id": _raw(row.get("token_id")),
                "pool_id": position_pool or None,
                "pair": pair,
                "protocol": row.get("protocol"),
                "status": row.get("status"),
                "liquidity_raw": _raw(row.get("liquidity")),
                "token_amounts": token_amounts,
                "known_principal_usdg": known_value,
                "known_active_principal_usdg": principal,
                "known_pending_claim_usdg": claim,
                "valuation_state": valuation_state,
                "valuation_block": row.get("valuation_block"),
                "valuation_timestamp": row.get("valuation_timestamp"),
                "valuation_basis": row.get("valuation_basis") or "unknown",
                "coverage": dict(row.get("coverage") or {}),
            })
            group = pool_groups.setdefault(position_pool, {
                "pool_id": position_pool or None,
                "pair": pair,
                "protocol": row.get("protocol"),
                "position_count": 0,
                "valued_positions": 0,
                "partially_valued_positions": 0,
                "unvalued_positions": 0,
                "known_values": [],
                "raw": [defaultdict(int), defaultdict(int)],
                "raw_known": [defaultdict(int), defaultdict(int)],
                "token": [
                    {"address": pool.get("token0"), "symbol": pool.get("symbol0"), "decimals": pool.get("decimals0")},
                    {"address": pool.get("token1"), "symbol": pool.get("symbol1"), "decimals": pool.get("decimals1")},
                ],
            })
            group["position_count"] += 1
            group["known_values"].append(known_value)
            group[{
                "complete": "valued_positions",
                "partial": "partially_valued_positions",
                "unknown": "unvalued_positions",
            }[valuation_state]] += 1
            for side in (0, 1):
                for field in ("principal", "pending_claim"):
                    raw = token_amounts[side][f"{field}_raw"]
                    if raw is not None:
                        group["raw"][side][field] += int(raw)
                        group["raw_known"][side][field] += 1

        known_total = _known_sum(position_values)
        allocation_pools: list[dict[str, Any]] = []
        for group in pool_groups.values():
            pool_value = _known_sum(group.pop("known_values"))
            raw_maps = group.pop("raw")
            raw_known = group.pop("raw_known")
            tokens = group.pop("token")
            token_amounts = []
            for side in (0, 1):
                token_amounts.append({
                    **tokens[side],
                    "known_principal_raw": (
                        str(raw_maps[side]["principal"])
                        if raw_known[side]["principal"] else None
                    ),
                    "known_pending_claim_raw": (
                        str(raw_maps[side]["pending_claim"])
                        if raw_known[side]["pending_claim"] else None
                    ),
                    "positions_with_known_principal": raw_known[side]["principal"],
                    "positions_with_known_pending_claim": raw_known[side]["pending_claim"],
                    "principal_complete": raw_known[side]["principal"] == group["position_count"],
                    "pending_claim_complete": raw_known[side]["pending_claim"] == group["position_count"],
                })
            group.update({
                "known_principal_usdg": pool_value,
                "share_of_known_principal_pct": (
                    100.0 * pool_value / known_total
                    if pool_value is not None and known_total not in (None, 0.0) else None
                ),
                "token_amounts": token_amounts,
                "inspector": "/pool?" + urlencode({"id": group["pool_id"], "owner": owner}),
            })
            allocation_pools.append(group)
        allocation_pools.sort(key=lambda row: (
            row.get("known_principal_usdg") is not None,
            row.get("known_principal_usdg") or 0.0,
            row.get("pool_id") or "",
        ), reverse=True)

        configurations: list[dict[str, Any]] = []
        protocol_mix: dict[str, dict[str, Any]] = {}
        fee_mix: dict[str, dict[str, Any]] = {}
        widths: list[int] = []
        lifecycle_entries: list[dict[str, Any]] = []
        for row in positions:
            position_pool = str(row.get("pool_id") or "").lower()
            pool = metadata.get(position_pool, {})
            position_protocol = str(row.get("protocol") or pool.get("protocol") or "unknown")
            fee = _fee(pool, position_protocol)
            lower = row.get("tick_lower")
            upper = row.get("tick_upper")
            width = (
                int(upper) - int(lower)
                if lower is not None and upper is not None and int(upper) >= int(lower)
                else None
            )
            if width is not None and position_protocol in {"v3", "v4"}:
                widths.append(width)
            current_tick = pool.get("current_tick")
            in_range = (
                int(lower) <= int(current_tick) < int(upper)
                if lower is not None and upper is not None and current_tick is not None else None
            )
            episodes = [
                dict(item) for item in row.get("episodes") or () if isinstance(item, Mapping)
            ]
            opened = [int(item["opened_at"]) for item in episodes if item.get("opened_at") is not None]
            closed = [int(item["closed_at"]) for item in episodes if item.get("closed_at") is not None]
            completed = sum(str(item.get("status") or "") == "complete" for item in episodes)
            configurations.append({
                "position_key": row.get("position_key"),
                "token_id": _raw(row.get("token_id")),
                "pool_id": position_pool or None,
                "pair": _pair(row, pool),
                "protocol": position_protocol,
                "identity_match": _ownership_match(row, owner),
                "identity_basis": row.get("identity_basis"),
                "current": _is_current(row),
                "status": row.get("status"),
                "range": {
                    "tick_lower": lower, "tick_upper": upper,
                    "width_ticks": width, "current_tick": current_tick,
                    "in_range": in_range,
                    "observation_block": pool.get("valuation_block"),
                    "observation_timestamp": pool.get("valuation_timestamp"),
                },
                "fee": fee,
                "lifecycle": {
                    "episodes_observed": len(episodes),
                    "completed_episodes": completed,
                    "first_opened_at": min(opened) if opened else None,
                    "last_closed_at": max(closed) if closed else None,
                    "last_event_at": row.get("last_event_at"),
                },
                "coverage": dict(row.get("coverage") or {}),
                "inspector": "/pool?" + urlencode({"id": position_pool, "owner": owner}),
            })
            mix = protocol_mix.setdefault(position_protocol, {
                "protocol": position_protocol, "returned_positions": 0,
                "current_beneficial_positions": 0,
            })
            mix["returned_positions"] += 1
            if row in direct:
                mix["current_beneficial_positions"] += 1
            fees = fee_mix.setdefault(fee["mode"], {
                "mode": fee["mode"], "returned_positions": 0, "pools": set(),
            })
            fees["returned_positions"] += 1
            fees["pools"].add(position_pool)
            for episode in episodes:
                if episode.get("opened_at") is not None:
                    lifecycle_entries.append({
                        "kind": "opened", "timestamp": episode["opened_at"],
                        "position_key": row.get("position_key"), "pool_id": position_pool or None,
                        "pair": _pair(row, pool), "identity_match": _ownership_match(row, owner),
                    })
                if episode.get("closed_at") is not None:
                    lifecycle_entries.append({
                        "kind": "closed", "timestamp": episode["closed_at"],
                        "position_key": row.get("position_key"), "pool_id": position_pool or None,
                        "pair": _pair(row, pool), "identity_match": _ownership_match(row, owner),
                        "duration_s": episode.get("duration_s"), "status": episode.get("status"),
                    })

        latest, current_status = self._latest_activity(
            owner, window, protocol, pool_id, positions,
        )
        lifecycle_entries.sort(key=lambda row: int(row.get("timestamp") or 0), reverse=True)
        detail_coverage = dict(detail.get("coverage") or {})
        source_limit_hit = len(source_positions) >= _OWNER_POSITION_LIMIT
        identity_basis = detail.get("identity_basis") or "unobserved"
        fully_valued_count = sum(
            row["valuation_state"] == "complete" for row in allocation_positions
        )
        partially_valued_count = sum(
            row["valuation_state"] == "partial" for row in allocation_positions
        )
        unvalued_count = len(allocation_positions) - fully_valued_count - partially_valued_count
        found = bool(positions or latest)
        fee_modes = []
        for row in fee_mix.values():
            pools = row.pop("pools")
            fee_modes.append({**row, "pool_count": len(pools)})
        fee_modes.sort(key=lambda row: row["mode"])
        limitations = [
            "Observed configurations and timing do not reveal LP intent or a private strategy model.",
            "Custody matches are shown separately and are not attributed to the custody address as beneficial ownership.",
            "USDG values are known-only block-pinned principal and pending-principal observations; unknown fees and unpriced tokens are excluded.",
            "Raw liquidity is position-local and is never summed across pools.",
            "This endpoint is read-only and does not copy or execute positions.",
        ]
        if source_limit_hit:
            limitations.append(
                "The owner detail reached its base 200-position page boundary; allocation and configuration groups describe only the first 200 returned positions."
            )
        if not _window_complete(detail_coverage, window):
            limitations.append("Indexed history does not prove complete coverage of the requested window.")
        if custody:
            limitations.append("Custody-observed positions may represent multiple beneficial owners and receive no custody-level value subtotal.")

        return {
            "owner": owner,
            "window": window,
            "coverage": {
                "found": found,
                "state": detail_coverage.get("state"),
                "indexed_head": detail_coverage.get("indexed_head"),
                "history_from": detail_coverage.get("history_from"),
                "history_from_block": detail_coverage.get("history_from_block"),
                "history_to": detail_coverage.get("history_to"),
                "history_target": detail_coverage.get("history_target"),
                "window_complete": _window_complete(detail_coverage, window),
                "identity": {
                    "basis": identity_basis,
                    "beneficial_owner_observed": str(detail.get("owner") or "").lower() == owner,
                    "custody_observed": str(detail.get("custody") or "").lower() == owner,
                    "current_activity_match": (
                        latest.get("identity_match")
                        if isinstance(latest, Mapping)
                        and latest.get("qualification") == "provisional_canonical"
                        else None
                    ),
                },
                "returned_positions": len(positions),
                "source_positions_received": len(source_positions),
                "positions_omitted_by_research_limit": max(
                    0, len(source_positions) - len(positions)
                ),
                "source_position_limit": _OWNER_POSITION_LIMIT,
                "possible_position_truncation": source_limit_hit,
                "valuation": {
                    "basis": _USDG_BASIS,
                    "scope": "returned current beneficial-owner positions only",
                    "positions_with_known_subtotal": fully_valued_count + partially_valued_count,
                    "fully_valued_positions": fully_valued_count,
                    "partially_valued_positions": partially_valued_count,
                    "unvalued_positions": unvalued_count,
                    "complete_for_returned_current_positions": (
                        bool(direct) and fully_valued_count == len(direct)
                    ),
                },
                "current_activity": current_status,
            },
            "allocation": {
                "basis": "returned current beneficial-owner positions",
                "returned_position_scope": True,
                "current_beneficial_positions": len(direct),
                "custody_positions_observed": len(custody),
                "known_principal_usdg": known_total,
                "known_pending_claim_usdg": _known_sum(claim_values),
                "valued_positions": fully_valued_count,
                "partially_valued_positions": partially_valued_count,
                "unvalued_positions": unvalued_count,
                "pools": allocation_pools,
                "positions": allocation_positions,
            },
            "configurations": {
                "basis": "returned positions matching owner or custody identity",
                "returned_position_scope": True,
                "positions": configurations,
                "protocol_mix": sorted(protocol_mix.values(), key=lambda row: row["protocol"]),
                "fee_mode_mix": fee_modes,
                "range_width_ticks": {
                    "observations": len(widths),
                    "minimum": min(widths) if widths else None,
                    "median": statistics.median(widths) if widths else None,
                    "maximum": max(widths) if widths else None,
                },
            },
            "activity": {
                "latest": latest,
                "qualification": (
                    latest.get("qualification") if isinstance(latest, Mapping) else "none"
                ),
                "positions_returned": len(positions),
                "episodes_observed": sum(
                    len(row.get("episodes") or ()) for row in positions
                ),
                "recent_lifecycle": lifecycle_entries[:100],
                "recent_lifecycle_limit": 100,
            },
            "limitations": limitations,
        }
