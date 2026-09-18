"""Public source projection, evidence, and fixed-upstream boundaries."""
from __future__ import annotations

import json
import threading
import time

import pytest
import requests

from rhpools import lp_fomo_flow
from rhpools.lp_fomo_flow import (
    FomoFlowContractError,
    FomoFlowService,
    FomoFlowUnavailable,
    PUBLIC_APOLLO_ORIGIN,
    PUBLIC_RHTRENCHES_ORIGIN,
)


class _Response:
    def __init__(self, payload, status=200):
        self.body = json.dumps(payload).encode()
        self.status_code = status
        self.headers = {
            "content-type": "application/json",
            "content-length": str(len(self.body)),
        }
        self.closed = False

    def iter_content(self, chunk_size):
        assert chunk_size == 65_536
        yield self.body

    def close(self):
        self.closed = True


class _Session:
    def __init__(self, payload):
        self.payload = payload
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return _Response(self.payload)

class _RoutingSession:
    def __init__(self, tape, status):
        self.tape = tape
        self.status = status
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        payload = self.status if url.endswith("/api/status") else self.tape
        return _Response(payload)


def _event(chain="solana", identity_kind="verified-fomo"):
    evm = chain != "solana"
    return {
        "canonicalActionId": f"{chain}:transaction:2:increase",
        "traderStrategyId": f"public-{chain}-strategy",
        "identity": {
            "kind": identity_kind,
            "traderId": f"public-{chain}-trader",
            "handle": "Published handle" if identity_kind == "verified-fomo" else "Unattributed wallet",
            "wallet": "0x" + "11" * 20 if evm else "5Gt3iAVDs9jVStvceqhhCkYCTYAuCQjYBNzyJsCXDhFs",
            "privateThesis": "must not cross the projection",
        },
        "sourceRef": {
            "kind": "chain-observed",
            "txHash": "0x" + "22" * 32 if evm else "2" * 88,
            "instructionOrLogIndex": 2,
            "blockHash": "0x" + "33" * 32 if evm else "3" * 44,
            "blockNumberOrSlot": "123456",
            "confirmationStatus": "confirmed",
            "poolId": "must-not-be-inferred-or-forwarded",
        },
        "asset": {
            "chain": chain,
            "address": "0x" + "44" * 20 if evm else "moonThZEkkTVoNB7v6YVCQiT56JYDZ1oN185ba3WizL",
        },
        "assetSymbol": "TEST",
        "assetName": "Published asset",
        "assetLogoUrl": "https://tracking.example/logo.png",
        "action": "increase",
        "leaderPositionBeforeUsd": "10.125",
        "leaderPositionAfterUsd": "12.625",
        "tradeSizeUsd": "2.5",
        "leaderFillPriceUsd": "0.0000125",
        "tradeQuantity": "200000",
        "occurredAt": "2026-09-06T12:00:00.000Z",
        "observedAt": "2026-09-06T12:00:01.250Z",
        "socialLabels": ["whale", "smart-money"],
    }


def _page(items):
    return {
        "items": items,
        "nextCursor": "YWJjMTIz",
        "readAt": "2026-09-06T12:00:02.000Z",
        "evidenceThrough": max((item["observedAt"] for item in items), default=None),
        "privateRepositoryField": "must not cross the projection",
    }


def test_projection_preserves_identity_and_clocks_but_drops_unpublished_enrichment():
    session = _Session(_page([
        _event("solana", "verified-fomo"),
        _event("base", "observed-wallet"),
    ]))
    service = FomoFlowService(session=session)

    result = service.flow({"chain": "base", "limit": "50", "verified": "false"})

    assert result["provenance"]["upstream_origin"] == PUBLIC_APOLLO_ORIGIN
    assert result["coverage"]["supported_chains"] == ["solana", "base", "robinhood"]
    assert result["coverage"]["full_chain_complete"] is False
    assert result["coverage"]["upstream_page_items"] == 2
    assert result["coverage"]["returned_items"] == 1
    assert result["coverage"]["items_filtered_by_chain"] == 1
    assert result["coverage"]["pool_attribution"]["state"] == "not-provided"
    item = result["items"][0]
    assert item["identity"]["kind"] == "observed-wallet"
    assert item["evidence"] == {
        "occurred_at": "2026-09-06T12:00:00.000Z",
        "occurred_at_unix": None,
        "observed_at": "2026-09-06T12:00:01.250Z",
    }
    assert item["source"]["transaction_hash"] == "0x" + "22" * 32
    assert item["economics"]["trade_size_usd"] == "2.5"
    serialized = json.dumps(result)
    assert "privateThesis" not in serialized
    assert "privateRepositoryField" not in serialized
    assert "socialLabels" not in serialized
    assert "assetLogoUrl" not in serialized
    assert "poolId" not in serialized
    assert session.calls[0][0] == f"{PUBLIC_APOLLO_ORIGIN}/v1/copy/flow"
    assert session.calls[0][1]["params"] == {"limit": "50"}


def test_public_query_cannot_select_an_origin_or_all_chain_scope():
    session = _Session(_page([]))
    service = FomoFlowService(session=session)

    with pytest.raises(ValueError, match="unsupported Fomo flow query parameter"):
        service.flow({"origin": "https://attacker.example"})
    with pytest.raises(ValueError, match="all-chain scope is not available"):
        service.flow({"chain": "all"})
    with pytest.raises(ValueError, match="provided once"):
        service.flow({"verified": ["true", "false"]})
    assert session.calls == []

    with pytest.raises(ValueError, match="public DNS hostname"):
        FomoFlowService(origin="https://127.0.0.1")
    with pytest.raises(ValueError, match="local or private hostname"):
        FomoFlowService(origin="https://flow.internal")
    with pytest.raises(ValueError, match="without credentials"):
        FomoFlowService(origin="https://user:pass@example.com")


def test_contract_drift_never_upgrades_identity_or_widens_chain_coverage():
    wrong_chain = _event("solana")
    wrong_chain["asset"]["chain"] = "ethereum"
    service = FomoFlowService(session=_Session(_page([wrong_chain])))
    with pytest.raises(FomoFlowContractError, match="outside the supported set"):
        service.flow({})

    observed = _event("solana", "observed-wallet")
    verified_service = FomoFlowService(session=_Session(_page([observed])))
    with pytest.raises(FomoFlowContractError, match="non-verified identity"):
        verified_service.flow({"verified": "true"})

    mismatched_page = _page([_event()])
    mismatched_page["evidenceThrough"] = "2026-09-06T11:59:00.000Z"
    evidence_service = FomoFlowService(session=_Session(mismatched_page))
    with pytest.raises(FomoFlowContractError, match="evidenceThrough"):
        evidence_service.flow({})


def _rh_row(*, record_id=91, flags=None, priced="cash_leg"):
    return {
        "id": record_id,
        "ts": 1_788_756_398,
        "tx": "0x" + "aa" * 32,
        "side": "buy",
        "usd": 490.263801,
        "amount": 330780.98621992517,
        "price": 0.0014821402118743309,
        "new_position": 1,
        "is_stock": 0,
        "block": 56_557_343,
        "priced": priced,
        "quote_token": "0x" + "bb" * 20,
        "two_sided": 0,
        "funding": "fomo",
        "handle": "published_handle",
        "display_name": "Published Display Name",
        "followers": 123_456,
        "wallet": "0x" + "cc" * 20,
        "token": "0x" + "dd" * 20,
        "symbol": "TOKEN",
        "name": "Published token",
        "mark": 1.5,
        "liquidity": 2_000_000,
        "pair_url": "https://dexscreener.com/robinhood/derived-pair",
        "mcap": 3_000_000,
        "buys24": 12,
        "sells24": 7,
        "pair_created_at": 1_788_700_000,
        "flags": [] if flags is None else flags,
        "realized_pnl": 999_999,
        "reputation": {"grade": "derived"},
        "followers_inferred": ["another-wallet"],
    }


def _rh_status():
    return {
        "ok": True,
        "chain": "robinhood",
        "chain_id": 4663,
        "chain_name": "robinhood chain",
        "wallets": 147,
        "uptime": 2039,
        "source": "websocket",
        "lag_seconds": 0.1,
        "last_block": 56_559_625,
        "indexer_age": 0.4,
        "trades": 46_826,
        "first_ts": 1_787_872_060,
        "latency": {
            "n": 139,
            "median": 1.4,
            "p90": 1.8,
            "since": 1_788_752_998,
        },
        "viewers": 157,
        "last_ts": 1_788_756_398,
        "server_ts": 1_788_756_416,
    }


def test_rhtrenches_projects_raw_robinhood_evidence_and_omits_derived_rows_and_fields():
    clean = _rh_row()
    warned = _rh_row(record_id=92, flags=["not a real buy (spoofed)"])
    estimated = _rh_row(record_id=93, priced="no_cash_leg")
    session = _RoutingSession([clean, warned, estimated], _rh_status())
    service = FomoFlowService(session=session)

    result = service.flow({
        "source": "rhtrenches", "chain": "robinhood", "limit": "3",
    })

    assert result["query"] == {
        "source": "rhtrenches",
        "chain": "robinhood",
        "chain_scope": "robinhood",
        "verified_only": False,
        "limit": 3,
    }
    assert result["sources"]["selected"] == "rhtrenches"
    assert result["provenance"]["upstream_origin"] == PUBLIC_RHTRENCHES_ORIGIN
    assert result["provenance"]["evidence_time_basis"].startswith("occurred_at")
    assert result["coverage"]["supported_chains"] == ["robinhood"]
    assert result["coverage"]["publisher_status"]["chain_id"] == 4663
    assert result["coverage"]["publisher_status"]["last_block"] == "56559625"
    assert result["coverage"]["publisher_status"]["tracked_wallets"] == 147
    assert result["coverage"]["rows_omitted"] == {
        "estimated-value": 1,
        "publisher-warning": 1,
    }
    assert result["next_cursor"] is None
    assert len(result["items"]) == 1
    item = result["items"][0]
    assert item["publisher"] == "rhtrenches"
    assert item["source_event_id"] == "91"
    assert item["canonical_action_id"] is None
    assert item["identity"] == {
        "kind": "observed-wallet",
        "trader_id": None,
        "handle": "published_handle",
        "wallet": "0x" + "cc" * 20,
    }
    assert item["source"] == {
        "kind": "chain-observed",
        "transaction_hash": "0x" + "aa" * 32,
        "instruction_or_log_index": None,
        "block_hash": None,
        "block_number_or_slot": "56557343",
        "confirmation_status": None,
    }
    assert item["evidence"]["occurred_at_unix"] == "1788756398"
    assert item["evidence"]["observed_at"] is None
    assert item["economics"]["value_basis"] == "rhtrenches-cash-leg"
    serialized = json.dumps(item)
    for excluded in (
        "display_name", "followers_inferred", "realized_pnl", "reputation",
        "pair_url", "liquidity", "mcap", "buys24", "sells24",
        "not a real buy", "derived-pair",
    ):
        assert excluded not in serialized
    calls = {url: kwargs["params"] for url, kwargs in session.calls}
    assert calls == {
        f"{PUBLIC_RHTRENCHES_ORIGIN}/api/tape": {"limit": "100", "stocks": "true"},
        f"{PUBLIC_RHTRENCHES_ORIGIN}/api/status": {},
    }


def test_rhtrenches_rejects_cross_chain_verified_and_cursor_queries_before_fetch():
    session = _RoutingSession([], _rh_status())
    service = FomoFlowService(session=session)

    with pytest.raises(ValueError, match="Robinhood Chain only"):
        service.flow({"source": "rhtrenches", "chain": "solana"})
    with pytest.raises(ValueError, match="observed-wallet identity only"):
        service.flow({"source": "rhtrenches", "verified": "true"})
    with pytest.raises(ValueError, match="does not accept cursor"):
        service.flow({"source": "rhtrenches", "cursor": "YWJj"})
    assert session.calls == []


class _Clock:
    def __init__(self):
        self.now = 1000.0
        self.slept = []

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.slept.append(seconds)


class _ScriptedSession:
    """Serves per-path scripts of payloads, `_Response`s, or exceptions; the last entry repeats."""

    def __init__(self, **scripts):
        self.scripts = {f"/{name}": list(script) for name, script in scripts.items()}
        self.calls = []
        self.gate = None

    def get(self, url, **kwargs):
        self.calls.append(url)
        if self.gate is not None and url.endswith("/v1/copy/flow"):
            self.gate.wait()
        path = next(path for path in self.scripts if url.endswith(path))
        script = self.scripts[path]
        step = script.pop(0) if len(script) > 1 else script[0]
        if isinstance(step, BaseException):
            raise step
        return step if isinstance(step, _Response) else _Response(step)


@pytest.fixture
def clock(monkeypatch):
    fake = _Clock()
    monkeypatch.setattr(lp_fomo_flow, "time", fake)
    return fake


def test_connection_errors_and_gateway_statuses_retry_once(clock):
    good = _page([_event()])
    flaky = _ScriptedSession(**{"v1/copy/flow": [requests.ConnectionError("reset"), good]})
    assert len(FomoFlowService(session=flaky).flow({})["items"]) == 1
    assert len(flaky.calls) == 2 and clock.slept == [0.5]

    gateway = _ScriptedSession(**{"v1/copy/flow": [_Response({}, 502), good]})
    assert len(FomoFlowService(session=gateway).flow({})["items"]) == 1
    assert len(gateway.calls) == 2

    twice = _ScriptedSession(**{"v1/copy/flow": [_Response({}, 503)]})
    with pytest.raises(FomoFlowUnavailable):
        FomoFlowService(session=twice).flow({})
    assert len(twice.calls) == 2

    hard = _ScriptedSession(**{"v1/copy/flow": [_Response({}, 500), good]})
    with pytest.raises(FomoFlowUnavailable):
        FomoFlowService(session=hard).flow({})
    assert len(hard.calls) == 1

    bad_cursor = _ScriptedSession(**{"v1/copy/flow": [_Response({}, 400), good]})
    with pytest.raises(ValueError):
        FomoFlowService(session=bad_cursor).flow({"cursor": "abc"})
    assert len(bad_cursor.calls) == 1


def test_expired_page_is_served_stale_when_refresh_fails(clock):
    session = _ScriptedSession(**{"v1/copy/flow": [_page([_event()]), _Response({}, 500)]})
    service = FomoFlowService(session=session)

    first = service.flow({})
    clock.now += 31
    stale = service.flow({})

    assert first["provenance"]["stale"] is False
    assert stale["provenance"]["stale"] is True
    assert "HTTP 500" in stale["provenance"]["stale_reason"]
    assert stale["provenance"]["retrieved_at"] == first["provenance"]["retrieved_at"]
    assert stale["items"] == first["items"]
    assert stale["coverage"]["publisher_circuit"] is None
    assert len(session.calls) == 2


def test_page_failing_its_contract_never_replaces_last_good(clock):
    drifted = _page([_event()])
    drifted["evidenceThrough"] = "2026-09-06T13:00:00.000Z"
    session = _ScriptedSession(**{"v1/copy/flow": [_page([_event()]), drifted]})
    service = FomoFlowService(session=session)

    first = service.flow({})
    clock.now += 31
    stale = service.flow({})

    assert stale["provenance"]["stale"] is True
    assert stale["items"] == first["items"]


def test_circuit_opens_after_three_failures_and_serves_stale_without_upstream(clock):
    session = _ScriptedSession(**{"v1/copy/flow": [_page([_event()]), _Response({}, 500)]})
    service = FomoFlowService(session=session)
    service.flow({})

    responses = []
    for _ in range(3):
        clock.now += 31
        responses.append(service.flow({}))
    assert [r["coverage"]["publisher_circuit"] for r in responses[:2]] == [None, None]
    circuit = responses[2]["coverage"]["publisher_circuit"]
    assert circuit["state"] == "open" and circuit["consecutive_failures"] == 3
    assert "HTTP 500" in circuit["last_error"]
    calls_before = len(session.calls)

    clock.now += 31
    held = service.flow({})
    assert held["provenance"]["stale"] is True
    assert held["coverage"]["publisher_circuit"]["retry_after_seconds"] <= 29
    assert len(session.calls) == calls_before

    session.scripts["/v1/copy/flow"] = [_page([_event(), _event("base")])]
    clock.now += 30
    recovered = service.flow({})
    assert recovered["provenance"]["stale"] is False
    assert recovered["coverage"]["publisher_circuit"] is None
    assert len(recovered["items"]) == 2
    assert len(session.calls) == calls_before + 1


def test_open_circuit_with_no_last_good_page_is_unavailable(clock):
    session = _ScriptedSession(**{"v1/copy/flow": [requests.ConnectionError("down")]})
    service = FomoFlowService(session=session)
    for _ in range(3):
        with pytest.raises(FomoFlowUnavailable):
            service.flow({})
    calls_before = len(session.calls)

    with pytest.raises(FomoFlowUnavailable, match="circuit open"):
        service.flow({})
    assert len(session.calls) == calls_before


def test_stalled_apollo_read_does_not_block_rhtrenches():
    session = _ScriptedSession(**{
        "v1/copy/flow": [_page([_event()])],
        "api/tape": [[_rh_row()]],
        "api/status": [_rh_status()],
    })
    session.gate = threading.Event()
    service = FomoFlowService(session=session)
    apollo = threading.Thread(target=service.flow, args=({},), daemon=True)
    apollo.start()
    for _ in range(200):
        if any(url.endswith("/v1/copy/flow") for url in session.calls):
            break
        time.sleep(0.005)

    results = []
    rhtrenches = threading.Thread(
        target=lambda: results.append(service.flow({"source": "rhtrenches"})), daemon=True,
    )
    rhtrenches.start()
    rhtrenches.join(timeout=2)
    session.gate.set()
    assert not rhtrenches.is_alive(), "rhtrenches read waited on the stalled Apollo read"
    assert len(results[0]["items"]) == 1
    apollo.join(timeout=5)
    assert not apollo.is_alive()
