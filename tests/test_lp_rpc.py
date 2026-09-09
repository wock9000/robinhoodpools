import json
import threading
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

import pytest

from rhpools import lp_rpc
from rhpools.lp_chain import CHAIN_ID


@pytest.fixture
def provider(monkeypatch):
    source = lp_rpc._Source("unit", "https://unit.test/rpc")
    monkeypatch.setattr(lp_rpc, "_source_list", lambda _url, _capability: (source,))
    monkeypatch.setattr(lp_rpc, "head_subscription_urls", lambda: ())
    factory = lp_rpc.build_rpc_factory(source.url, RuntimeError)
    yield factory
    factory.close()


def test_missing_archive_state_does_not_poison_headers_or_bypass_cooldown(provider, monkeypatch):
    clock = SimpleNamespace(now=100.0)
    monkeypatch.setattr(lp_rpc, "time", SimpleNamespace(
        monotonic=lambda: clock.now, time=lambda: 1000.0 + clock.now,
    ))
    calls = []
    archive_available = False

    def post(_client, _source, payload):
        def response(item):
            calls.append(item["method"])
            result = {"jsonrpc": "2.0", "id": item["id"]}
            if item["method"] == "eth_chainId":
                result["result"] = hex(CHAIN_ID)
            elif item["method"] == "eth_call":
                if archive_available:
                    result["result"] = "0x01"
                else:
                    result["error"] = {"code": -32000, "message": "missing trie node"}
            elif item["method"] == "eth_getBlockByNumber":
                result["result"] = {"number": item["params"][0]}
            else:
                result["result"] = "0x200"
            return result
        return [response(item) for item in payload] if isinstance(payload, list) else response(payload)

    monkeypatch.setattr(lp_rpc.RoutedRpc, "_post", post)
    archive = provider("enrichment")
    params = [{"to": "0x" + "1" * 40, "data": "0x"}, "0x10"]
    with pytest.raises(RuntimeError, match="missing trie node"):
        archive.call("eth_call", params)

    assert provider("live").call("eth_blockNumber") == "0x200"
    assert provider("history").batch([
        ("eth_getBlockByNumber", ["0x10", False]),
    ]) == [{"number": "0x10"}]
    status = provider.status()
    assert status["head"]["sources"][0]["state"] == "available"
    assert status["history_state"]["sources"][0]["state"] == "failed"

    archive_available = True
    clock.now = 100.499
    with pytest.raises(RuntimeError, match="cooldown"):
        archive.call("eth_call", params)
    assert calls.count("eth_call") == 1
    clock.now = 100.5
    assert archive.call("eth_call", params) == "0x01"
    assert provider.status()["history_state"]["sources"][0]["state"] == "available"


def test_slow_response_does_not_serialize_other_rpc_lanes(provider, monkeypatch):
    slow_started = threading.Event()
    release_slow = threading.Event()
    monkeypatch.setattr(lp_rpc, "_minimum_interval", lambda _url: 0.0)

    def post(_url, *, data, **_kwargs):
        payload = json.loads(data)
        method = payload["method"]
        if method == "eth_chainId":
            result = hex(CHAIN_ID)
        elif method == "eth_getBlockByNumber":
            slow_started.set()
            if not release_slow.wait(5):
                raise TimeoutError("slow RPC was not released")
            result = {"number": "0x10"}
        else:
            result = "0x200"
        encoded = json.dumps({"jsonrpc": "2.0", "id": payload["id"], "result": result}).encode()
        return SimpleNamespace(
            raise_for_status=lambda: None,
            raw=SimpleNamespace(read=lambda *_args, **_options: encoded),
            close=lambda: None,
        )

    monkeypatch.setattr(provider._registry, "_session", lambda _source: SimpleNamespace(post=post))
    history = provider("history")
    live = provider("live")
    with ThreadPoolExecutor(max_workers=2) as executor:
        blocked = executor.submit(history.call, "eth_getBlockByNumber", ["0x10", False])
        try:
            assert slow_started.wait(2)
            current = executor.submit(live.call, "eth_blockNumber")
            assert current.result(timeout=2) == "0x200"
            assert not blocked.done()
        finally:
            release_slow.set()
        assert blocked.result(timeout=2) == {"number": "0x10"}


def test_rpc_credentials_reject_public_files_and_bad_urls_without_leaking(tmp_path, monkeypatch):
    credential = tmp_path / "provider.url"
    credential.write_text("https://rpc.example.test/?key=example-only\n")
    credential.chmod(0o644)
    monkeypatch.setenv("LP_RPC_LOG_URL_FILES", str(credential))
    monkeypatch.delenv("LP_RPC_LOG_URLS", raising=False)
    monkeypatch.setenv("LP_RPC_DISABLE_ALCHEMY", "1")
    with pytest.raises(ValueError, match="LP_RPC_LOG_URL_FILES"):
        lp_rpc.build_rpc_factory("", RuntimeError)
    credential.chmod(0o600)
    credential.write_text("file:///private/example-only\n")
    with pytest.raises(ValueError) as failure:
        lp_rpc.build_rpc_factory("", RuntimeError)
    assert "example-only" not in str(failure.value)


def test_rpc_demand_counts_batch_items_and_expires_window(monkeypatch):
    clock = SimpleNamespace(now=100.0)
    monkeypatch.setattr(lp_rpc.time, "monotonic", lambda: clock.now)
    gate = lp_rpc._SourceGate(started_at=100.0)
    gate.record(4)
    gate.record(1)
    clock.now = 102.0
    traffic = gate.traffic()
    assert traffic["http_requests"] == 2
    assert traffic["rpc_calls"] == 5
    assert traffic["http_rps"] == 1
    assert traffic["rpc_calls_per_second"] == 2.5
    clock.now = 160.0
    assert gate.traffic()["rpc_calls_per_second"] == 0
    gate.record(2)
    assert gate.traffic()["rpc_calls"] == 7
    assert gate.traffic()["rpc_calls_per_second"] == round(2 / 60, 3)


def test_rpc_transport_traceback_does_not_expose_url_credentials(monkeypatch):
    import traceback

    credential = "test-credential-never-log"
    source = lp_rpc._Source("private-provider", f"https://rpc.example.test/?key={credential}")
    monkeypatch.setattr(lp_rpc, "_source_list", lambda *_args: (source,))
    monkeypatch.setattr(lp_rpc, "head_subscription_urls", lambda: ())
    factory = lp_rpc.build_rpc_factory("", RuntimeError)

    def failed_request(*_args, **_kwargs):
        raise lp_rpc.requests.ConnectionError(f"connection failed for {source.url}")

    monkeypatch.setattr(lp_rpc.requests.Session, "post", failed_request)
    try:
        with pytest.raises(RuntimeError) as failure:
            factory("live").call("eth_blockNumber")
        assert credential not in "".join(traceback.format_exception(failure.value))
        assert credential not in json.dumps(factory.status())
    finally:
        factory.close()


def test_execution_revert_does_not_disable_state_provider(provider, monkeypatch):
    def post(_client, _source, payload):
        response = {"jsonrpc": "2.0", "id": payload["id"]}
        if payload["method"] == "eth_chainId":
            response["result"] = hex(CHAIN_ID)
        elif payload["params"][0]["data"] == "0xbad0":
            response["error"] = {"code": 3, "message": "execution reverted"}
        else:
            response["result"] = "0x01"
        return response

    monkeypatch.setattr(lp_rpc.RoutedRpc, "_post", post)
    client = provider("state")
    with pytest.raises(RuntimeError, match="execution reverted"):
        client.call("eth_call", [{"to": "0x" + "1" * 40, "data": "0xbad0"}, "latest"])
    assert client.call(
        "eth_call", [{"to": "0x" + "1" * 40, "data": "0x600d"}, "latest"],
    ) == "0x01"
    assert provider.status()["state"]["sources"][0]["state"] == "available"


def test_execution_revert_in_batch_is_not_replaced_by_another_provider(monkeypatch):
    sources = (
        lp_rpc._Source("primary", "https://primary.test/rpc"),
        lp_rpc._Source("fallback", "https://fallback.test/rpc"),
    )
    monkeypatch.setattr(lp_rpc, "_source_list", lambda *_args: sources)
    monkeypatch.setattr(lp_rpc, "head_subscription_urls", lambda: ())

    def post(_client, source, payload):
        def response(item):
            result = {"jsonrpc": "2.0", "id": item["id"]}
            if item["method"] == "eth_chainId":
                result["result"] = hex(CHAIN_ID)
            elif source.name == "primary" and item["params"][0]["data"] == "0xbad0":
                result["error"] = {"code": -32000, "message": "execution reverted: unknown selector"}
            else:
                result["result"] = "0x01" if source.name == "primary" else "0x99"
            return result
        return [response(item) for item in payload] if isinstance(payload, list) else response(payload)

    monkeypatch.setattr(lp_rpc.RoutedRpc, "_post", post)
    factory = lp_rpc.build_rpc_factory("", RuntimeError)
    try:
        client = factory("state")
        good = ("eth_call", [{"to": "0x" + "1" * 40, "data": "0x600d"}, "latest"])
        bad = ("eth_call", [{"to": "0x" + "1" * 40, "data": "0xbad0"}, "latest"])
        with pytest.raises(RuntimeError, match="execution reverted"):
            client.batch([good, bad])
        assert client.batch([good]) == ["0x01"]
    finally:
        factory.close()


@pytest.mark.parametrize("lifecycle", ["mint", "burn"])
def test_nfpm_lifecycle_absence_survives_routed_batch_fallback(
        provider, monkeypatch, tmp_path, lifecycle):
    from eth_abi import encode
    from rhpools.lp_market_index import MarketIndexer, RpcError
    from rhpools.lp_market_protocols import (
        ProtocolDecodeError, UNISWAP_V3_POSITION_MANAGER,
        decode_position_state_results, position_state_requests,
    )
    from rhpools.lp_market_store import MarketStore

    monkeypatch.setattr(provider._registry, "error_type", RpcError)
    zero, owner = "0x" + "0" * 40, "0x" + "1" * 40
    missing_pin = "0x9" if lifecycle == "mint" else "0xa"
    liquidity = 100 if lifecycle == "mint" else 0
    revert_data = "0x08c379a0" + encode(["string"], ["Invalid token ID"]).hex()
    position_data = "0x" + encode(
        ["uint96", "address", "address", "address", "uint24", "int24",
         "int24", "uint128", "uint256", "uint256", "uint128", "uint128"],
        [0, zero, owner, "0x" + "2" * 40, 500, -60, 60, liquidity, 0, 0, 0, 0],
    ).hex()

    def post(_client, _source, payload):
        def response(item):
            result = {"jsonrpc": "2.0", "id": item["id"]}
            if item["method"] == "eth_chainId":
                result["result"] = hex(CHAIN_ID)
            elif item["params"][1] == missing_pin:
                result["error"] = {
                    "code": 3,
                    "message": "RuntimeError: execution reverted: Invalid token ID",
                    "data": revert_data,
                }
            else:
                result["result"] = position_data
            return result
        return [response(item) for item in payload] if isinstance(payload, list) else response(payload)

    monkeypatch.setattr(lp_rpc.RoutedRpc, "_post", post)
    event = {
        "protocol": "nft", "kind": "transfer", "block_number": 10,
        "block_hash": "0x" + "a" * 64, "tx_hash": "0x" + "b" * 64,
        "tx_index": 0, "log_index": 1, "token_id": "42",
        "custody": UNISWAP_V3_POSITION_MANAGER,
        "data": {
            "manager_protocol": "v3", "mint": lifecycle == "mint",
            "burn": lifecycle == "burn",
            "prior_owner": zero if lifecycle == "mint" else owner,
            "new_owner": owner if lifecycle == "mint" else zero,
        },
    }
    with MarketStore(tmp_path / "market.sqlite") as store:
        scanner = MarketIndexer(store, SimpleNamespace(), "", rpc=provider)
        try:
            requests = position_state_requests([event])
            results = scanner._rpc_state_batch(scanner._state_rpc_calls(requests))
            update = decode_position_state_results(requests, results)[0]["data"]
            missing = "position_before" if lifecycle == "mint" else "position_after"
            present = "position_after" if lifecycle == "mint" else "position_before"
            assert update[missing]["exists"] is False
            assert update[missing]["claims_empty"] is True
            assert update[present]["exists"] is True
            assert update[present]["liquidity"] == str(liquidity)

            event["data"].update(mint=False, burn=False)
            with pytest.raises(ProtocolDecodeError, match="pinned eth_call failed"):
                decode_position_state_results(position_state_requests([event]), results)
        finally:
            scanner.close()
