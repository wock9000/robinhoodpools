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


def test_header_batch_uses_one_bounded_burst_and_restores_response_order(
    monkeypatch,
):
    source = lp_rpc._Source("remote", "https://remote.test/rpc")
    monkeypatch.setattr(lp_rpc, "_source_list", lambda *_args: (source,))
    monkeypatch.setattr(lp_rpc, "head_subscription_urls", lambda: ())
    factory = lp_rpc.build_rpc_factory(source.url, RuntimeError)
    client = factory("live")
    batches = []

    def post(_source, payload):
        batches.append(len(payload))
        return [
            {
                "jsonrpc": "2.0",
                "id": item["id"],
                "result": {"number": item["params"][0]},
            }
            for item in reversed(payload)
        ]

    monkeypatch.setattr(client, "_ensure_chain", lambda _source: None)
    monkeypatch.setattr(client, "_post", post)
    calls = [
        ("eth_getBlockByNumber", [hex(number), False])
        for number in range(56)
    ]
    try:
        results = client.batch(calls)
        assert batches == [56]
        assert [result["number"] for result in results] == [
            hex(number) for number in range(56)
        ]
    finally:
        factory.close()


def test_explicit_headers_prefer_local_and_retry_only_missing_items(
    monkeypatch,
):
    remote = lp_rpc._Source("remote", "https://remote.test/rpc")
    local = lp_rpc._Source("local", "http://127.0.0.1:8545")
    monkeypatch.setattr(lp_rpc, "_source_list", lambda *_args: (remote, local))
    monkeypatch.setattr(lp_rpc, "head_subscription_urls", lambda: ())
    factory = lp_rpc.build_rpc_factory(remote.url, RuntimeError)
    client = factory("live")
    attempts = []

    def post(source, payload):
        if isinstance(payload, dict):
            attempts.append((source.name, (payload["method"],)))
            return {
                "jsonrpc": "2.0",
                "id": payload["id"],
                "result": "0x100",
            }
        tags = tuple(item["params"][0] for item in payload)
        attempts.append((source.name, tags))
        responses = []
        for item in reversed(payload):
            tag = item["params"][0]
            result = None if source is local and tag == "0xb" else {"number": tag}
            responses.append({
                "jsonrpc": "2.0",
                "id": item["id"],
                "result": result,
            })
        return responses

    monkeypatch.setattr(client, "_ensure_chain", lambda _source: None)
    monkeypatch.setattr(client, "_post", post)
    try:
        assert client.call("eth_blockNumber") == "0x100"
        results = client.batch([
            ("eth_getBlockByNumber", ["0xc", False]),
            ("eth_getBlockByNumber", ["0xb", False]),
            ("eth_getBlockByNumber", ["0xd", False]),
        ])
        assert [result["number"] for result in results] == ["0xc", "0xb", "0xd"]
        assert attempts == [
            ("remote", ("eth_blockNumber",)),
            ("local", ("0xc", "0xb", "0xd")),
            ("remote", ("0xb",)),
        ]
    finally:
        factory.close()


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


def test_wss_credentials_reject_exposed_files_and_wrong_transport(tmp_path, monkeypatch):
    credential = tmp_path / "provider.url"
    credential.write_text("wss://rpc.example.test/private-token/\n")
    credential.chmod(0o644)
    monkeypatch.setenv("LP_RPC_HEAD_WSS_URL_FILES", str(credential))
    with pytest.raises(ValueError) as failure:
        lp_rpc.head_subscription_urls()
    assert "private-token" not in str(failure.value)
    credential.chmod(0o600)
    credential.write_text("https://rpc.example.test/private-token/\n")
    with pytest.raises(ValueError) as failure:
        lp_rpc.head_subscription_urls()
    assert "private-token" not in str(failure.value)

    credential.write_text("wss://rpc.example.test/private-token/#note\n")
    with pytest.raises(ValueError) as failure:
        lp_rpc.head_subscription_urls()
    assert "private-token" not in str(failure.value)


def test_wss_credentials_preserve_source_precedence_and_local_safety(tmp_path, monkeypatch):
    credential = tmp_path / "provider.url"
    credential.write_text("wss://sponsored.example.test/token/\nws://127.0.0.1:8549\n")
    credential.chmod(0o600)
    monkeypatch.setenv("LP_RPC_HEAD_WSS_URL_FILES", str(credential))
    monkeypatch.setenv("LP_RPC_HEAD_WSS_URLS", "wss://explicit.example.test")
    monkeypatch.setenv("RHP_RPC_WSS", "wss://sponsored.example.test/token/")
    monkeypatch.setenv("LP_RPC_DISABLE_LOCAL_FALLBACK", "1")
    monkeypatch.setenv("LP_RPC_DISABLE_ALCHEMY", "1")
    assert lp_rpc.head_subscription_urls() == (
        "wss://explicit.example.test",
        "wss://sponsored.example.test/token/",
        "wss://robinhood-rpc.publicnode.com",
    )


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


def test_large_batch_respects_provider_item_limit(provider, monkeypatch):
    clock = SimpleNamespace(now=100.0)
    gate = lp_rpc._SourceGate(started_at=clock.now)
    arrivals = []

    def sleep(delay):
        clock.now += delay

    monkeypatch.setattr(lp_rpc, "time", SimpleNamespace(
        monotonic=lambda: clock.now, time=lambda: clock.now, sleep=sleep,
    ))
    monkeypatch.setattr(lp_rpc, "_gate", lambda _url: gate)

    def post(_url, *, data, **_kwargs):
        payload = json.loads(data)
        items = payload if isinstance(payload, list) else [payload]
        arrivals[:] = [at for at in arrivals if at > clock.now - 1]
        limited = len(arrivals) + len(items) > 50
        arrivals.extend([clock.now] * len(items))
        responses = []
        for item in reversed(items):
            result = {"jsonrpc": "2.0", "id": item["id"]}
            if limited:
                result["error"] = {
                    "code": -32007, "message": "50/second request limit reached",
                }
            else:
                result["result"] = (
                    hex(CHAIN_ID) if item["method"] == "eth_chainId"
                    else {"number": item["params"][0]}
                )
            responses.append(result)
        encoded = json.dumps(
            responses if isinstance(payload, list) else responses[0],
        ).encode()
        return SimpleNamespace(
            raise_for_status=lambda: None,
            raw=SimpleNamespace(read=lambda *_args, **_kwargs: encoded),
            close=lambda: None,
        )

    session = SimpleNamespace(post=post)
    monkeypatch.setattr(provider._registry, "_session", lambda _source: session)
    calls = [("eth_getBlockByNumber", [hex(index), False]) for index in range(100)]
    assert provider("live").batch(calls) == [
        {"number": hex(index)} for index in range(100)
    ]


@pytest.mark.parametrize("failure_kind", ["transport", "item", "exhausted"])
def test_chunk_failure_keeps_completed_results(monkeypatch, failure_kind):
    sources = (
        lp_rpc._Source("primary", "https://chunk-primary.test/rpc"),
        lp_rpc._Source("fallback", "https://chunk-fallback.test/rpc"),
    )
    monkeypatch.setattr(lp_rpc, "_source_list", lambda *_args: sources)
    monkeypatch.setattr(lp_rpc, "head_subscription_urls", lambda: ())

    def post(_client, source, payload):
        if not isinstance(payload, list):
            return {"jsonrpc": "2.0", "id": payload["id"], "result": hex(CHAIN_ID)}
        fails = source.name == "primary" and any(
            item["params"][0]["data"] == hex(12) for item in payload
        )
        if (
            fails and failure_kind != "item"
            or source.name == "fallback" and failure_kind == "exhausted"
        ):
            raise RuntimeError("connection reset")
        responses = []
        for item in reversed(payload):
            result = {"jsonrpc": "2.0", "id": item["id"]}
            if fails and item["params"][0]["data"] == hex(12):
                result["error"] = {"code": -32000, "message": "missing trie node"}
            else:
                result["result"] = "0x01" if source.name == "primary" else "0x99"
            responses.append(result)
        return responses

    monkeypatch.setattr(lp_rpc.RoutedRpc, "_post", post)
    factory = lp_rpc.build_rpc_factory("", RuntimeError)
    calls = [
        ("eth_call", [{"to": "0x" + "1" * 40, "data": hex(index)}, "latest"])
        for index in range(25)
    ]
    fallback_indexes = (
        set(range(10, 25)) if failure_kind != "item" else {12, *range(20, 25)}
    )
    try:
        if failure_kind == "exhausted":
            results = factory("state").batch_results(calls)
            assert len(results) == 25
            assert results[:10] == ["0x01"] * 10
            assert all(isinstance(result, RuntimeError) for result in results[10:])
            return
        assert factory("state").batch(calls) == [
            "0x99" if index in fallback_indexes else "0x01"
            for index in range(25)
        ]
    finally:
        factory.close()


@pytest.mark.parametrize("endpoint", [
    "https://rpc.example.test/?key={credential}",
    "https://rpc.example.test/{credential}/",
])
def test_rpc_transport_traceback_does_not_expose_url_credentials(monkeypatch, endpoint):
    import traceback

    credential = "test-credential-never-log"
    source = lp_rpc._Source("private-provider", endpoint.format(credential=credential))
    monkeypatch.setattr(lp_rpc, "_source_list", lambda *_args: (source,))
    monkeypatch.setattr(lp_rpc, "head_subscription_urls", lambda: ())
    factory = lp_rpc.build_rpc_factory("", RuntimeError)

    def failed_request(*_args, **_kwargs):
        raise lp_rpc.requests.ConnectionError(f"connection failed for {source.url}; token={credential}")

    monkeypatch.setattr(lp_rpc.requests.Session, "post", failed_request)
    try:
        with pytest.raises(RuntimeError) as failure:
            factory("live").call("eth_blockNumber")
        assert credential not in "".join(traceback.format_exception(failure.value))
        assert credential not in json.dumps(factory.status())
    finally:
        factory.close()
