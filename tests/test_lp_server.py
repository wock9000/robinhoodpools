"""Public HTTP boundaries and the terminal's live-stream contract."""
from contextlib import contextmanager
import http.client
import json
import threading
import time
from types import SimpleNamespace

from rhpools.lp_server import Handler, LPHTTPServer, _load_assets
from test_lp_market_service import (
    TOKEN, header, lp_effect, pools, position_state, service,
)


@contextmanager
def serving(app, **resources):
    runtime = SimpleNamespace(
        lp=app, stopping=threading.Event(), assets=_load_assets(),
        origins=frozenset({"https://rhpools.lol"}), enable_prepare=False,
        **resources,
    )
    handler = type("TestHandler", (Handler,), {"runtime": runtime})
    server = LPHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    connection = http.client.HTTPConnection(*server.server_address, timeout=3)
    try:
        yield connection
    finally:
        runtime.stopping.set()
        connection.close()
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)


def test_terminal_receives_heads_activity_and_scoped_wallets(tmp_path, monkeypatch):
    app = service(tmp_path / "market.sqlite")
    try:
        app.store.upsert_pools(pools())
        block = header(100, int(time.time()))
        event = lp_effect(
            block, "v4", "add", 1000, (1000000, 1000000),
            position_state(0), position_state(1000),
        )
        event["pool"] = pools()[1]
        monkeypatch.setattr(app.indexer, "_decode_current", lambda *a, **k: [event])
        cursor = app.indexer.feed_updates()
        app.indexer._publish_current_block(block, [], source="test")
        with serving(app) as connection:
            connection.request("GET", (
                "/api/lp/stream?view=terminal&current_only=1&kind=lp"
                "&identity_scope=wallets&owner_limit=1"
                f"&block_after={cursor['sequence']}&feed_epoch={cursor['feed_epoch']}"
            ))
            response = connection.getresponse()
            assert response.status == 200
            frames = {}
            event_name = "message"
            while not {"block", "activity", "owners"}.issubset(frames):
                raw = response.readline()
                if not raw:
                    raise AssertionError("Stream ended before required publications")
                line = raw.decode().strip()
                if line.startswith("event: "):
                    event_name = line[7:]
                elif line.startswith("data: "):
                    frames[event_name] = json.loads(line[6:])
                elif not line:
                    event_name = "message"
            assert frames["block"]["hash"] == block["hash"]
            assert frames["activity"]["rows"][0]["kind"] == "add"
            assert [row["owner"] for row in frames["owners"]["rows"]] == [TOKEN]
            response.close()
    finally:
        app.close()


def test_private_routes_stay_closed_and_upstream_errors_are_redacted(tmp_path):
    app = service(tmp_path / "market.sqlite")
    secret = "test-only-provider-credential"

    def unavailable(_query):
        raise RuntimeError("https://example.invalid/rpc?key=" + secret)

    try:
        with serving(app, public=SimpleNamespace(pools=unavailable)) as connection:
            for path in ("/api/pred.json", "/api/live.json", "/stream", "/health"):
                connection.request("GET", path)
                response = connection.getresponse()
                assert response.status == 404
                response.read()
            connection.request("GET", "/api/v1/pools?token=" + TOKEN)
            response = connection.getresponse()
            assert response.status == 503
            assert secret not in response.read().decode()
            connection.request("POST", "/api/workbench/prepare", "{}", {
                "Content-Type": "application/json",
                "Origin": "https://rhpools.lol", "Host": "rhpools.lol",
            })
            response = connection.getresponse()
            assert response.status == 403
            response.read()
    finally:
        app.close()


def test_cold_workbench_route_waits_for_bounded_index_publication(tmp_path):
    app = service(tmp_path / "market.sqlite")
    pool_id = "0x" + "ab" * 32

    class ColdMarket:
        def __init__(self):
            self.condition = threading.Condition()
            self.waiting = threading.Event()
            self.pool = None

        def _pool_by_id(self, candidate):
            with self.condition:
                return self.pool if str(candidate).lower() == pool_id else None

        def wait_pool(self, candidate, timeout):
            deadline = time.monotonic() + timeout
            with self.condition:
                self.waiting.set()
                while self._pool_by_id(candidate) is None:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        return None
                    self.condition.wait(remaining)
                return self.pool

        def detail(self, candidate, _owner=None):
            pool = self._pool_by_id(candidate)
            if pool is None:
                raise ValueError("unknown pool")
            return {"pool": pool, "revision": 1, "epoch": 0}

    market = ColdMarket()

    class ColdIndexer:
        def __init__(self):
            self.calls = []
            self.thread = None

        def request_pool_resolution(self, candidate, transaction_hash=""):
            self.calls.append((candidate, transaction_hash))
            if self.thread is not None:
                return

            def publish():
                assert market.waiting.wait(1)
                with market.condition:
                    market.pool = {"id": pool_id, "pair": "ASSET/USDG"}
                    market.condition.notify_all()

            self.thread = threading.Thread(target=publish)
            self.thread.start()

    indexer = ColdIndexer()
    lp = SimpleNamespace(indexer=indexer, store=app.store)
    try:
        with serving(lp, market=market) as connection:
            connection.request(
                "GET", f"/api/workbench/pool?id={pool_id}&tx=0xfeed",
            )
            response = connection.getresponse()
            assert response.status == 200
            assert json.loads(response.read())["pool"]["id"] == pool_id
        indexer.thread.join(1)
        assert indexer.calls == [(pool_id, "0xfeed")]
    finally:
        app.close()
