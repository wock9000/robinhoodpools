"""Public HTTP boundaries and the terminal's live-stream contract."""
from contextlib import contextmanager
import http.client
import json
import threading
import time
from types import SimpleNamespace

from rhpools.lp_server import Handler, LPHTTPServer, Startup, _load_assets
from test_lp_market_service import (
    TOKEN, V3, header, lp_effect, pools, position_state, service, swap,
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


def test_starting_server_answers_probes_until_runtime_attaches(tmp_path):
    """A store that takes hours to open must not leave the socket bound but mute."""
    startup = Startup(_load_assets())
    handler = type("StartingHandler", (Handler,), {"startup": startup, "runtime": None})
    server = LPHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    connection = http.client.HTTPConnection(*server.server_address, timeout=3)
    app = None
    try:
        startup.phase("store")
        connection.request("GET", "/")
        response = connection.getresponse()
        assert response.status == 200
        assert b"<title>Robinhood Pools / Chain 4663</title>" in response.read()

        connection.request("GET", "/api/lp/status")
        response = connection.getresponse()
        assert response.status == 503
        assert response.getheader("Retry-After")
        payload = json.loads(response.read())
        assert payload["state"] == "starting"
        assert payload["startup"]["phase"] == "store"
        assert payload["startup"]["started_at"] == startup.started_at
        assert {"cpu_ms", "read_bytes", "write_bytes"} <= payload["startup"]["activity"].keys()

        for method, path, body in (
            ("GET", "/api/lp/stream?view=terminal", None),
            ("POST", "/api/lp/allocation", "{}"),
        ):
            connection.request(method, path, body, {"Content-Type": "application/json"})
            response = connection.getresponse()
            assert response.status == 503
            assert json.loads(response.read())["state"] == "starting"
        connection.request("GET", "/health")
        response = connection.getresponse()
        assert response.status == 404
        response.read()

        app = service(tmp_path / "market.sqlite")
        handler.runtime = SimpleNamespace(
            lp=app, stopping=threading.Event(), assets=startup.assets,
            origins=frozenset(), enable_prepare=False,
        )
        connection.close()
        connection = http.client.HTTPConnection(*server.server_address, timeout=3)
        connection.request("GET", "/api/lp/status")
        response = connection.getresponse()
        assert response.status == 200
        assert json.loads(response.read())["state"] != "starting"
    finally:
        connection.close()
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)
        if app is not None:
            app.close()


def test_pools_page_republishes_symbol_completion_without_new_events(tmp_path):
    app = service(tmp_path / "market.sqlite")
    try:
        unknown = {**pools()[0], "symbol0": None, "decimals0": None}
        app.store.upsert_pools([unknown])
        block = header(100, int(time.time()) - 60)
        app.store.ingest([block], [swap(block, V3, "v3")])
        with serving(app) as connection:
            connection.request("GET", "/api/lp/pools?window=all")
            response = connection.getresponse()
            assert response.status == 200
            etag = response.getheader("ETag")
            first = json.loads(response.read())
            assert first["rows"][0]["token0"]["symbol"] is None

            # Symbol completion bumps the store without any new event; the
            # reused aggregate frame must not pin the page's publication.
            app.store.save_token_metadata(TOKEN, "ASSET", 6)
            with app._cache_lock:
                app._cache.clear()
            connection.request(
                "GET", "/api/lp/pools?window=all", headers={"If-None-Match": etag},
            )
            response = connection.getresponse()
            assert response.status == 200
            assert response.getheader("ETag") != etag
            second = json.loads(response.read())
            assert second["rows"][0]["token0"]["symbol"] == "ASSET"
            assert second["revision"] > first["revision"]
            assert second["aggregate_revision"] == first["aggregate_revision"]
            assert second["events_revision"] == first["events_revision"]
    finally:
        app.close()
