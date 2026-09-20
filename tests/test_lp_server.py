"""Public HTTP boundaries and the terminal's live-stream contract."""
from contextlib import contextmanager
import http.client
import json
import threading
import time
from types import SimpleNamespace

from rhpools.lp_server import Handler, LPHTTPServer, _load_assets, lane_slots
from test_lp_market_service import (
    TOKEN, V3, header, lp_effect, pools, position_state, service, swap,
)


@contextmanager
def serving(app, slots=None, **resources):
    runtime = SimpleNamespace(
        lp=app, stopping=threading.Event(), assets=_load_assets(),
        origins=frozenset({"https://rhpools.lol"}), enable_prepare=False,
        **resources,
    )
    attrs = {"runtime": runtime}
    if slots is not None:
        attrs["api_slots"] = lane_slots(slots)
    handler = type("TestHandler", (Handler,), attrs)
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


def test_reconnected_stream_drops_the_previous_feed_cursor(tmp_path, monkeypatch):
    app = service(tmp_path / "market.sqlite")
    waiting = threading.Event()
    release = threading.Event()
    original_wait = app.wait_stream

    def wait_for_publication(after, timeout):
        waiting.set()
        assert release.wait(3)
        return original_wait(after, timeout)

    def read_frame(response):
        name, payload = "message", None
        while True:
            raw = response.readline()
            assert raw, "stream closed before the next publication"
            line = raw.decode().strip()
            if line.startswith("event: "):
                name = line[7:]
            elif line.startswith("data: "):
                payload = json.loads(line[6:])
            elif not line and payload is not None:
                return name, payload

    try:
        app.store.upsert_pools(pools())
        now = int(time.time())
        block = header(100, now)
        app.indexer._publish_current_block(block, [], source="test")
        monkeypatch.setattr(app, "wait_stream", wait_for_publication)
        with serving(app) as connection:
            connection.request(
                "GET", "/api/lp/stream?current_only=1&kind=all&owners=0",
                headers={"Last-Event-ID": "0:previous-process:1000000"},
            )
            response = connection.getresponse()
            assert response.status == 200
            assert read_frame(response)[1]["number"] == 100
            assert waiting.wait(2)
            event = swap(block, V3, "v3")
            app.indexer._emit_current_activity(block, [event], source="test")
            app.indexer._publish_current_block(
                header(101, now + 1), [], source="test",
            )
            release.set()
            frames = dict(read_frame(response) for _ in range(2))
            assert frames["activity"]["rows"][0]["tx_hash"] == event["tx_hash"]
            assert frames["block"]["number"] == 101
            response.close()
    finally:
        release.set()
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


CACHE_MATRIX = {
    "/api/lp/status": "public, max-age=1, stale-while-revalidate=2",
    "/api/lp/overview?window=24h": "public, max-age=3, stale-while-revalidate=30",
    "/api/lp/overview?window=7d": "public, max-age=30, stale-while-revalidate=120",
    "/api/lp/pools?window=30d": "public, max-age=120, stale-while-revalidate=600",
    "/api/lp/pools": "public, max-age=3, stale-while-revalidate=30",
    "/api/lp/tape?window=24h&kind=lp": "public, max-age=2, stale-while-revalidate=10",
    "/api/lp/dislocations?min_bps=25": "public, max-age=2, stale-while-revalidate=10",
    "/api/lp/owners?window=24h": "public, max-age=5, stale-while-revalidate=60",
    "/api/lp/search?q=asset": "public, max-age=5, stale-while-revalidate=30",
}


def test_public_routes_publish_edge_cache_headers(tmp_path):
    app = service(tmp_path / "market.sqlite")
    try:
        app.store.upsert_pools(pools())
        block = header(100, int(time.time()) - 30)
        app.store.ingest([block], [swap(block, V3, "v3")])
        with serving(app) as connection:
            for target, cache_control in CACHE_MATRIX.items():
                connection.request("GET", target)
                response = connection.getresponse()
                response.read()
                assert response.status == 200, target
                assert response.getheader("Cache-Control") == cache_control, target
                assert response.getheader("ETag", "").startswith('W/"'), target
                assert response.getheader("Vary") == "Accept-Encoding", target

            connection.request("GET", "/api/lp/overview?window=24h")
            first = connection.getresponse()
            first.read()
            connection.request("GET", "/api/lp/overview?window=24h", headers={
                "If-None-Match": 'W/"stale", ' + first.getheader("ETag"),
            })
            revalidated = connection.getresponse()
            assert revalidated.status == 304
            assert revalidated.read() == b""
            assert revalidated.getheader("ETag") == first.getheader("ETag")
            assert revalidated.getheader("Cache-Control") == CACHE_MATRIX["/api/lp/overview?window=24h"]

            connection.request("GET", "/api/lp/overview?window=never")
            rejected = connection.getresponse()
            rejected.read()
            assert rejected.status == 400
            assert rejected.getheader("Cache-Control") == "no-store"
            assert rejected.getheader("ETag") is None

            connection.request("GET", "/api/lp/stream?view=terminal&current_only=1")
            stream = connection.getresponse()
            assert stream.status == 200
            assert stream.getheader("Cache-Control") == "no-store,no-transform"
            stream.close()
    finally:
        app.close()


def test_slow_lane_exhaustion_leaves_fast_routes_answering(tmp_path):
    app = service(tmp_path / "market.sqlite")
    release = threading.Event()
    entered = threading.Event()

    def blocked_owners(_query):
        entered.set()
        release.wait(5)
        return {"rows": [], "revision": 1, "epoch": 0}

    try:
        app.owners = blocked_owners
        with serving(app, slots=2) as connection:
            holder = http.client.HTTPConnection(connection.host, connection.port, timeout=10)
            holder.request("GET", "/api/lp/owners?window=24h")
            assert entered.wait(3)

            connection.request("GET", "/api/lp/owners?window=7d")
            shed = connection.getresponse()
            shed.read()
            assert shed.status == 503
            assert shed.getheader("Retry-After") == "1"
            assert shed.getheader("Cache-Control") == "no-store"

            connection.request("GET", "/api/lp/status")
            status = connection.getresponse()
            status.read()
            assert status.status == 200

            release.set()
            assert holder.getresponse().status == 200
            holder.close()
    finally:
        release.set()
        app.close()
