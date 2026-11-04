import sqlite3
import threading
from contextlib import contextmanager
from types import SimpleNamespace

from golden.anonymous import stub_runtime
from rhpools.lp_flow_tags import FlowTag
from rhpools.lp_gate import Gate, Limits
from rhpools.lp_server import Handler, LPHTTPServer
from test_gate import HOLDER, OWNER, Clock, FakeRpc, signed_policy
from test_gate_server import ORIGIN, browser_sign_in, call

TX = "0x" + "ab" * 32
POOL = "0x" + "cd" * 32


class FakeTagger:
    def __init__(self):
        self.rows = []

    def tag(self, rows):
        self.rows.extend(rows)
        return [FlowTag(row["tx_hash"], row["pool_id"], frozenset({"PONS", "FOMO"}), frozenset({"pons_hook", "fomo_fill"}), None) for row in rows]


def market():
    connection = sqlite3.connect(":memory:", check_same_thread=False)
    connection.executescript(
        "CREATE TABLE events(tx_hash TEXT, log_index INTEGER, pool_id TEXT, block_number INTEGER, timestamp INTEGER);"
        "CREATE INDEX events_tx_log_idx ON events(tx_hash, log_index);"
    )
    connection.executemany("INSERT INTO events VALUES (?,?,?,?,?)", [(TX, 0, POOL, 100, 1_700_000_000), (TX, 1, POOL, 100, 1_700_000_000)])

    @contextmanager
    def reader_snapshot():
        yield connection

    return SimpleNamespace(store=SimpleNamespace(reader_snapshot=reader_snapshot, close_reader=lambda: None))


@contextmanager
def serving(tmp_path, tagger):
    clock, rpc = Clock(), FakeRpc()
    gate = Gate(tmp_path / "gate.sqlite", owner=OWNER, rpc_url="http://127.0.0.1:1", hosts=frozenset({"rhpools.lol"}), clock=clock, rpc=rpc, limits=Limits(key_rps=50, key_burst=50))
    gate.clock, gate.rpc = clock, rpc
    runtime = stub_runtime()
    runtime.gate, runtime.tags, runtime.lp = gate, tagger, market()
    handler = type("TagsHandler", (Handler,), {"runtime": runtime, "log_message": lambda *a: None})
    server = LPHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server.server_address, gate
    finally:
        runtime.stopping.set()
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)
        gate.close()


def test_tags_are_holder_only_and_rows_come_from_the_market(tmp_path):
    tagger = FakeTagger()
    with serving(tmp_path, tagger) as (address, gate):
        assert call(address, "GET", f"/api/v1/tags?tx={TX}", headers=ORIGIN)[0] == 401
        gate.apply_policy(*signed_policy(grace_s=60, threshold={"trade": "1", "lp": "1", "api": "1", "flags": "500"}), via="cli")
        gate.rpc.balances[HOLDER.lower()] = 100
        gate.clock.now += 31
        secret, _cookie, _body = browser_sign_in(address, gate)
        headers = {**ORIGIN, "Cookie": f"__Host-rhp_session={secret}"}
        status, _, body = call(address, "GET", f"/api/v1/tags?tx={TX}", headers=headers)
        assert status == 403 and body["gate"]["feature"] == "flags"
        gate.rpc.balances[HOLDER.lower()] = 600
        gate.clock.now += 31
        status, response_headers, body = call(address, "GET", f"/api/v1/tags?tx={TX.upper().replace('0X', '0x')},{'0x' + 'ee' * 32}", headers=headers)
        assert status == 200 and response_headers["Cache-Control"] == "private, no-store"
        assert body["tags"][TX][POOL]["tags"] == ["FOMO", "PONS"]
        assert body["tags"]["0x" + "ee" * 32] == {}
        assert tagger.rows == [{"tx_hash": TX, "pool_id": POOL, "block_number": 100, "timestamp": 1_700_000_000}]
        assert call(address, "GET", "/api/v1/tags?tx=0x12", headers=headers)[0] == 400
