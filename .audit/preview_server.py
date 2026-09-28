"""Local preview of this branch: branch UI, gate, tx and tags on the real chain; market data proxied from production.

    .venv/bin/python .audit/preview_server.py [--port 8297]

Never writes to the live market database: pools are read with mode=ro, and every market route is forwarded to
the running production service on 127.0.0.1:8196. Gate and tag state live under ~/.local/state/rhpools/preview.
"""
from __future__ import annotations

import argparse
import contextlib
import http.client
import os
import signal
import sqlite3
import sys
import threading
import time
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import urlencode, urlsplit

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "tests"), str(ROOT / "src")]

from eth_keys import keys  # noqa: E402

from golden.anonymous import stub_runtime  # noqa: E402
from rhpools.lp_flow_tags import FlowTagger, PostgresListener, TagStore  # noqa: E402
from rhpools.lp_gate import Gate, GatePolicy  # noqa: E402
from rhpools.lp_server import (  # noqa: E402
    _GATE_GET, _GATE_POST, _TX_GET, _TX_POST, KEYED_STREAM, TAGS_PATH, Handler, LPHTTPServer,
    _load_assets, _pool_identity_reader,
)
from rhpools.tx_core import JsonRpc, TxCore  # noqa: E402
from rhpools.tx_plan import TxPolicy  # noqa: E402
from rhpools.tx_routes import RouteBook  # noqa: E402

CHAIN_ID = 4663
RPC = "http://127.0.0.1:8547"
UPSTREAM = ("127.0.0.1", 8196)
MARKET_DB = Path.home() / ".local/share/rhpools-nocow/lp_market.sqlite"
STATE = Path.home() / ".local/state/rhpools/preview"
SECRETS = Path.home() / ".secrets/rhpools"
USDG = "0x5fc5360d0400a0fd4f2af552add042d716f1d168"
HOP_BY_HOP = {"connection", "keep-alive", "transfer-encoding", "upgrade", "proxy-connection", "te", "trailer"}
BRANCH_API = set(_GATE_GET) | set(_GATE_POST) | set(_TX_GET) | set(_TX_POST) | {KEYED_STREAM, TAGS_PATH}


@contextlib.contextmanager
def market_reader():
    connection = sqlite3.connect(f"file:{MARKET_DB}?mode=ro", uri=True, timeout=5)
    try:
        connection.execute("PRAGMA query_only=ON")
        yield connection
    finally:
        connection.close()


def upstream_events(query: dict[str, str]):
    connection = http.client.HTTPConnection(*UPSTREAM, timeout=60)
    connection.request("GET", "/api/lp/stream?" + urlencode(query), headers={"Accept": "text/event-stream"})
    response = connection.getresponse()
    if response.status != 200:
        raise ValueError(f"upstream stream returned {response.status}")
    fields: dict[str, str] = {}
    try:
        while True:
            line = response.fp.readline()
            if not line:
                return
            line = line.decode().rstrip("\r\n")
            if line.startswith(":"):
                yield None
            elif not line:
                if "data" in fields:
                    yield fields
                fields = {}
            else:
                name, _, value = line.partition(":")
                fields[name] = value[1:] if value.startswith(" ") else value
    finally:
        connection.close()


class PreviewHandler(Handler):
    def _branch_owned(self) -> bool:
        path = urlsplit(self.path).path
        return path in BRANCH_API or path in self.runtime.assets

    def _proxy(self) -> None:
        length = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(length) if length else None
        headers = {k: v for k, v in self.headers.items() if k.lower() not in HOP_BY_HOP and k.lower() != "host"}
        connection = http.client.HTTPConnection(*UPSTREAM, timeout=120)
        try:
            connection.request(self.command, self.path, body=body, headers=headers)
            response = connection.getresponse()
            self.send_response(response.status)
            for name, value in response.getheaders():
                if name.lower() not in HOP_BY_HOP:
                    self.send_header(name, value)
            self.send_header("Connection", "close")
            self.end_headers()
            self.close_connection = True
            if self.command == "HEAD":
                return
            while chunk := response.read1(65536):
                self.wfile.write(chunk)
                self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError, TimeoutError):
            pass
        finally:
            connection.close()

    def do_GET(self) -> None:
        return super().do_GET() if self._branch_owned() else self._proxy()

    def do_HEAD(self) -> None:
        return super().do_HEAD() if self._branch_owned() else self._proxy()

    def do_POST(self) -> None:
        return super().do_POST() if self._branch_owned() else self._proxy()

    def do_OPTIONS(self) -> None:
        return super().do_OPTIONS() if self._branch_owned() else self._proxy()

    def _lp_stream(self, query, sink) -> None:
        sink.open()
        last = time.monotonic()
        for fields in upstream_events(query):
            if fields is None:
                sink.heartbeat()
            else:
                sink.event(fields.get("id", ""), fields.get("event"), fields["data"].encode())
            if time.monotonic() - last > 5:
                sink.tick()
                last = time.monotonic()


def signed_policy(owner: keys.PrivateKey, threshold_raw: int) -> tuple[GatePolicy, str]:
    policy = GatePolicy.parse({
        "version": 1, "token": USDG, "decimals": 6,
        "threshold": {"trade": str(threshold_raw), "lp": str(threshold_raw), "api": str(threshold_raw), "flags": "0"},
        "grace_s": 120, "issued_at": int(time.time()),
    })
    return policy, owner.sign_msg_hash(policy.digest(CHAIN_ID)).to_hex()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=8297)
    ap.add_argument("--threshold-usdg", type=float, default=1.0)
    args = ap.parse_args()
    STATE.mkdir(parents=True, exist_ok=True)
    owner = keys.PrivateKey(bytes.fromhex((SECRETS / "owner.key").read_text().strip()[2:]))
    owner_address = owner.public_key.to_checksum_address()
    hosts = frozenset({f"localhost:{args.port}"})
    gate = Gate(STATE / "gate.sqlite", owner=owner_address, rpc_url=RPC, hosts=hosts)
    if gate.policy().version == 0:
        gate.apply_policy(*signed_policy(owner, int(args.threshold_usdg * 10**6)), via="cli")
    rpc = JsonRpc(RPC)
    runtime = stub_runtime()
    runtime.lp = SimpleNamespace(store=SimpleNamespace(reader_snapshot=market_reader, close_reader=lambda: None))
    runtime.gate = gate
    runtime.tx = TxCore(rpc, RouteBook(market_reader, rpc), TxPolicy(75, owner_address.lower()))
    runtime.tx_unavailable = None if runtime.tx.enabled else "pinned contract code changed; trading disabled"
    dsn = next((line.split("=", 1)[1].strip() for line in (SECRETS / "listener.env").read_text().splitlines()
                if line.startswith("RHP_LISTENER_DSN=")), None)
    runtime.tags = FlowTagger(JsonRpc(RPC, timeout=15), TagStore(str(STATE / "tags.sqlite")),
                              _pool_identity_reader(runtime.lp.store), PostgresListener(dsn) if dsn else None)
    runtime.assets = _load_assets()
    runtime.origins = frozenset({f"http://localhost:{args.port}"})
    handler = type("PreviewHandler", (PreviewHandler,), {"runtime": runtime})
    server = LPHTTPServer(("127.0.0.1", args.port), handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    policy = gate.policy()
    print(f"preview   http://localhost:{args.port}/  (branch UI; market data from production 127.0.0.1:8196, read-only)")
    print(f"chain     real Robinhood Chain {CHAIN_ID} via {RPC}; transactions spend real funds")
    print(f"owner     {owner_address}  (policy signer and fee recipient)")
    print(f"gate      token USDG, holder threshold {int(policy.threshold['trade']) / 10**6} USDG, grace {policy.grace_s} s")
    print(f"tx core   enabled={runtime.tx.enabled} {runtime.tx_unavailable or ''}", flush=True)
    stopping = threading.Event()
    signal.signal(signal.SIGINT, lambda *a: stopping.set())
    signal.signal(signal.SIGTERM, lambda *a: stopping.set())
    stopping.wait()
    runtime.stopping.set()
    server.shutdown()
    server.server_close()
    gate.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
