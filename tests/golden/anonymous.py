"""Anonymous HTTP surface recorder.

Every existing route is served from deterministic stub resources and the raw
response (status, headers, body, first stream frames) is captured. Recorded
once from snapshot 1b00978; replayed by tests/test_gate_golden.py against the
current tree. Run from a checkout whose venv installs that checkout's rhpools:

    .venv/bin/python /path/to/tests/golden/anonymous.py --out anonymous.json
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import http.client
import json
import sys
import threading
from pathlib import Path
from types import SimpleNamespace

from rhpools.lp_server import _ASSETS, _ROUTES, Handler, LPHTTPServer, _load_assets

POOL_ID = "0x" + "ab" * 32
FILLER = {"pad": ["x" * 40] * 40}


def _payload(name: str, query: dict | None = None, **extra) -> dict:
    return {
        "route": name, "query": dict(sorted((query or {}).items())),
        "revision": 7, "epoch": 3, "status": {"revision": 7, "epoch": 3},
        **FILLER, **extra,
    }


class _Store:
    def close_reader(self) -> None:
        pass


class _Indexer:
    def request_pool_resolution(self, pool_id, tx) -> None:
        pass


class _Lp:
    store = _Store()
    indexer = _Indexer()

    def status(self):
        return {"chain_id": 4663, "state": "ready", "revision": 7, "epoch": 3, **FILLER}

    def __getattr__(self, name):
        if name in {"search", "overview", "pools", "tape", "dislocations", "owners", "closed", "owner"}:
            return lambda query: _payload(name, query)
        raise AttributeError(name)

    def stream_updates(self, query, block_after, feed_epoch):
        events = [] if block_after >= 2 else [
            {"sequence": 1, "event": "block", "data": {"number": 100, "hash": "0x" + "01" * 32}},
            {"sequence": 2, "event": "activity", "data": {"rows": [{"id": "r1", "kind": "add"}]}},
        ]
        return {"reset": False, "feed_epoch": "fe1", "sequence": 2, "events": events}

    def poll_frame(self, query, after, epoch):
        if after >= 7:
            return None
        return {"revision": 7, "epoch": 3, "reset": False, "rows": [{"id": "r1", "kind": "add"}]}

    def poll_owners(self, query, revision):
        if revision is not None:
            return None
        return {"revision": 7, "current_activity": {"epoch": 3}, "rows": [{"owner": "0x" + "12" * 20}]}

    def wait_stream(self, block_after, timeout) -> None:
        threading.Event().wait(min(timeout, 0.05))


class _Market:
    def catalog(self, query):
        return _payload("catalog", query)

    def wait_pool(self, pool_id, timeout):
        return {"id": pool_id} if pool_id == POOL_ID else None

    def _pool_by_id(self, pool_id):
        return None

    def detail(self, pool_id, owner=None):
        if pool_id != POOL_ID:
            raise ValueError("unknown pool")
        return {"pool": {"id": pool_id}, "revision": 7, "epoch": 3, **FILLER}

    def wait_detail(self, pool_id, owner, revision, timeout):
        return None if revision == 7 else self.detail(pool_id, owner)


def stub_runtime() -> SimpleNamespace:
    named = lambda name: SimpleNamespace(**{name: lambda query: _payload(name, query)})
    return SimpleNamespace(
        lp=_Lp(), market=_Market(), stopping=threading.Event(), assets=_load_assets(),
        origins=frozenset({"https://rhpools.lol"}), enable_prepare=False,
        public=SimpleNamespace(pools=lambda q: _payload("public.pools", q), assets=lambda q: _payload("public.assets", q)),
        research=named("owner"), flow=named("flow"),
    )


def requests() -> list[dict]:
    plan: list[dict] = []

    def add(method, path, headers=None, body=None, frames=0):
        plan.append({"method": method, "path": path, "headers": headers or {}, "body": body, "frames": frames})

    for path in _ROUTES:
        query = "?window=7d" if path in {"/api/lp/overview", "/api/lp/pools"} else ("?id=" + POOL_ID if path == "/api/workbench/pool" else "")
        add("GET", path + query)
        add("GET", path + query, {"Accept-Encoding": "gzip"})
        add("GET", path + query, {"If-None-Match": "@etag"})
        add("HEAD", path + query)
        add("OPTIONS", path)
        add("GET", path + query, {"Authorization": "Basic dXNlcjpwdw==", "Cookie": "session=abc"})
    for path in _ASSETS:
        add("GET", path)
        add("GET", path, {"Accept-Encoding": "gzip"})
        add("OPTIONS", path)
    add("GET", "/api/workbench/capabilities")
    add("GET", "/api/workbench/capabilities", {"Host": "127.0.0.1"})
    add("OPTIONS", "/api/workbench/capabilities")
    add("HEAD", "/api/lp/stream")
    for path in ("/api/nope", "/stream", "/health", "/api/gate/me", "/api/v1/stream"):
        add("GET", path)
        add("OPTIONS", path)
        add("POST", path, {"Content-Type": "application/json", "Origin": "https://rhpools.lol", "Host": "rhpools.lol"}, "{}")
    for path in ("/api/lp/allocation", "/api/workbench/simulate", "/api/workbench/prepare"):
        add("POST", path, {"Content-Type": "application/json"}, "{}")
        add("POST", path, {"Content-Type": "text/plain", "Origin": "https://rhpools.lol", "Host": "rhpools.lol"}, "{}")
        add("POST", path, {"Content-Type": "application/json", "Origin": "https://rhpools.lol", "Host": "rhpools.lol"}, "")
    add("GET", "/api/lp/stream?view=terminal&current_only=1&channel=both", frames=3)
    add("GET", "/api/lp/stream?channel=heads", frames=2)
    add("GET", "/api/workbench/stream?id=" + POOL_ID, frames=1)
    return plan


def _read_frames(response, count: int) -> list[str]:
    frames, current = [], []
    while len(frames) < count:
        raw = response.readline()
        if not raw:
            break
        line = raw.decode()
        if line == "\n":
            frames.append("".join(current))
            current = []
        else:
            current.append(line)
    return frames


def capture(address, request: dict, etags: dict) -> dict:
    headers = dict(request["headers"])
    if headers.get("If-None-Match") == "@etag":
        headers["If-None-Match"] = etags.get(request["path"], "@etag")
    connection = http.client.HTTPConnection(*address, timeout=5)
    try:
        connection.request(request["method"], request["path"], request["body"], headers)
        response = connection.getresponse()
        record = {
            "request": {**request, "headers": headers},
            "status": response.status,
            "headers": [[k, v] for k, v in response.getheaders() if k.lower() != "date"],
        }
        if request["frames"]:
            record["frames"] = _read_frames(response, request["frames"])
        elif request["path"] in _ASSETS:
            record["body_sha256"] = hashlib.sha256(response.read()).hexdigest()
        else:
            record["body"] = base64.b64encode(response.read()).decode()
        if response.getheader("ETag"):
            etags.setdefault(request["path"], response.getheader("ETag"))
        return record
    finally:
        connection.close()


def record(plan: list[dict] | None = None, handler=None, **resources) -> list[dict]:
    runtime = stub_runtime()
    runtime.__dict__.update(resources)
    handler = type("GoldenHandler", (handler or Handler,), {"runtime": runtime, "log_message": lambda *a: None})
    server = LPHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        etags: dict = {}
        return [capture(server.server_address, request, etags) for request in plan or requests()]
    finally:
        runtime.stopping.set()
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", type=Path, default=Path(__file__).with_name("anonymous.json"))
    args = ap.parse_args(argv)
    records = record()
    args.out.write_text(json.dumps(records, indent=0, sort_keys=True) + "\n")
    print(f"{len(records)} responses -> {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
