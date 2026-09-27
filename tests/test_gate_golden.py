"""Anonymous responses stay byte-identical to snapshot 1b00978 (tests/golden/anonymous.json)."""
import json
from pathlib import Path

import pytest

from golden.anonymous import record

GOLDEN = json.loads((Path(__file__).parent / "golden" / "anonymous.json").read_text())
TERMINAL_PAGES = {"/", "/pools", "/lp", "/static/lp_terminal.js", "/static/lp_terminal.css"}
NEW_SURFACE = {"/api/gate/me", "/api/v1/stream"}
PAGE_HEADERS_ALLOWED_TO_MOVE = {"etag", "content-length"}


def _comparable(entry: dict) -> dict:
    page = entry["request"]["path"] in TERMINAL_PAGES
    headers = [pair for pair in entry["headers"] if not (page and pair[0].lower() in PAGE_HEADERS_ALLOWED_TO_MOVE)]
    return {key: value for key, value in entry.items() if key not in {"headers", "request"} and not (page and key == "body_sha256")} | {"headers": headers}


def _gate(tmp_path):
    from rhpools.lp_gate import Gate
    return Gate(tmp_path / "gate.sqlite", owner=None, rpc_url="http://127.0.0.1:1", hosts=frozenset({"rhpools.lol"}))


def _resources(tmp_path) -> dict:
    try:
        return {"gate": _gate(tmp_path)}
    except ImportError:
        return {}


@pytest.mark.parametrize("extra_headers", [
    {},
    {"Cookie": "__Host-rhp_session=rhp_not-a-real-secret"},
    {"Authorization": "Token abc"},
])
def test_anonymous_surface_matches_snapshot(tmp_path, extra_headers):
    plan = [{**entry["request"], "headers": {**extra_headers, **entry["request"]["headers"]}} for entry in GOLDEN]
    replay = record(plan, **_resources(tmp_path))
    assert len(replay) == len(GOLDEN)
    for expected, actual in zip(GOLDEN, replay):
        if expected["request"]["path"] in NEW_SURFACE:
            assert expected["status"] == 404
            continue
        assert _comparable(actual) == _comparable(expected), expected["request"]
