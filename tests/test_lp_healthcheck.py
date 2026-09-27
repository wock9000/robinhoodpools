"""Availability recovery must not turn stale data or an edge outage into downtime."""
import json
from types import SimpleNamespace

from rhpools import lp_healthcheck as health


def test_failed_restarts_obey_cooldown_hourly_limit_and_clock_rollback(tmp_path, monkeypatch):
    args = SimpleNamespace(app_unit="robinhoodpools.service", timeout=1)
    state = health._empty_state()
    state["origin_failures"] = 3
    path = tmp_path / "health.json"
    clock = [10000]
    restarts = []
    monkeypatch.setattr(health, "_systemd_status", lambda *_: {
        "active_state": "active", "active_age_s": 1000,
    })

    def restart(*_):
        restarts.append(clock[0])
        return False, "restart_failed"

    monkeypatch.setattr(health, "_restart_unit", restart)
    for now in (10000, 10001, 10600, 11200, 11800, 8000):
        clock[0] = now
        health._app_recovery(args, state, now, path)
    assert restarts == [10000, 10600, 11200]


def test_stale_index_and_public_failure_do_not_restart_healthy_origin(tmp_path, monkeypatch, capsys):
    restarts = []
    payload = {
        "head": 200000, "indexed_head": 100000, "lag_blocks": 100000,
        "state": "catching_up", "as_of": 1,
    }
    monkeypatch.setattr(health, "_probe_target", lambda url, timeout: (
        ({"ok": True, "checks": []}, payload) if "127.0.0.1" in url
        else ({"ok": False, "checks": [{"path": "/", "ok": False, "status": 403}]}, None)
    ))
    monkeypatch.setattr(health, "_systemd_status", lambda *_: {
        "active_state": "active", "active_age_s": 1000,
    })
    monkeypatch.setattr(health, "_restart_unit", lambda unit, timeout: restarts.append(unit))
    argv = ["--state-path", str(tmp_path / "health.json")]
    for _ in range(4):
        assert health.main(argv) == 1
        report = json.loads(capsys.readouterr().out)
        assert report["origin"]["ok"] is True
        assert report["origin"]["status"]["fresh"] is False
        assert report["public"]["ok"] is False
    assert restarts == []


def _starting(started_at, phase, read_bytes, activity=True):
    payload = {
        "chain_id": 4663, "state": "starting", "startup": {
            "started_at": started_at, "phase": phase, "elapsed_s": 1,
            "activity": {"cpu_ms": 5, "read_bytes": read_bytes, "write_bytes": 0} if activity else None,
        },
    }
    return {"ok": False, "starting": True, "checks": []}, payload


def test_starting_origin_is_restarted_only_after_progress_stalls(tmp_path, monkeypatch):
    probes = {}
    restarts = []
    monkeypatch.setattr(health, "_probe_target", lambda url, timeout: (
        probes["origin"] if "127.0.0.1" in url else ({"ok": False, "checks": []}, None)
    ))
    monkeypatch.setattr(health, "_systemd_status", lambda *_: {
        "active_state": "active", "active_age_s": 100_000,
    })

    def restart(unit, _timeout):
        restarts.append(unit)
        return True, "restarted"

    monkeypatch.setattr(health, "_restart_unit", restart)
    clock = [1_000_000]
    monkeypatch.setattr(health.time, "time", lambda: clock[0])
    argv = ["--state-path", str(tmp_path / "health.json"), "--startup-stall-seconds", "300"]

    # Hours of slow WAL recovery: bytes keep moving, so nothing is restarted.
    for step in range(60):
        clock[0] += 60
        probes["origin"] = _starting(1.0, "store", read_bytes=step * 1_000_000)
        assert health.main(argv) == 1
        assert restarts == []
    state = json.loads((tmp_path / "health.json").read_text())
    assert state["origin_failures"] == 0

    # Counters freeze: failures accrue only once the stall window is exceeded.
    stalled = _starting(1.0, "store", read_bytes=59 * 1_000_000)
    for _ in range(5):
        clock[0] += 60
        probes["origin"] = stalled
        health.main(argv)
    assert restarts == []
    for _ in range(3):
        clock[0] += 60
        health.main(argv)
    assert restarts == ["robinhoodpools.service"]

    # The replacement process starts fresh: a new started_at resets the stall clock.
    clock[0] += 60
    probes["origin"] = _starting(2.0, "market", read_bytes=0)
    assert health.main(argv) == 1
    assert json.loads((tmp_path / "health.json").read_text())["origin_failures"] == 0


def test_starting_origin_without_activity_counters_cannot_mask_failure(tmp_path, monkeypatch):
    monkeypatch.setattr(health, "_probe_target", lambda url, timeout: (
        _starting(1.0, "store", 0, activity=False) if "127.0.0.1" in url else ({"ok": False, "checks": []}, None)
    ))
    argv = ["--state-path", str(tmp_path / "health.json")]
    for expected in (1, 2):
        assert health.main(argv) == 1
        assert json.loads((tmp_path / "health.json").read_text())["origin_failures"] == expected


def test_generic_503_status_body_is_not_a_startup_record():
    assert health._validate_starting(b'{"error":"API request capacity reached"}') is None
    assert health._validate_starting(b'{"chain_id":4663,"state":"starting","startup":{"started_at":1,"elapsed_s":1,"phase":"store","activity":{"cpu_ms":-1,"read_bytes":0,"write_bytes":0}}}') is None
    assert health._validate_starting(b'{"chain_id":4663,"state":"starting","startup":{"started_at":1,"elapsed_s":1,"phase":"store","activity":null}}') is not None
