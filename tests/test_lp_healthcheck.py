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
