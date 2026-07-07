#!/usr/bin/env python3
"""M1694 P2: minimal smoke tests for stone CRUD + exec session control endpoints.

Not a full regression suite (0 -> 100% coverage on a 13,852-line server is not
realistic in one pass) — covers the highest-traffic paths so an obvious break
(500, wrong status code, missing field) fails fast instead of surfacing live.

Runs against the live hub on HUB_URL (default http://127.0.0.1:9001) using a
throwaway project so it never touches real project data. Requires the hub to
already be running — this is an integration smoke test, not a unit test.

Run: python3 -m pytest tests/test_smoke_api.py -v
"""
import os
import shutil
import uuid

import pytest
import requests

HUB_URL = os.environ.get("HUB_URL", "http://127.0.0.1:9001")
TEST_PROJ_NAME = f"_SmokeTest_{uuid.uuid4().hex[:8]}"


@pytest.fixture(scope="module")
def proj_id():
    """Register the project via the real creation endpoint (POST /api/northstar/create,
    NOT POST /api/northstar — that one is bulk-save-only and silently no-ops on a
    single dict body, discovered while writing this suite). The endpoint creates the
    on-disk project dir itself and returns the sanitized folder id."""
    r = requests.post(f"{HUB_URL}/api/northstar/create", json={"name": TEST_PROJ_NAME}, timeout=10)
    assert r.status_code == 200, f"project creation failed: {r.status_code} {r.text}"
    pid = r.json()["id"]
    yield pid
    # M1694 P2 fix: DELETE /api/northstar/{id} must run BEFORE the directory is removed —
    # discovered live that the endpoint's existence check used to look at the filesystem,
    # not the DB, so removing the directory first left the project_meta row orphaned
    # forever (the endpoint would 404 and never reach its own DELETE statement). Fixed
    # server-side too, but keep this order regardless — don't rely on cleanup succeeding
    # only because of that one fix.
    try:
        requests.delete(f"{HUB_URL}/api/northstar/{pid}", timeout=10)
    except Exception:
        pass
    shutil.rmtree(os.path.join(os.path.expanduser("~/.hub/projects"), pid), ignore_errors=True)


def test_hub_is_reachable():
    r = requests.get(f"{HUB_URL}/api/northstar", timeout=10)
    assert r.status_code == 200


def test_stone_create(proj_id):
    r = requests.post(
        f"{HUB_URL}/api/northstar/{proj_id}/milestones",
        json={"text": "smoke test stone", "status": "pending"},
        timeout=10,
    )
    assert r.status_code == 200
    d = r.json()
    assert d.get("ok") is True
    assert d["milestone"]["id"].startswith("M")


def test_stone_list(proj_id):
    r = requests.get(f"{HUB_URL}/api/northstar/{proj_id}/milestones", timeout=10)
    assert r.status_code == 200
    d = r.json()
    assert isinstance(d.get("milestones"), list)
    assert len(d["milestones"]) >= 1


def test_stone_patch_and_append_message(proj_id):
    create = requests.post(
        f"{HUB_URL}/api/northstar/{proj_id}/milestones",
        json={"text": "smoke test patch target", "status": "pending"},
        timeout=10,
    ).json()
    mid = create["milestone"]["id"]

    r = requests.patch(
        f"{HUB_URL}/api/northstar/{proj_id}/milestones/{mid}",
        json={"append_message": {"role": "claude", "text": "smoke reply"}},
        timeout=10,
    )
    assert r.status_code == 200
    assert r.json().get("ok") is True

    listed = requests.get(f"{HUB_URL}/api/northstar/{proj_id}/milestones", timeout=10).json()
    stone = next(m for m in listed["milestones"] if m["id"] == mid)
    assert stone["conversation"][-1]["text"] == "smoke reply"


def test_stone_patch_rejects_claude_self_reply(proj_id):
    """M190 protocol: claude cannot post two consecutive claude messages."""
    create = requests.post(
        f"{HUB_URL}/api/northstar/{proj_id}/milestones",
        json={"text": "smoke test self-reply guard", "status": "pending"},
        timeout=10,
    ).json()
    mid = create["milestone"]["id"]

    requests.patch(
        f"{HUB_URL}/api/northstar/{proj_id}/milestones/{mid}",
        json={"append_message": {"role": "claude", "text": "first"}},
        timeout=10,
    )
    r = requests.patch(
        f"{HUB_URL}/api/northstar/{proj_id}/milestones/{mid}",
        json={"append_message": {"role": "claude", "text": "second"}},
        timeout=10,
    )
    assert r.status_code == 409
    assert r.json().get("error") == "claude_self_reply_blocked"


def test_stone_delete(proj_id):
    create = requests.post(
        f"{HUB_URL}/api/northstar/{proj_id}/milestones",
        json={"text": "smoke test delete target", "status": "pending"},
        timeout=10,
    ).json()
    mid = create["milestone"]["id"]

    r = requests.delete(f"{HUB_URL}/api/northstar/{proj_id}/milestones/{mid}", timeout=10)
    assert r.status_code == 200

    listed = requests.get(f"{HUB_URL}/api/northstar/{proj_id}/milestones", timeout=10).json()
    assert all(m["id"] != mid for m in listed["milestones"])


def test_execute_project_init_mode_when_no_milestones():
    """A project with zero active milestones should hit INIT mode, not spawn a session."""
    name = f"_SmokeTestEmpty_{uuid.uuid4().hex[:8]}"
    created = requests.post(f"{HUB_URL}/api/northstar/create", json={"name": name}, timeout=10).json()
    pid = created["id"]
    try:
        r = requests.post(f"{HUB_URL}/api/northstar/{pid}/execute", json={}, timeout=15)
        assert r.status_code == 200
        assert r.json().get("mode") == "init"
    finally:
        try:
            requests.delete(f"{HUB_URL}/api/northstar/{pid}", timeout=10)
        except Exception:
            pass
        shutil.rmtree(os.path.join(os.path.expanduser("~/.hub/projects"), pid), ignore_errors=True)


def test_execute_project_badge_no_spawn_when_session_dead(proj_id):
    """M1096/M1710: badge-triggered execute must never spawn a dead session."""
    r = requests.post(
        f"{HUB_URL}/api/northstar/{proj_id}/execute",
        json={"from_badge": True},
        timeout=15,
    )
    assert r.status_code == 200
    d = r.json()
    # proj_id has no live tmux session, so this must short-circuit to no-op.
    assert d.get("status") == "badge_no_spawn" or d.get("mode") == "init"
