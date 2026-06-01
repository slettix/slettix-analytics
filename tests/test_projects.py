"""Unit tests for dataportal/projects.py (PRJ-1, epic #199)."""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT / "dataportal"))


@pytest.fixture(autouse=True)
def isolated_db(monkeypatch, tmp_path):
    """Hver test får sin egen midlertidige SQLite-fil."""
    db_file = tmp_path / "test.db"
    monkeypatch.setenv("AUTH_DB", str(db_file))
    # Reload moduler så de plukker opp ny DB_PATH
    for mod in ["auth", "projects"]:
        if mod in sys.modules:
            del sys.modules[mod]
    import auth, projects  # noqa: F401
    auth.init_db()
    yield
    # Cleanup-modulene så neste test får fersk state
    for mod in ["auth", "projects"]:
        sys.modules.pop(mod, None)


def _make_user(username="alice"):
    import auth
    return auth.create_user(username, f"{username}@example.com", "pwd123")


# ─── Schema-migrering ────────────────────────────────────────────────────────


def test_schema_creates_tables():
    import auth
    with auth._db() as conn:
        tables = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    assert {"projects", "project_memberships", "project_artifacts", "project_events"} <= tables


def test_schema_idempotent():
    """Re-kjør init_project_schema flere ganger uten feil."""
    import projects
    projects.init_project_schema()
    projects.init_project_schema()  # should not raise


# ─── Slug-generering ─────────────────────────────────────────────────────────


def test_slugify_basic():
    from projects import _slugify
    assert _slugify("Kreft & bostedsmønster Q2 2026") == "kreft-bostedsm-nster-q2-2026"


def test_slugify_handles_only_special_chars():
    from projects import _slugify
    assert _slugify("!@#$%") == "prosjekt"


def test_slugify_strips_leading_trailing_dashes():
    from projects import _slugify
    assert _slugify("  hello world  ") == "hello-world"


def test_unique_slug_appends_suffix_on_collision():
    import projects
    user = _make_user()
    p1 = projects.create_project("Mitt prosjekt", owner_id=user["id"])
    p2 = projects.create_project("Mitt prosjekt", owner_id=user["id"])
    p3 = projects.create_project("Mitt prosjekt", owner_id=user["id"])
    assert p1["slug"] == "mitt-prosjekt"
    assert p2["slug"] == "mitt-prosjekt-2"
    assert p3["slug"] == "mitt-prosjekt-3"


# ─── create_project ──────────────────────────────────────────────────────────


def test_create_project_sets_owner_membership():
    import projects
    user = _make_user()
    p = projects.create_project("Test", owner_id=user["id"])
    assert p["status"] == "active"
    assert p["owner_id"] == user["id"]
    assert projects.get_role(p["id"], user["id"]) == "owner"


def test_create_project_logs_event():
    import projects
    user = _make_user()
    p = projects.create_project("Test", owner_id=user["id"])
    events = projects.list_events(p["id"])
    assert len(events) == 1
    assert events[0]["event_type"] == "project_created"


def test_create_project_empty_name_raises():
    import projects
    user = _make_user()
    with pytest.raises(ValueError):
        projects.create_project("", owner_id=user["id"])


# ─── get/list ────────────────────────────────────────────────────────────────


def test_get_project_by_slug_or_id():
    import projects
    user = _make_user()
    p = projects.create_project("Test", owner_id=user["id"])
    assert projects.get_project(p["slug"])["id"] == p["id"]
    assert projects.get_project(p["id"])["slug"] == p["slug"]
    assert projects.get_project("ikke-finnes") is None


def test_list_all_projects_skips_archived_by_default():
    import projects
    user = _make_user()
    p1 = projects.create_project("Aktiv", owner_id=user["id"])
    p2 = projects.create_project("Arkivert", owner_id=user["id"])
    projects.archive_project(p2["id"])
    active = projects.list_all_projects()
    assert {p["id"] for p in active} == {p1["id"]}
    all_inc = projects.list_all_projects(include_archived=True)
    assert {p["id"] for p in all_inc} == {p1["id"], p2["id"]}


def test_list_user_projects_returns_only_member_projects():
    import projects
    alice = _make_user("alice")
    bob   = _make_user("bob")
    p_a = projects.create_project("Alice sitt", owner_id=alice["id"])
    p_b = projects.create_project("Bobs", owner_id=bob["id"])
    assert {p["id"] for p in projects.list_user_projects(alice["id"])} == {p_a["id"]}
    assert {p["id"] for p in projects.list_user_projects(bob["id"])}   == {p_b["id"]}


# ─── update / archive / delete ───────────────────────────────────────────────


def test_update_project_metadata():
    import projects
    user = _make_user()
    p = projects.create_project("Old name", owner_id=user["id"])
    updated = projects.update_project(p["slug"], name="New name", actor_id=user["id"])
    assert updated["name"] == "New name"


def test_archive_then_unarchive():
    import projects
    user = _make_user()
    p = projects.create_project("Test", owner_id=user["id"])
    assert projects.archive_project(p["id"])["status"] == "archived"
    assert projects.unarchive_project(p["id"])["status"] == "active"


def test_delete_project_cascades_artifacts_and_events():
    import projects
    user = _make_user()
    p = projects.create_project("Test", owner_id=user["id"])
    projects.add_artifact(p["id"], "notebook", "nb.ipynb",
                          ref_url="/jupyter/nb.ipynb", added_by=user["id"])
    assert projects.delete_project(p["id"]) is True
    assert projects.get_project(p["id"]) is None
    import auth
    with auth._db() as conn:
        n_art = conn.execute("SELECT COUNT(*) FROM project_artifacts WHERE project_id = ?",
                             (p["id"],)).fetchone()[0]
        n_evt = conn.execute("SELECT COUNT(*) FROM project_events WHERE project_id = ?",
                             (p["id"],)).fetchone()[0]
    assert n_art == 0 and n_evt == 0


# ─── Medlemmer ───────────────────────────────────────────────────────────────


def test_add_member_with_role():
    import projects
    alice = _make_user("alice")
    bob   = _make_user("bob")
    p = projects.create_project("Test", owner_id=alice["id"])
    projects.add_member(p["id"], bob["id"], "contributor", actor_id=alice["id"])
    assert projects.get_role(p["id"], bob["id"]) == "contributor"


def test_add_member_owner_role_rejected():
    import projects
    alice = _make_user("alice")
    bob   = _make_user("bob")
    p = projects.create_project("Test", owner_id=alice["id"])
    with pytest.raises(ValueError, match="transfer"):
        projects.add_member(p["id"], bob["id"], "owner")


def test_remove_member_works_for_non_owner():
    import projects
    alice = _make_user("alice")
    bob   = _make_user("bob")
    p = projects.create_project("Test", owner_id=alice["id"])
    projects.add_member(p["id"], bob["id"], "viewer")
    assert projects.remove_member(p["id"], bob["id"]) is True
    assert projects.is_member(p["id"], bob["id"]) is False


def test_cannot_remove_owner_directly():
    import projects
    alice = _make_user("alice")
    p = projects.create_project("Test", owner_id=alice["id"])
    with pytest.raises(ValueError, match="transfer"):
        projects.remove_member(p["id"], alice["id"])


def test_transfer_ownership_demotes_old_owner():
    import projects
    alice = _make_user("alice")
    bob   = _make_user("bob")
    p = projects.create_project("Test", owner_id=alice["id"])
    projects.transfer_ownership(p["id"], bob["id"], actor_id=alice["id"])
    assert projects.get_role(p["id"], bob["id"])   == "owner"
    assert projects.get_role(p["id"], alice["id"]) == "contributor"
    refreshed = projects.get_project(p["id"])
    assert refreshed["owner_id"] == bob["id"]


def test_change_role():
    import projects
    alice = _make_user("alice")
    bob   = _make_user("bob")
    p = projects.create_project("Test", owner_id=alice["id"])
    projects.add_member(p["id"], bob["id"], "viewer")
    projects.change_role(p["id"], bob["id"], "contributor", actor_id=alice["id"])
    assert projects.get_role(p["id"], bob["id"]) == "contributor"


def test_list_members_includes_role_and_username():
    import projects
    alice = _make_user("alice")
    bob   = _make_user("bob")
    p = projects.create_project("Test", owner_id=alice["id"])
    projects.add_member(p["id"], bob["id"], "viewer")
    members = projects.list_members(p["id"])
    usernames_roles = {(m["username"], m["role"]) for m in members}
    assert usernames_roles == {("alice", "owner"), ("bob", "viewer")}


# ─── Artefakter ──────────────────────────────────────────────────────────────


def test_add_artifact_all_types():
    import projects
    user = _make_user()
    p = projects.create_project("Test", owner_id=user["id"])
    for atype in projects.ARTIFACT_TYPES:
        a = projects.add_artifact(p["id"], atype, f"{atype}-art",
                                  ref_url=f"/x/{atype}", added_by=user["id"])
        assert a["artifact_type"] == atype


def test_add_artifact_invalid_type_raises():
    import projects
    user = _make_user()
    p = projects.create_project("Test", owner_id=user["id"])
    with pytest.raises(ValueError, match="artefakt"):
        projects.add_artifact(p["id"], "ufo", "x", added_by=user["id"])


def test_add_artifact_with_tags():
    import projects
    user = _make_user()
    p = projects.create_project("Test", owner_id=user["id"])
    a = projects.add_artifact(p["id"], "notebook", "nb", ref_url="/x",
                              tags=["hypotese", "Q2"], added_by=user["id"])
    assert a["tags"] == ["hypotese", "Q2"]


def test_remove_artifact():
    import projects
    user = _make_user()
    p = projects.create_project("Test", owner_id=user["id"])
    a = projects.add_artifact(p["id"], "notebook", "nb", ref_url="/x", added_by=user["id"])
    assert projects.remove_artifact(p["id"], a["id"]) is True
    assert projects.list_artifacts(p["id"]) == []


def test_list_artifacts_filter_by_type():
    import projects
    user = _make_user()
    p = projects.create_project("Test", owner_id=user["id"])
    projects.add_artifact(p["id"], "notebook",  "nb",   ref_url="/x", added_by=user["id"])
    projects.add_artifact(p["id"], "dashboard", "dash", ref_url="/y", added_by=user["id"])
    nbs = projects.list_artifacts(p["id"], artifact_type="notebook")
    assert len(nbs) == 1
    assert nbs[0]["artifact_type"] == "notebook"


# ─── Events ─────────────────────────────────────────────────────────────────


def test_event_logged_for_each_action():
    import projects
    alice = _make_user("alice")
    bob   = _make_user("bob")
    p = projects.create_project("Test", owner_id=alice["id"])
    projects.add_member(p["id"], bob["id"], "contributor")
    a = projects.add_artifact(p["id"], "notebook", "nb", ref_url="/x", added_by=bob["id"])
    projects.remove_artifact(p["id"], a["id"])
    types = [e["event_type"] for e in projects.list_events(p["id"])]
    # Sortert nyeste først
    assert types == ["artifact_removed", "artifact_added", "member_added", "project_created"]


def test_log_event_invalid_type_raises():
    import projects
    user = _make_user()
    p = projects.create_project("Test", owner_id=user["id"])
    with pytest.raises(ValueError, match="event-type"):
        projects.log_event(p["id"], "ufo", user["id"])
