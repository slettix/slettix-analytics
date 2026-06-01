"""
projects — analyseprosjekt-konseptet (epic #199, PRJ-1).

Tilbyr CRUD-helpers for analyseprosjekter, medlemskap, artefakter og
aktivitetsfeed. Gjenbruker auth-DB-en (samme SQLite-fil) for å holde
relasjoner enkelt.

Migrering kjøres fra `auth.init_db()` via `init_project_schema()` så vi
slipper å koordinere to init-flows.

Brukere som vil interagere på rad-nivå bruker funksjonene her i stedet for
direkte SQL — det gir oss ett sted å håndheve invarianter (én eier per
prosjekt, unike slugs, gyldige roller osv.)
"""

from __future__ import annotations

import json
import re
import sqlite3
import uuid
from datetime import datetime, timezone
from typing import Optional

import auth


# ── Konstanter ─────────────────────────────────────────────────────────────────

ROLES                 = ("owner", "contributor", "viewer")
PROJECT_STATUSES      = ("active", "archived")
ARTIFACT_TYPES        = ("notebook", "dashboard", "dataproduct", "document", "ml_model")
PROJECT_EVENT_TYPES   = (
    "project_created",
    "project_archived",
    "project_unarchived",
    "project_updated",
    "project_deleted",
    "member_added",
    "member_removed",
    "role_changed",
    "ownership_transferred",
    "artifact_added",
    "artifact_removed",
)

_SLUG_RE = re.compile(r"[^a-z0-9]+")
_SLUG_MAX_LEN = 50


# ── Schema-migrering ───────────────────────────────────────────────────────────

_SCHEMA_DDL = """
CREATE TABLE IF NOT EXISTS projects (
    id          TEXT PRIMARY KEY,
    slug        TEXT UNIQUE NOT NULL,
    name        TEXT NOT NULL,
    description TEXT,
    owner_id    TEXT NOT NULL,
    status      TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active','archived')),
    created_at  TEXT NOT NULL,
    updated_at  TEXT NOT NULL,
    FOREIGN KEY (owner_id) REFERENCES users(id)
);
CREATE INDEX IF NOT EXISTS idx_projects_owner  ON projects(owner_id);
CREATE INDEX IF NOT EXISTS idx_projects_status ON projects(status);

CREATE TABLE IF NOT EXISTS project_memberships (
    project_id TEXT NOT NULL,
    user_id    TEXT NOT NULL,
    role       TEXT NOT NULL CHECK (role IN ('owner','contributor','viewer')),
    added_at   TEXT NOT NULL,
    added_by   TEXT,
    PRIMARY KEY (project_id, user_id),
    FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE,
    FOREIGN KEY (user_id)    REFERENCES users(id)
);
CREATE INDEX IF NOT EXISTS idx_memberships_user ON project_memberships(user_id);

CREATE TABLE IF NOT EXISTS project_artifacts (
    id            TEXT PRIMARY KEY,
    project_id    TEXT NOT NULL,
    artifact_type TEXT NOT NULL CHECK (artifact_type IN ('notebook','dashboard','dataproduct','document','ml_model')),
    ref_url       TEXT,
    ref_id        TEXT,
    title         TEXT NOT NULL,
    description   TEXT,
    tags          TEXT,
    added_by      TEXT NOT NULL,
    added_at      TEXT NOT NULL,
    FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE,
    FOREIGN KEY (added_by)   REFERENCES users(id)
);
CREATE INDEX IF NOT EXISTS idx_artifacts_project ON project_artifacts(project_id);
CREATE INDEX IF NOT EXISTS idx_artifacts_type    ON project_artifacts(artifact_type);

CREATE TABLE IF NOT EXISTS project_events (
    id         TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    actor_id   TEXT,
    payload    TEXT,
    ts         TEXT NOT NULL,
    FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE,
    FOREIGN KEY (actor_id)   REFERENCES users(id)
);
CREATE INDEX IF NOT EXISTS idx_events_project_ts ON project_events(project_id, ts DESC);
"""


def init_project_schema() -> None:
    """Kalles fra auth.init_db() — idempotent."""
    with auth._db() as conn:
        conn.executescript(_SCHEMA_DDL)


# ── Hjelpere ───────────────────────────────────────────────────────────────────


def _now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def _new_id() -> str:
    return str(uuid.uuid4())


def _slugify(name: str) -> str:
    """kebab-case-slug fra fritekst-navn."""
    s = _SLUG_RE.sub("-", name.lower()).strip("-")
    return s[:_SLUG_MAX_LEN] or "prosjekt"


def _unique_slug(conn: sqlite3.Connection, base: str) -> str:
    """Returner base, eller base-2, base-3 … til vi finner ledig."""
    slug = base
    suffix = 2
    while conn.execute("SELECT 1 FROM projects WHERE slug = ?", (slug,)).fetchone():
        slug = f"{base}-{suffix}"
        suffix += 1
    return slug


def _row_to_project(row: sqlite3.Row) -> dict:
    return {
        "id":          row["id"],
        "slug":        row["slug"],
        "name":        row["name"],
        "description": row["description"],
        "owner_id":    row["owner_id"],
        "status":      row["status"],
        "created_at":  row["created_at"],
        "updated_at":  row["updated_at"],
    }


def _row_to_artifact(row: sqlite3.Row) -> dict:
    return {
        "id":            row["id"],
        "project_id":    row["project_id"],
        "artifact_type": row["artifact_type"],
        "ref_url":       row["ref_url"],
        "ref_id":        row["ref_id"],
        "title":         row["title"],
        "description":   row["description"],
        "tags":          json.loads(row["tags"]) if row["tags"] else [],
        "added_by":      row["added_by"],
        "added_at":      row["added_at"],
    }


# ── Prosjekt-CRUD ──────────────────────────────────────────────────────────────


def create_project(name: str, owner_id: str, description: str = "") -> dict:
    """Opprett nytt prosjekt. Owner blir automatisk lagt til som medlem."""
    name = (name or "").strip()
    if not name:
        raise ValueError("Prosjektnavn kan ikke være tomt")
    project_id = _new_id()
    now        = _now_iso()
    with auth._db() as conn:
        slug = _unique_slug(conn, _slugify(name))
        conn.execute(
            """INSERT INTO projects (id, slug, name, description, owner_id, status, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, 'active', ?, ?)""",
            (project_id, slug, name, description or "", owner_id, now, now),
        )
        conn.execute(
            """INSERT INTO project_memberships (project_id, user_id, role, added_at, added_by)
               VALUES (?, ?, 'owner', ?, ?)""",
            (project_id, owner_id, now, owner_id),
        )
        conn.execute(
            """INSERT INTO project_events (id, project_id, event_type, actor_id, payload, ts)
               VALUES (?, ?, 'project_created', ?, ?, ?)""",
            (_new_id(), project_id, owner_id, json.dumps({"name": name, "slug": slug}), now),
        )
        row = conn.execute("SELECT * FROM projects WHERE id = ?", (project_id,)).fetchone()
    return _row_to_project(row)


def get_project(slug_or_id: str) -> Optional[dict]:
    with auth._db() as conn:
        row = conn.execute(
            "SELECT * FROM projects WHERE slug = ? OR id = ?",
            (slug_or_id, slug_or_id),
        ).fetchone()
    return _row_to_project(row) if row else None


def list_all_projects(include_archived: bool = False) -> list[dict]:
    """Returner alle prosjekter (metadata er public). Sortert nyeste først."""
    sql = "SELECT * FROM projects"
    if not include_archived:
        sql += " WHERE status = 'active'"
    sql += " ORDER BY updated_at DESC"
    with auth._db() as conn:
        return [_row_to_project(r) for r in conn.execute(sql).fetchall()]


def list_user_projects(user_id: str, include_archived: bool = False) -> list[dict]:
    """Prosjekter der bruker er medlem (uansett rolle)."""
    sql = """
        SELECT p.* FROM projects p
        INNER JOIN project_memberships m ON m.project_id = p.id
        WHERE m.user_id = ?
    """
    if not include_archived:
        sql += " AND p.status = 'active'"
    sql += " ORDER BY p.updated_at DESC"
    with auth._db() as conn:
        return [_row_to_project(r) for r in conn.execute(sql, (user_id,)).fetchall()]


def update_project(slug_or_id: str, *, name: str | None = None, description: str | None = None,
                   actor_id: str | None = None) -> Optional[dict]:
    fields, values = [], []
    if name is not None:
        if not name.strip():
            raise ValueError("Navn kan ikke være tomt")
        fields.append("name = ?")
        values.append(name.strip())
    if description is not None:
        fields.append("description = ?")
        values.append(description)
    if not fields:
        return get_project(slug_or_id)
    fields.append("updated_at = ?")
    values.append(_now_iso())
    with auth._db() as conn:
        row = conn.execute("SELECT id FROM projects WHERE slug = ? OR id = ?",
                           (slug_or_id, slug_or_id)).fetchone()
        if not row:
            return None
        project_id = row["id"]
        values.append(project_id)
        conn.execute(f"UPDATE projects SET {', '.join(fields)} WHERE id = ?", values)
        log_event(project_id, "project_updated", actor_id,
                  {"changes": {f.split(" = ")[0]: v for f, v in zip(fields[:-1], values[:-2])}},
                  conn=conn)
    return get_project(slug_or_id)


def archive_project(slug_or_id: str, actor_id: str | None = None) -> Optional[dict]:
    return _set_project_status(slug_or_id, "archived", "project_archived", actor_id)


def unarchive_project(slug_or_id: str, actor_id: str | None = None) -> Optional[dict]:
    return _set_project_status(slug_or_id, "active", "project_unarchived", actor_id)


def _set_project_status(slug_or_id: str, status: str, event_type: str, actor_id: str | None) -> Optional[dict]:
    if status not in PROJECT_STATUSES:
        raise ValueError(f"Ugyldig status: {status}")
    now = _now_iso()
    with auth._db() as conn:
        row = conn.execute("SELECT id FROM projects WHERE slug = ? OR id = ?",
                           (slug_or_id, slug_or_id)).fetchone()
        if not row:
            return None
        pid = row["id"]
        conn.execute("UPDATE projects SET status = ?, updated_at = ? WHERE id = ?", (status, now, pid))
        log_event(pid, event_type, actor_id, {"status": status}, conn=conn)
    return get_project(slug_or_id)


def delete_project(slug_or_id: str, actor_id: str | None = None) -> bool:
    """Permanent slett. Eksplisitt rydding av relaterte rader siden SQLite-
    FK-CASCADE krever PRAGMA foreign_keys=ON som ikke er garantert satt
    på alle connections."""
    with auth._db() as conn:
        row = conn.execute("SELECT id FROM projects WHERE slug = ? OR id = ?",
                           (slug_or_id, slug_or_id)).fetchone()
        if not row:
            return False
        pid = row["id"]
        conn.execute("DELETE FROM project_artifacts   WHERE project_id = ?", (pid,))
        conn.execute("DELETE FROM project_memberships WHERE project_id = ?", (pid,))
        conn.execute("DELETE FROM project_events      WHERE project_id = ?", (pid,))
        conn.execute("DELETE FROM projects            WHERE id = ?",        (pid,))
    return True


# ── Medlemskap ─────────────────────────────────────────────────────────────────


def add_member(project_id: str, user_id: str, role: str, actor_id: str | None = None) -> dict:
    if role not in ROLES:
        raise ValueError(f"Ugyldig rolle: {role}")
    if role == "owner":
        raise ValueError("Bruk transfer_ownership for å sette ny owner")
    now = _now_iso()
    with auth._db() as conn:
        conn.execute(
            """INSERT OR REPLACE INTO project_memberships
               (project_id, user_id, role, added_at, added_by) VALUES (?, ?, ?, ?, ?)""",
            (project_id, user_id, role, now, actor_id),
        )
        log_event(project_id, "member_added", actor_id,
                  {"user_id": user_id, "role": role}, conn=conn)
    return {"project_id": project_id, "user_id": user_id, "role": role, "added_at": now}


def remove_member(project_id: str, user_id: str, actor_id: str | None = None) -> bool:
    with auth._db() as conn:
        row = conn.execute(
            "SELECT role FROM project_memberships WHERE project_id = ? AND user_id = ?",
            (project_id, user_id),
        ).fetchone()
        if not row:
            return False
        if row["role"] == "owner":
            raise ValueError("Kan ikke fjerne owner — bruk transfer_ownership først")
        conn.execute(
            "DELETE FROM project_memberships WHERE project_id = ? AND user_id = ?",
            (project_id, user_id),
        )
        log_event(project_id, "member_removed", actor_id, {"user_id": user_id}, conn=conn)
    return True


def change_role(project_id: str, user_id: str, new_role: str, actor_id: str | None = None) -> bool:
    if new_role not in ROLES or new_role == "owner":
        raise ValueError(f"Ugyldig rolle: {new_role} (bruk transfer_ownership for owner)")
    with auth._db() as conn:
        row = conn.execute(
            "SELECT role FROM project_memberships WHERE project_id = ? AND user_id = ?",
            (project_id, user_id),
        ).fetchone()
        if not row:
            return False
        if row["role"] == "owner":
            raise ValueError("Kan ikke endre owner-rolle — bruk transfer_ownership")
        conn.execute(
            "UPDATE project_memberships SET role = ? WHERE project_id = ? AND user_id = ?",
            (new_role, project_id, user_id),
        )
        log_event(project_id, "role_changed", actor_id,
                  {"user_id": user_id, "old_role": row["role"], "new_role": new_role},
                  conn=conn)
    return True


def transfer_ownership(project_id: str, new_owner_id: str, actor_id: str | None = None) -> bool:
    """Sett ny owner. Forrige owner blir contributor (ikke fjernet)."""
    with auth._db() as conn:
        prj = conn.execute("SELECT owner_id FROM projects WHERE id = ?", (project_id,)).fetchone()
        if not prj:
            return False
        old_owner = prj["owner_id"]
        if old_owner == new_owner_id:
            return True  # no-op
        # Sørg for at new_owner finnes som medlem
        conn.execute(
            """INSERT OR REPLACE INTO project_memberships
               (project_id, user_id, role, added_at, added_by) VALUES (?, ?, 'owner', ?, ?)""",
            (project_id, new_owner_id, _now_iso(), actor_id),
        )
        # Demoter gammel owner til contributor
        conn.execute(
            "UPDATE project_memberships SET role = 'contributor' WHERE project_id = ? AND user_id = ?",
            (project_id, old_owner),
        )
        conn.execute("UPDATE projects SET owner_id = ?, updated_at = ? WHERE id = ?",
                     (new_owner_id, _now_iso(), project_id))
        log_event(project_id, "ownership_transferred", actor_id,
                  {"old_owner": old_owner, "new_owner": new_owner_id}, conn=conn)
    return True


def list_members(project_id: str) -> list[dict]:
    with auth._db() as conn:
        rows = conn.execute(
            """SELECT m.*, u.username, u.email
               FROM project_memberships m
               LEFT JOIN users u ON u.id = m.user_id
               WHERE m.project_id = ?
               ORDER BY m.role, m.added_at""",
            (project_id,),
        ).fetchall()
    return [
        {
            "user_id":  r["user_id"],
            "username": r["username"],
            "email":    r["email"],
            "role":     r["role"],
            "added_at": r["added_at"],
            "added_by": r["added_by"],
        }
        for r in rows
    ]


def get_role(project_id: str, user_id: str) -> str | None:
    with auth._db() as conn:
        row = conn.execute(
            "SELECT role FROM project_memberships WHERE project_id = ? AND user_id = ?",
            (project_id, user_id),
        ).fetchone()
    return row["role"] if row else None


def is_member(project_id: str, user_id: str) -> bool:
    return get_role(project_id, user_id) is not None


# ── Artefakter ─────────────────────────────────────────────────────────────────


def add_artifact(project_id: str, artifact_type: str, title: str, *,
                 ref_url: str | None = None, ref_id: str | None = None,
                 description: str = "", tags: list[str] | None = None,
                 added_by: str) -> dict:
    if artifact_type not in ARTIFACT_TYPES:
        raise ValueError(f"Ugyldig artefakt-type: {artifact_type}")
    title = (title or "").strip()
    if not title:
        raise ValueError("Artefakt-tittel kan ikke være tom")
    artifact_id = _new_id()
    now = _now_iso()
    tags_json = json.dumps(tags or [])
    with auth._db() as conn:
        conn.execute(
            """INSERT INTO project_artifacts
               (id, project_id, artifact_type, ref_url, ref_id, title, description, tags, added_by, added_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (artifact_id, project_id, artifact_type, ref_url, ref_id, title,
             description, tags_json, added_by, now),
        )
        log_event(project_id, "artifact_added", added_by,
                  {"artifact_id": artifact_id, "type": artifact_type, "title": title},
                  conn=conn)
        row = conn.execute("SELECT * FROM project_artifacts WHERE id = ?", (artifact_id,)).fetchone()
    return _row_to_artifact(row)


def remove_artifact(project_id: str, artifact_id: str, actor_id: str | None = None) -> bool:
    with auth._db() as conn:
        row = conn.execute(
            "SELECT title, artifact_type FROM project_artifacts WHERE id = ? AND project_id = ?",
            (artifact_id, project_id),
        ).fetchone()
        if not row:
            return False
        conn.execute("DELETE FROM project_artifacts WHERE id = ?", (artifact_id,))
        log_event(project_id, "artifact_removed", actor_id,
                  {"artifact_id": artifact_id, "title": row["title"], "type": row["artifact_type"]},
                  conn=conn)
    return True


def list_artifacts(project_id: str, artifact_type: str | None = None) -> list[dict]:
    sql = "SELECT * FROM project_artifacts WHERE project_id = ?"
    params: list = [project_id]
    if artifact_type:
        sql += " AND artifact_type = ?"
        params.append(artifact_type)
    sql += " ORDER BY added_at DESC"
    with auth._db() as conn:
        return [_row_to_artifact(r) for r in conn.execute(sql, params).fetchall()]


# ── Aktivitetsfeed ─────────────────────────────────────────────────────────────


def log_event(project_id: str, event_type: str, actor_id: str | None,
              payload: dict | None = None, *, conn=None) -> None:
    """Logg en aktivitet. Hvis conn er gitt, bruk eksisterende transaksjon."""
    if event_type not in PROJECT_EVENT_TYPES:
        raise ValueError(f"Ukjent event-type: {event_type}")
    payload_json = json.dumps(payload or {})
    now = _now_iso()
    sql    = """INSERT INTO project_events (id, project_id, event_type, actor_id, payload, ts)
                VALUES (?, ?, ?, ?, ?, ?)"""
    params = (_new_id(), project_id, event_type, actor_id, payload_json, now)
    if conn is not None:
        conn.execute(sql, params)
    else:
        with auth._db() as c:
            c.execute(sql, params)


def list_events(project_id: str, limit: int = 50) -> list[dict]:
    with auth._db() as conn:
        rows = conn.execute(
            """SELECT e.*, u.username AS actor_username
               FROM project_events e
               LEFT JOIN users u ON u.id = e.actor_id
               WHERE e.project_id = ?
               ORDER BY e.ts DESC LIMIT ?""",
            (project_id, limit),
        ).fetchall()
    return [
        {
            "id":             r["id"],
            "event_type":     r["event_type"],
            "actor_id":       r["actor_id"],
            "actor_username": r["actor_username"],
            "payload":        json.loads(r["payload"]) if r["payload"] else {},
            "ts":             r["ts"],
        }
        for r in rows
    ]
