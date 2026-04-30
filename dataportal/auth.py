"""
auth.py — JWT-basert autentisering for Slettix Data Portal

Arkitektur:
  - Access token  : kortlivet JWT (30 min), signert med HS256
  - Refresh token : langlivet UUID (7 dager), lagret i SQLite (revocerbart)

Tabeller (SQLite):
  users            — brukere med rolle (admin / user)
  refresh_tokens   — aktive refresh-tokens (revocerbare)
  access_requests  — forespørsler om tilgang til restricted produkter
  user_products    — hvilke restricted produkter en bruker har tilgang til
"""

import os
import sqlite3
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Optional

from jose import JWTError, jwt
from passlib.context import CryptContext

# ── konfigurasjon ───────────────────────────────────────────────────────────────

SECRET_KEY       = os.environ.get("JWT_SECRET", "slettix-dev-jwt-secret-change-in-prod")
ALGORITHM        = "HS256"
ACCESS_TOKEN_TTL = timedelta(minutes=30)
REFRESH_TOKEN_TTL = timedelta(days=7)

DB_PATH = os.environ.get("AUTH_DB", "/opt/dataportal/auth.db")

_pwd_ctx = CryptContext(schemes=["bcrypt"], deprecated="auto")

# ── database ────────────────────────────────────────────────────────────────────

@contextmanager
def _db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


PERSONAS = ("analyst", "engineer", "domain_owner")


def init_db() -> None:
    with _db() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS users (
                id           TEXT PRIMARY KEY,
                username     TEXT UNIQUE NOT NULL,
                email        TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                role         TEXT NOT NULL DEFAULT 'user',
                created_at   TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS refresh_tokens (
                token_id    TEXT PRIMARY KEY,
                user_id     TEXT NOT NULL,
                expires_at  TEXT NOT NULL,
                revoked     INTEGER NOT NULL DEFAULT 0,
                FOREIGN KEY (user_id) REFERENCES users(id)
            );

            CREATE TABLE IF NOT EXISTS access_requests (
                id           TEXT PRIMARY KEY,
                user_id      TEXT NOT NULL,
                product_id   TEXT NOT NULL,
                status       TEXT NOT NULL DEFAULT 'pending',
                requested_at TEXT NOT NULL,
                resolved_at  TEXT,
                resolved_by  TEXT,
                FOREIGN KEY (user_id) REFERENCES users(id)
            );

            CREATE TABLE IF NOT EXISTS user_products (
                user_id    TEXT NOT NULL,
                product_id TEXT NOT NULL,
                granted_at TEXT NOT NULL,
                PRIMARY KEY (user_id, product_id),
                FOREIGN KEY (user_id) REFERENCES users(id)
            );

            CREATE TABLE IF NOT EXISTS subscriptions (
                id          TEXT PRIMARY KEY,
                user_id     TEXT NOT NULL,
                product_id  TEXT NOT NULL,
                created_at  TEXT NOT NULL,
                UNIQUE (user_id, product_id),
                FOREIGN KEY (user_id) REFERENCES users(id)
            );

            CREATE TABLE IF NOT EXISTS domain_memberships (
                user_id   TEXT NOT NULL,
                domain    TEXT NOT NULL,
                PRIMARY KEY (user_id, domain),
                FOREIGN KEY (user_id) REFERENCES users(id)
            );

            CREATE TABLE IF NOT EXISTS incidents (
                id           TEXT PRIMARY KEY,
                product_id   TEXT NOT NULL,
                title        TEXT NOT NULL,
                description  TEXT,
                status       TEXT NOT NULL DEFAULT 'open',
                severity     TEXT NOT NULL DEFAULT 'warning',
                created_at   TEXT NOT NULL,
                updated_at   TEXT NOT NULL,
                resolved_at  TEXT,
                created_by   TEXT NOT NULL DEFAULT 'system',
                updated_by   TEXT
            );

            CREATE TABLE IF NOT EXISTS usage_events (
                id         TEXT PRIMARY KEY,
                product_id TEXT NOT NULL,
                action     TEXT NOT NULL,
                user_id    TEXT,
                ts         TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_usage_product ON usage_events(product_id);
            CREATE INDEX IF NOT EXISTS idx_usage_ts ON usage_events(ts);
        """)
        # GUIDE-1/4: legg til persona og onboarding-felter (idempotent migrasjon)
        existing_cols = {r[1] for r in conn.execute("PRAGMA table_info(users)").fetchall()}
        if "persona" not in existing_cols:
            conn.execute("ALTER TABLE users ADD COLUMN persona TEXT")
        if "onboarding_completed_at" not in existing_cols:
            conn.execute("ALTER TABLE users ADD COLUMN onboarding_completed_at TEXT")

        # Opprett admin-bruker hvis ingen finnes
        row = conn.execute("SELECT COUNT(*) FROM users").fetchone()
        if row[0] == 0:
            _create_user_inner(conn, "admin", "admin@slettix.local", "admin", role="admin")


def _create_user_inner(conn, username: str, email: str, password: str, role: str = "user") -> dict:
    user_id = str(uuid.uuid4())
    now     = datetime.now(tz=timezone.utc).isoformat()
    conn.execute(
        "INSERT INTO users (id, username, email, password_hash, role, created_at) VALUES (?,?,?,?,?,?)",
        (user_id, username, email, _pwd_ctx.hash(password), role, now),
    )
    return {"id": user_id, "username": username, "email": email, "role": role, "created_at": now}

# ── brukerfunksjoner ────────────────────────────────────────────────────────────

def create_user(username: str, email: str, password: str, role: str = "user") -> dict:
    with _db() as conn:
        return _create_user_inner(conn, username, email, password, role)


def get_user_by_username(username: str) -> Optional[dict]:
    with _db() as conn:
        row = conn.execute(
            "SELECT id, username, email, password_hash, role, created_at FROM users WHERE username = ?",
            (username,),
        ).fetchone()
        return dict(row) if row else None


def get_user_by_id(user_id: str) -> Optional[dict]:
    with _db() as conn:
        row = conn.execute(
            "SELECT id, username, email, role, created_at FROM users WHERE id = ?",
            (user_id,),
        ).fetchone()
        return dict(row) if row else None


def list_users() -> list[dict]:
    with _db() as conn:
        rows = conn.execute(
            "SELECT id, username, email, role, created_at FROM users ORDER BY created_at DESC"
        ).fetchall()
        return [dict(r) for r in rows]


def update_user_role(user_id: str, role: str) -> None:
    with _db() as conn:
        conn.execute("UPDATE users SET role = ? WHERE id = ?", (role, user_id))


def delete_user(user_id: str) -> None:
    with _db() as conn:
        conn.execute("DELETE FROM refresh_tokens WHERE user_id = ?", (user_id,))
        conn.execute("DELETE FROM user_products WHERE user_id = ?", (user_id,))
        conn.execute("DELETE FROM access_requests WHERE user_id = ?", (user_id,))
        conn.execute("DELETE FROM domain_memberships WHERE user_id = ?", (user_id,))
        conn.execute("DELETE FROM users WHERE id = ?", (user_id,))


def verify_password(plain: str, hashed: str) -> bool:
    return _pwd_ctx.verify(plain, hashed)

# ── JWT ─────────────────────────────────────────────────────────────────────────

def create_access_token(user_id: str, username: str, role: str) -> str:
    expire  = datetime.now(tz=timezone.utc) + ACCESS_TOKEN_TTL
    payload = {"sub": user_id, "username": username, "role": role, "exp": expire}
    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)


def decode_access_token(token: str) -> Optional[dict]:
    try:
        return jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
    except JWTError:
        return None

# ── refresh tokens ──────────────────────────────────────────────────────────────

def create_refresh_token(user_id: str) -> str:
    token_id   = str(uuid.uuid4())
    expires_at = (datetime.now(tz=timezone.utc) + REFRESH_TOKEN_TTL).isoformat()
    with _db() as conn:
        conn.execute(
            "INSERT INTO refresh_tokens (token_id, user_id, expires_at) VALUES (?,?,?)",
            (token_id, user_id, expires_at),
        )
    return token_id


def rotate_refresh_token(old_token_id: str) -> Optional[tuple[str, str]]:
    """
    Validerer og roterer refresh token.
    Returnerer (new_refresh_token, user_id) eller None hvis ugyldig/utgått/revocert.
    """
    with _db() as conn:
        row = conn.execute(
            "SELECT user_id, expires_at, revoked FROM refresh_tokens WHERE token_id = ?",
            (old_token_id,),
        ).fetchone()
        if not row:
            return None
        if row["revoked"]:
            return None
        if datetime.fromisoformat(row["expires_at"]) < datetime.now(tz=timezone.utc):
            return None
        user_id = row["user_id"]
        # Revoker gammelt token
        conn.execute("UPDATE refresh_tokens SET revoked = 1 WHERE token_id = ?", (old_token_id,))
        # Opprett nytt
        new_token_id = str(uuid.uuid4())
        new_expires  = (datetime.now(tz=timezone.utc) + REFRESH_TOKEN_TTL).isoformat()
        conn.execute(
            "INSERT INTO refresh_tokens (token_id, user_id, expires_at) VALUES (?,?,?)",
            (new_token_id, user_id, new_expires),
        )
        return new_token_id, user_id


def revoke_refresh_token(token_id: str) -> None:
    with _db() as conn:
        conn.execute("UPDATE refresh_tokens SET revoked = 1 WHERE token_id = ?", (token_id,))

# ── tilgang til restricted produkter ───────────────────────────────────────────

def user_has_product_access(user_id: str, product_id: str) -> bool:
    with _db() as conn:
        row = conn.execute(
            "SELECT 1 FROM user_products WHERE user_id = ? AND product_id = ?",
            (user_id, product_id),
        ).fetchone()
        return row is not None


def grant_product_access(user_id: str, product_id: str) -> None:
    now = datetime.now(tz=timezone.utc).isoformat()
    with _db() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO user_products (user_id, product_id, granted_at) VALUES (?,?,?)",
            (user_id, product_id, now),
        )


def revoke_product_access(user_id: str, product_id: str) -> None:
    with _db() as conn:
        conn.execute(
            "DELETE FROM user_products WHERE user_id = ? AND product_id = ?",
            (user_id, product_id),
        )

# ── tilgangsforespørsler ────────────────────────────────────────────────────────

def create_access_request(user_id: str, product_id: str) -> dict:
    with _db() as conn:
        # Sjekk om det allerede finnes en pending forespørsel
        existing = conn.execute(
            "SELECT id FROM access_requests WHERE user_id = ? AND product_id = ? AND status = 'pending'",
            (user_id, product_id),
        ).fetchone()
        if existing:
            return {"id": existing["id"], "status": "pending", "existing": True}
        req_id = str(uuid.uuid4())
        now    = datetime.now(tz=timezone.utc).isoformat()
        conn.execute(
            "INSERT INTO access_requests (id, user_id, product_id, status, requested_at) VALUES (?,?,?,?,?)",
            (req_id, user_id, product_id, "pending", now),
        )
        return {"id": req_id, "status": "pending", "existing": False}


def list_access_requests(status: Optional[str] = None) -> list[dict]:
    with _db() as conn:
        if status:
            rows = conn.execute(
                """SELECT ar.id, ar.product_id, ar.status, ar.requested_at, ar.resolved_at,
                          u.username, u.email
                   FROM access_requests ar JOIN users u ON ar.user_id = u.id
                   WHERE ar.status = ? ORDER BY ar.requested_at DESC""",
                (status,),
            ).fetchall()
        else:
            rows = conn.execute(
                """SELECT ar.id, ar.product_id, ar.status, ar.requested_at, ar.resolved_at,
                          u.username, u.email
                   FROM access_requests ar JOIN users u ON ar.user_id = u.id
                   ORDER BY ar.requested_at DESC""",
            ).fetchall()
        return [dict(r) for r in rows]


def subscribe(user_id: str, product_id: str) -> dict:
    """Registrer bruker som abonnent. INSERT OR IGNORE — idempotent."""
    sub_id = str(uuid.uuid4())
    now    = datetime.now(tz=timezone.utc).isoformat()
    with _db() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO subscriptions (id, user_id, product_id, created_at) VALUES (?,?,?,?)",
            (sub_id, user_id, product_id, now),
        )
        row = conn.execute(
            "SELECT id, user_id, product_id, created_at FROM subscriptions WHERE user_id = ? AND product_id = ?",
            (user_id, product_id),
        ).fetchone()
        return dict(row)


def unsubscribe(user_id: str, product_id: str) -> None:
    """Fjern bruker fra abonnentlisten."""
    with _db() as conn:
        conn.execute(
            "DELETE FROM subscriptions WHERE user_id = ? AND product_id = ?",
            (user_id, product_id),
        )


def list_subscribers(product_id: str) -> list[dict]:
    """Returner alle abonnenter for et produkt med brukerinfo."""
    with _db() as conn:
        rows = conn.execute(
            """SELECT s.user_id, u.username, u.email, s.created_at
               FROM subscriptions s JOIN users u ON s.user_id = u.id
               WHERE s.product_id = ?
               ORDER BY s.created_at""",
            (product_id,),
        ).fetchall()
        return [dict(r) for r in rows]


def is_subscribed(user_id: str, product_id: str) -> bool:
    """Sjekk om en bruker abonnerer på et produkt."""
    with _db() as conn:
        row = conn.execute(
            "SELECT 1 FROM subscriptions WHERE user_id = ? AND product_id = ?",
            (user_id, product_id),
        ).fetchone()
        return row is not None


def resolve_access_request(request_id: str, approved: bool, resolved_by: str) -> Optional[dict]:
    """Godkjenn eller avslå tilgangsforespørsel. Gir tilgang hvis godkjent."""
    with _db() as conn:
        row = conn.execute(
            "SELECT user_id, product_id, status FROM access_requests WHERE id = ?",
            (request_id,),
        ).fetchone()
        if not row or row["status"] != "pending":
            return None
        now    = datetime.now(tz=timezone.utc).isoformat()
        status = "approved" if approved else "rejected"
        conn.execute(
            "UPDATE access_requests SET status = ?, resolved_at = ?, resolved_by = ? WHERE id = ?",
            (status, now, resolved_by, request_id),
        )
        if approved:
            conn.execute(
                "INSERT OR IGNORE INTO user_products (user_id, product_id, granted_at) VALUES (?,?,?)",
                (row["user_id"], row["product_id"], now),
            )
        return {"id": request_id, "status": status}

# ── hjelpefunksjoner for request-kontekst ──────────────────────────────────────

def get_current_user(request) -> Optional[dict]:
    """Les innlogget bruker fra access_token-cookie. Returnerer None hvis ikke innlogget."""
    token = request.cookies.get("access_token")
    if not token:
        return None
    payload = decode_access_token(token)
    if not payload:
        return None
    user = {"id": payload["sub"], "username": payload["username"], "role": payload["role"]}
    # Slå opp persona og onboarding-status fra DB (bestemmer landingssidens innhold).
    try:
        with _db() as conn:
            row = conn.execute(
                "SELECT persona, onboarding_completed_at FROM users WHERE id = ?",
                (payload["sub"],),
            ).fetchone()
            if row:
                user["persona"] = row["persona"]
                user["onboarding_completed_at"] = row["onboarding_completed_at"]
    except Exception:
        user["persona"] = None
        user["onboarding_completed_at"] = None
    return user

# ── domeneprivilegier ──────────────────────────────────────────────────────────

def add_domain_membership(user_id: str, domain: str) -> None:
    with _db() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO domain_memberships (user_id, domain) VALUES (?,?)",
            (user_id, domain),
        )

def remove_domain_membership(user_id: str, domain: str) -> None:
    with _db() as conn:
        conn.execute(
            "DELETE FROM domain_memberships WHERE user_id = ? AND domain = ?",
            (user_id, domain),
        )

def get_user_domains(user_id: str) -> list[str]:
    with _db() as conn:
        rows = conn.execute(
            "SELECT domain FROM domain_memberships WHERE user_id = ?", (user_id,)
        ).fetchall()
        return [r["domain"] for r in rows]

def list_domain_members(domain: str) -> list[dict]:
    with _db() as conn:
        rows = conn.execute(
            """SELECT u.id, u.username, u.email, u.role
               FROM domain_memberships dm JOIN users u ON dm.user_id = u.id
               WHERE dm.domain = ?
               ORDER BY u.username""",
            (domain,),
        ).fetchall()
        return [dict(r) for r in rows]

def has_pii_access(user: dict | None) -> bool:
    """Admin-brukere og brukere med pii_access-rolle har tilgang til PII-kolonner."""
    if not user:
        return False
    return user.get("role") in ("admin", "pii_access")


# ── incidents ──────────────────────────────────────────────────────────────────

def create_incident(product_id: str, title: str, description: str = "",
                    severity: str = "warning", created_by: str = "system") -> dict:
    inc_id = str(uuid.uuid4())
    now    = datetime.now(tz=timezone.utc).isoformat()
    with _db() as conn:
        # Unngå duplikat åpne incidents for samme produkt og tittel
        existing = conn.execute(
            "SELECT id FROM incidents WHERE product_id = ? AND title = ? AND status != 'resolved'",
            (product_id, title),
        ).fetchone()
        if existing:
            return dict(conn.execute("SELECT * FROM incidents WHERE id = ?", (existing["id"],)).fetchone())
        conn.execute(
            """INSERT INTO incidents (id, product_id, title, description, status, severity,
               created_at, updated_at, created_by)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (inc_id, product_id, title, description, "open", severity, now, now, created_by),
        )
        return {"id": inc_id, "product_id": product_id, "title": title,
                "description": description, "status": "open", "severity": severity,
                "created_at": now, "updated_at": now, "created_by": created_by}


def list_incidents(product_id: str | None = None, status: str | None = None) -> list[dict]:
    with _db() as conn:
        query  = "SELECT * FROM incidents"
        params = []
        where  = []
        if product_id:
            where.append("product_id = ?")
            params.append(product_id)
        if status:
            where.append("status = ?")
            params.append(status)
        if where:
            query += " WHERE " + " AND ".join(where)
        query += " ORDER BY created_at DESC"
        return [dict(r) for r in conn.execute(query, params).fetchall()]


def track_usage(product_id: str, action: str, user_id: str | None = None) -> None:
    """Logg en brukshendelse for et dataprodukt. Mislykkes stille."""
    try:
        event_id = str(uuid.uuid4())
        now      = datetime.now(tz=timezone.utc).isoformat()
        with _db() as conn:
            conn.execute(
                "INSERT INTO usage_events (id, product_id, action, user_id, ts) VALUES (?,?,?,?,?)",
                (event_id, product_id, action, user_id, now),
            )
    except Exception:
        pass


def get_usage_counts(days: int = 30) -> list[dict]:
    """Returner topp-10 produkter etter antall visninger siste N dager."""
    cutoff = (datetime.now(tz=timezone.utc) - timedelta(days=days)).isoformat()
    with _db() as conn:
        rows = conn.execute(
            """SELECT product_id, COUNT(*) AS views
               FROM usage_events
               WHERE ts >= ? AND action = 'view'
               GROUP BY product_id
               ORDER BY views DESC
               LIMIT 10""",
            (cutoff,),
        ).fetchall()
        return [dict(r) for r in rows]


def get_product_views(product_id: str, days: int = 30) -> int:
    """Antall visninger for ett produkt siste N dager."""
    cutoff = (datetime.now(tz=timezone.utc) - timedelta(days=days)).isoformat()
    with _db() as conn:
        row = conn.execute(
            "SELECT COUNT(*) FROM usage_events WHERE product_id = ? AND action = 'view' AND ts >= ?",
            (product_id, cutoff),
        ).fetchone()
        return row[0] if row else 0


def update_incident(incident_id: str, status: str, updated_by: str) -> dict | None:
    valid = {"open", "acknowledged", "resolving", "resolved"}
    if status not in valid:
        return None
    now = datetime.now(tz=timezone.utc).isoformat()
    with _db() as conn:
        row = conn.execute("SELECT * FROM incidents WHERE id = ?", (incident_id,)).fetchone()
        if not row:
            return None
        resolved_at = now if status == "resolved" else row["resolved_at"]
        conn.execute(
            "UPDATE incidents SET status=?, updated_at=?, updated_by=?, resolved_at=? WHERE id=?",
            (status, now, updated_by, resolved_at, incident_id),
        )
        return dict(conn.execute("SELECT * FROM incidents WHERE id = ?", (incident_id,)).fetchone())


# ── GUIDE-1/4: persona og onboarding ────────────────────────────────────────────

# Onboarding-steg som vises på /getting-started. `predicate` evaluerer mot
# eksisterende data i databasen — vi unngår å duplisere state.
ONBOARDING_STEPS = [
    {
        "key":         "set_persona",
        "title":       "Velg din rolle",
        "description": "Tilpass forsiden basert på om du er analytiker, dataingeniør eller domeneeier.",
        "url":         "/getting-started?focus=persona",
    },
    {
        "key":         "view_product",
        "title":       "Åpne et dataprodukt",
        "description": "Klikk inn på et produkt fra katalogen for å se schema, lineage og kvalitet.",
        "url":         "/",
    },
    {
        "key":         "open_glossary",
        "title":       "Bli kjent med plattformkonseptene",
        "description": "Bla i glossaret for forklaring av Medallion, Data Mesh, IDP og mer.",
        "url":         "/glossary",
    },
    {
        "key":         "subscribe",
        "title":       "Abonner på et produkt",
        "description": "Få varsel når et produkt får schema- eller SLO-endringer.",
        "url":         "/",
    },
    {
        "key":         "generate_notebook",
        "title":       "Generer en notebook",
        "description": "Lag et utgangspunkt for analyse i Jupyter via «Åpne i Jupyter».",
        "url":         "/",
    },
]


def set_persona(user_id: str, persona: str) -> bool:
    if persona not in PERSONAS:
        return False
    with _db() as conn:
        conn.execute("UPDATE users SET persona = ? WHERE id = ?", (persona, user_id))
    return True


def get_persona(user_id: str) -> str | None:
    with _db() as conn:
        row = conn.execute("SELECT persona FROM users WHERE id = ?", (user_id,)).fetchone()
    return (row["persona"] if row else None) or None


def get_onboarding_progress(user_id: str) -> dict:
    """Returnér {step_key: bool} basert på state i databasen.

    Stegene er fullført hvis:
      set_persona       — users.persona er satt
      view_product      — minst én usage_events.action='view' for brukeren
      open_glossary     — minst én usage_events.action='view_glossary'
      subscribe         — minst én rad i subscriptions
      generate_notebook — minst én usage_events.action='generate_notebook'
    """
    with _db() as conn:
        user_row = conn.execute(
            "SELECT persona, onboarding_completed_at FROM users WHERE id = ?",
            (user_id,),
        ).fetchone()
        if not user_row:
            return {s["key"]: False for s in ONBOARDING_STEPS}

        persona = user_row["persona"]

        def _has_event(action: str) -> bool:
            row = conn.execute(
                "SELECT 1 FROM usage_events WHERE user_id = ? AND action = ? LIMIT 1",
                (user_id, action),
            ).fetchone()
            return bool(row)

        sub_row = conn.execute(
            "SELECT 1 FROM subscriptions WHERE user_id = ? LIMIT 1", (user_id,)
        ).fetchone()

        return {
            "set_persona":       bool(persona),
            "view_product":      _has_event("view"),
            "open_glossary":     _has_event("view_glossary"),
            "subscribe":         bool(sub_row),
            "generate_notebook": _has_event("generate_notebook"),
        }


def mark_onboarding_completed(user_id: str) -> None:
    """Skriver tidsstempel — UI kan bruke dette til å skjule velkomstmodal."""
    now = datetime.now(tz=timezone.utc).isoformat()
    with _db() as conn:
        conn.execute(
            "UPDATE users SET onboarding_completed_at = ? WHERE id = ?",
            (now, user_id),
        )


def is_onboarding_completed(user_id: str) -> bool:
    with _db() as conn:
        row = conn.execute(
            "SELECT onboarding_completed_at FROM users WHERE id = ?", (user_id,)
        ).fetchone()
    return bool(row and row["onboarding_completed_at"])
