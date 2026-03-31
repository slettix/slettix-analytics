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
        """)
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
    return {"id": payload["sub"], "username": payload["username"], "role": payload["role"]}
