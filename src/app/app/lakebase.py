"""
Lakebase (managed PostgreSQL) operational data layer for the CDP app.
Handles campaign state, NBA assignments, alert triage, and suppression lists.
Uses OAuth token-based authentication with automatic refresh.
"""

import asyncio
import logging
import os
import time
import uuid
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

_INSTANCE_NAME = os.environ.get("LAKEBASE_INSTANCE_NAME", "cdp-360-ops")
_DATABASE = os.environ.get("LAKEBASE_DATABASE_NAME", "databricks_postgres")

_pool = None
_token: str | None = None
_token_ts: float = 0
_TOKEN_TTL = 50 * 60  # refresh every 50 min (tokens expire at 60)


def _refresh_token() -> str:
    global _token, _token_ts
    now = time.time()
    if _token and (now - _token_ts) < _TOKEN_TTL:
        return _token
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    cred = w.database.generate_database_credential(
        request_id=str(uuid.uuid4()),
        instance_names=[_INSTANCE_NAME],
    )
    _token = cred.token
    _token_ts = now
    logger.info("Lakebase token refreshed for %s", _INSTANCE_NAME)
    return _token


def _get_conn_params() -> dict:
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    inst = w.database.get_database_instance(name=_INSTANCE_NAME)
    token = _refresh_token()
    user = w.current_user.me().user_name
    return {
        "host": inst.read_write_dns,
        "port": 5432,
        "dbname": _DATABASE,
        "user": user,
        "password": token,
        "sslmode": "require",
    }


def _conn():
    """Get or create a psycopg connection (sync)."""
    import psycopg
    params = _get_conn_params()
    return psycopg.connect(**params, autocommit=True)


def _execute(sql: str, params: tuple = ()) -> list[dict]:
    """Execute a SQL statement and return rows as dicts."""
    import psycopg
    conn_params = _get_conn_params()
    with psycopg.connect(**conn_params, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            if cur.description:
                cols = [d.name for d in cur.description]
                return [dict(zip(cols, row)) for row in cur.fetchall()]
            return []


def _execute_one(sql: str, params: tuple = ()) -> dict | None:
    rows = _execute(sql, params)
    return rows[0] if rows else None


# ── Schema Bootstrap ─────────────────────────────────────────────

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS campaigns (
    campaign_id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
    name TEXT NOT NULL,
    segment TEXT NOT NULL,
    action_type TEXT NOT NULL,
    channel TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'draft',
    owner TEXT,
    scheduled_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    notes TEXT
);

CREATE TABLE IF NOT EXISTS campaign_enrollments (
    enrollment_id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
    campaign_id TEXT NOT NULL REFERENCES campaigns(campaign_id) ON DELETE CASCADE,
    golden_id TEXT NOT NULL,
    merchant_name TEXT,
    status TEXT NOT NULL DEFAULT 'queued',
    sent_at TIMESTAMPTZ,
    outcome TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_enroll_campaign ON campaign_enrollments(campaign_id);
CREATE INDEX IF NOT EXISTS idx_enroll_merchant ON campaign_enrollments(golden_id);

CREATE TABLE IF NOT EXISTS nba_assignments (
    assignment_id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
    golden_id TEXT NOT NULL,
    merchant_name TEXT,
    action_type TEXT NOT NULL,
    channel TEXT,
    assignee TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    due_date DATE,
    priority_score DOUBLE PRECISION DEFAULT 0,
    revenue_impact DOUBLE PRECISION DEFAULT 0,
    notes TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_assign_assignee ON nba_assignments(assignee);
CREATE INDEX IF NOT EXISTS idx_assign_status ON nba_assignments(status);

CREATE TABLE IF NOT EXISTS alert_triage (
    triage_id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
    golden_id TEXT NOT NULL,
    merchant_name TEXT,
    anomaly_type TEXT NOT NULL,
    resolution TEXT NOT NULL DEFAULT 'open',
    notes TEXT,
    triaged_by TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resolved_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_triage_status ON alert_triage(resolution);

CREATE TABLE IF NOT EXISTS suppression_list (
    suppression_id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
    golden_id TEXT NOT NULL,
    reason TEXT NOT NULL,
    channel TEXT,
    expires_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_suppress_merchant ON suppression_list(golden_id);
"""


def bootstrap_schema():
    """Create operational tables if they don't exist."""
    try:
        import psycopg
        conn_params = _get_conn_params()
        with psycopg.connect(**conn_params, autocommit=True) as conn:
            with conn.cursor() as cur:
                for stmt in _SCHEMA_SQL.split(";"):
                    stmt = stmt.strip()
                    if stmt:
                        cur.execute(stmt)
        logger.info("Lakebase schema bootstrapped")
        return True
    except Exception as e:
        logger.error("Lakebase schema bootstrap failed: %s", e)
        return False


# ── Campaign Operations ──────────────────────────────────────────

def create_campaign(
    name: str, segment: str, action_type: str, channel: str,
    owner: str | None = None, scheduled_at: str | None = None,
    merchant_ids: list[dict] | None = None, notes: str | None = None,
) -> dict:
    """Create a campaign and optionally enroll merchants."""
    import psycopg
    conn_params = _get_conn_params()
    campaign_id = str(uuid.uuid4())
    with psycopg.connect(**conn_params) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO campaigns (campaign_id, name, segment, action_type, channel, status, owner, scheduled_at, notes)
                   VALUES (%s, %s, %s, %s, %s, 'scheduled', %s, %s, %s)
                   RETURNING *""",
                (campaign_id, name, segment, action_type, channel, owner, scheduled_at, notes),
            )
            campaign = dict(zip([d.name for d in cur.description], cur.fetchone()))

            enrolled = 0
            if merchant_ids:
                for m in merchant_ids:
                    gid = m.get("golden_id", "") if isinstance(m, dict) else str(m)
                    mname = m.get("merchant_name", "") if isinstance(m, dict) else ""
                    cur.execute(
                        """INSERT INTO campaign_enrollments (campaign_id, golden_id, merchant_name, status)
                           VALUES (%s, %s, %s, 'queued')""",
                        (campaign_id, gid, mname),
                    )
                    enrolled += 1
        conn.commit()
    campaign["merchants_enrolled"] = enrolled
    return campaign


def get_campaigns(status: str = "", limit: int = 50) -> list[dict]:
    where = "WHERE status = %s" if status else ""
    params = (status,) if status else ()
    sql = f"""
        SELECT c.*, COUNT(e.enrollment_id) AS merchants_enrolled
        FROM campaigns c
        LEFT JOIN campaign_enrollments e ON c.campaign_id = e.campaign_id
        {where}
        GROUP BY c.campaign_id
        ORDER BY c.created_at DESC
        LIMIT {min(int(limit), 200)}
    """
    return _execute(sql, params)


def get_campaign_detail(campaign_id: str) -> dict | None:
    campaign = _execute_one("SELECT * FROM campaigns WHERE campaign_id = %s", (campaign_id,))
    if not campaign:
        return None
    enrollments = _execute(
        "SELECT * FROM campaign_enrollments WHERE campaign_id = %s ORDER BY created_at",
        (campaign_id,),
    )
    campaign["enrollments"] = enrollments
    campaign["merchants_enrolled"] = len(enrollments)
    return campaign


def update_campaign_status(campaign_id: str, status: str) -> dict | None:
    return _execute_one(
        "UPDATE campaigns SET status = %s, updated_at = NOW() WHERE campaign_id = %s RETURNING *",
        (status, campaign_id),
    )


# ── NBA Assignments ──────────────────────────────────────────────

def create_assignment(
    golden_id: str, merchant_name: str, action_type: str, channel: str,
    assignee: str, due_date: str | None = None,
    priority_score: float = 0, revenue_impact: float = 0, notes: str | None = None,
) -> dict:
    row = _execute_one(
        """INSERT INTO nba_assignments
           (golden_id, merchant_name, action_type, channel, assignee, due_date, priority_score, revenue_impact, notes)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
           RETURNING *""",
        (golden_id, merchant_name, action_type, channel, assignee, due_date, priority_score, revenue_impact, notes),
    )
    return row or {}


def get_assignments(assignee: str = "", status: str = "", limit: int = 50) -> list[dict]:
    conditions = []
    params: list = []
    if assignee:
        conditions.append("assignee = %s")
        params.append(assignee)
    if status:
        conditions.append("status = %s")
        params.append(status)
    where = "WHERE " + " AND ".join(conditions) if conditions else ""
    return _execute(
        f"SELECT * FROM nba_assignments {where} ORDER BY priority_score DESC, created_at DESC LIMIT {min(int(limit), 200)}",
        tuple(params),
    )


def update_assignment_status(assignment_id: str, status: str, notes: str | None = None) -> dict | None:
    completed_at = "NOW()" if status in ("completed", "deferred") else "NULL"
    return _execute_one(
        f"""UPDATE nba_assignments
            SET status = %s, notes = COALESCE(%s, notes), completed_at = {completed_at}
            WHERE assignment_id = %s RETURNING *""",
        (status, notes, assignment_id),
    )


# ── Alert Triage ─────────────────────────────────────────────────

def triage_alert(
    golden_id: str, merchant_name: str, anomaly_type: str,
    resolution: str, triaged_by: str | None = None, notes: str | None = None,
) -> dict:
    resolved_at = "NOW()" if resolution != "investigating" else None
    row = _execute_one(
        """INSERT INTO alert_triage (golden_id, merchant_name, anomaly_type, resolution, triaged_by, notes, resolved_at)
           VALUES (%s, %s, %s, %s, %s, %s, %s)
           RETURNING *""",
        (golden_id, merchant_name, anomaly_type, resolution, triaged_by, notes, resolved_at),
    )
    return row or {}


def get_triage_log(resolution: str = "", limit: int = 50) -> list[dict]:
    where = "WHERE resolution = %s" if resolution else ""
    params = (resolution,) if resolution else ()
    return _execute(
        f"SELECT * FROM alert_triage {where} ORDER BY created_at DESC LIMIT {min(int(limit), 200)}",
        params,
    )


def get_triage_kpis() -> dict:
    row = _execute_one("""
        SELECT
          COUNT(*) AS total_triaged,
          COUNT(*) FILTER (WHERE resolution = 'investigating') AS investigating,
          COUNT(*) FILTER (WHERE resolution = 'resolved') AS resolved,
          COUNT(*) FILTER (WHERE resolution = 'false_positive') AS false_positive,
          COUNT(*) FILTER (WHERE resolution = 'escalated') AS escalated,
          COUNT(*) FILTER (WHERE resolution = 'open') AS open_alerts
        FROM alert_triage
    """)
    return row or {"total_triaged": 0, "investigating": 0, "resolved": 0, "false_positive": 0, "escalated": 0, "open_alerts": 0}


# ── Suppression List ─────────────────────────────────────────────

def check_suppression(golden_ids: list[str]) -> list[str]:
    """Return merchant IDs that are currently suppressed."""
    if not golden_ids:
        return []
    placeholders = ",".join(["%s"] * len(golden_ids))
    rows = _execute(
        f"SELECT DISTINCT golden_id FROM suppression_list WHERE golden_id IN ({placeholders}) AND (expires_at IS NULL OR expires_at > NOW())",
        tuple(golden_ids),
    )
    return [r["golden_id"] for r in rows]


def add_suppression(golden_id: str, reason: str, channel: str | None = None, expires_hours: int = 168) -> dict:
    row = _execute_one(
        """INSERT INTO suppression_list (golden_id, reason, channel, expires_at)
           VALUES (%s, %s, %s, NOW() + INTERVAL '%s hours')
           RETURNING *""",
        (golden_id, reason, channel, expires_hours),
    )
    return row or {}


# ── Ops KPIs (Aggregate) ────────────────────────────────────────

def get_ops_kpis() -> dict:
    """Aggregate operational KPIs across campaigns, assignments, and triage."""
    campaigns = _execute_one("""
        SELECT COUNT(*) AS total, COUNT(*) FILTER (WHERE status = 'scheduled') AS scheduled,
               COUNT(*) FILTER (WHERE status = 'executing') AS executing,
               COUNT(*) FILTER (WHERE status = 'completed') AS completed
        FROM campaigns
    """) or {}
    assignments = _execute_one("""
        SELECT COUNT(*) AS total, COUNT(*) FILTER (WHERE status = 'pending') AS pending,
               COUNT(*) FILTER (WHERE status = 'in_progress') AS in_progress,
               COUNT(*) FILTER (WHERE status = 'completed') AS completed,
               COUNT(*) FILTER (WHERE status = 'overdue') AS overdue
        FROM nba_assignments
    """) or {}
    triage = get_triage_kpis()
    return {"campaigns": campaigns, "assignments": assignments, "triage": triage}
