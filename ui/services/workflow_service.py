import os

import psycopg2
import psycopg2.extras


def get_db_connection():
    return psycopg2.connect(
        host=os.getenv('POSTGRES_HOST', 'postgres'),
        port=os.getenv('POSTGRES_PORT', 5432),
        database=os.getenv('POSTGRES_DB', 'eventdb'),
        user=os.getenv('POSTGRES_USER', 'eventuser'),
        password=os.getenv('POSTGRES_PASSWORD', 'eventpass'),
        cursor_factory=psycopg2.extras.RealDictCursor,
    )


def get_workflow_executions(page=1, per_page=50, status=None, date_from=None, date_to=None, search=None):
    filters = []
    params = []

    if status:
        filters.append("status = %s")
        params.append(status)
    if date_from:
        filters.append("started_at >= %s")
        params.append(date_from)
    if date_to:
        filters.append("started_at <= %s")
        params.append(date_to)
    if search:
        filters.append("name ILIKE %s")
        params.append(f"%{search}%")

    where = ("WHERE " + " AND ".join(filters)) if filters else ""
    offset = (page - 1) * per_page

    query = f"""
        SELECT id, name, status, started_at, completed_at,
               EXTRACT(EPOCH FROM duration)::int AS duration_seconds
        FROM workflow_executions
        {where}
        ORDER BY started_at DESC
        LIMIT %s OFFSET %s
    """
    params.extend([per_page, offset])

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(query, params)
            return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()


def get_workflow_execution_count(status=None, date_from=None, date_to=None, search=None):
    filters = []
    params = []

    if status:
        filters.append("status = %s")
        params.append(status)
    if date_from:
        filters.append("started_at >= %s")
        params.append(date_from)
    if date_to:
        filters.append("started_at <= %s")
        params.append(date_to)
    if search:
        filters.append("name ILIKE %s")
        params.append(f"%{search}%")

    where = ("WHERE " + " AND ".join(filters)) if filters else ""
    query = f"SELECT COUNT(*) AS total FROM workflow_executions {where}"

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(query, params)
            return cur.fetchone()['total']
    finally:
        conn.close()


def get_workflow_execution(execution_id):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, name, status, started_at, completed_at,
                       EXTRACT(EPOCH FROM duration)::int AS duration_seconds
                FROM workflow_executions
                WHERE id = %s
                """,
                (execution_id,),
            )
            row = cur.fetchone()
            return dict(row) if row else None
    finally:
        conn.close()


def get_workflow_steps(execution_id):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, workflow_execution_id, name, step_type, status,
                       started_at, completed_at,
                       EXTRACT(EPOCH FROM duration)::int AS duration_seconds,
                       error_message, sequence_order
                FROM workflow_steps
                WHERE workflow_execution_id = %s
                ORDER BY sequence_order
                """,
                (execution_id,),
            )
            return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()


def get_recent_status_changes(since_id=None):
    """Return executions updated after a given row for SSE streaming."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, name, status,
                       EXTRACT(EPOCH FROM duration)::int AS duration_seconds
                FROM workflow_executions
                ORDER BY started_at DESC
                LIMIT 20
                """,
            )
            return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()
