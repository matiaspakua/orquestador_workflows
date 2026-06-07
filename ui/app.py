import os
import json
import time
import psycopg2
import psycopg2.extras
from psycopg2 import OperationalError as PgOperationalError
from flask import Flask, render_template, jsonify, request, Response, stream_with_context

from services import workflow_service

app = Flask(__name__)

_grpc_server = None
try:
    import sys
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
    from common.grpc_service import start_grpc_server
    _grpc_server = start_grpc_server(workflow_service)
except Exception as e:
    app.logger.warning(f"gRPC server not started: {e}")


def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", 5432)),
        database=os.getenv("POSTGRES_DB", "eventdb"),
        user=os.getenv("POSTGRES_USER", "eventuser"),
        password=os.getenv("POSTGRES_PASSWORD", "eventpass"),
    )


@app.route("/health")
def health():
    return jsonify({"status": "healthy", "service": "web-ui"}), 200


@app.route("/ready")
def ready():
    try:
        conn = get_db_connection()
        conn.close()
        return jsonify({"status": "ready", "database": "connected"}), 200
    except Exception as e:
        return jsonify({"status": "not_ready", "database": str(e)}), 503


@app.route("/")
def dashboard():
    return render_template("index.html")


@app.route("/api/stats")
def get_stats():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT
                COUNT(*) as total_events,
                COUNT(CASE WHEN status = 'PENDING' THEN 1 END) as pending,
                COUNT(CASE WHEN status = 'PROCESSED' THEN 1 END) as processed,
                COUNT(CASE WHEN status = 'FAILED' THEN 1 END) as failed,
                COUNT(CASE WHEN status = 'ERROR' THEN 1 END) as errors
            FROM event_data
        """)
        stats = cursor.fetchone()

        cursor.execute("""
            SELECT data_type, status, COUNT(*) as count
            FROM event_data
            GROUP BY data_type, status
            ORDER BY data_type, status
        """)
        event_types = {}
        for row in cursor.fetchall():
            data_type, status, count = row
            if data_type not in event_types:
                event_types[data_type] = {}
            event_types[data_type][status] = count

        cursor.execute("""
            SELECT ed.data_type, ed.status, ed.created_at, ed.processed_at, ed.processed_by
            FROM event_data ed
            ORDER BY ed.created_at DESC
            LIMIT 10
        """)
        recent_events = []
        for row in cursor.fetchall():
            recent_events.append({
                "data_type": row[0],
                "status": row[1],
                "created_at": row[2].isoformat() if row[2] else None,
                "processed_at": row[3].isoformat() if row[3] else None,
                "processed_by": row[4],
            })

        cursor.execute("""
            SELECT el.event_id, el.consumer_id, el.status, el.processed_at,
                   el.processing_time_ms, el.error_message
            FROM event_logs el
            ORDER BY el.processed_at DESC
            LIMIT 10
        """)
        recent_logs = []
        for row in cursor.fetchall():
            recent_logs.append({
                "event_id": row[0],
                "consumer_id": row[1],
                "status": row[2],
                "processed_at": row[3].isoformat() if row[3] else None,
                "processing_time_ms": row[4],
                "error_message": row[5],
            })

        cursor.execute("""
            SELECT
                AVG(processing_time_ms) as avg_processing_time,
                MAX(processing_time_ms) as max_processing_time,
                COUNT(*) as total_processed,
                COUNT(CASE WHEN status = 'SUCCESS' THEN 1 END) as successful,
                COUNT(CASE WHEN status = 'ERROR' THEN 1 END) as errors
            FROM event_logs
            WHERE processed_at >= NOW() - INTERVAL '1 hour'
        """)
        performance = cursor.fetchone()

        conn.close()

        return jsonify({
            "general_stats": {
                "total_events": stats[0],
                "pending": stats[1],
                "processed": stats[2],
                "failed": stats[3],
                "errors": stats[4],
            },
            "event_types": event_types,
            "recent_events": recent_events,
            "recent_logs": recent_logs,
            "performance": {
                "avg_processing_time_ms": float(performance[0]) if performance[0] else 0,
                "max_processing_time_ms": performance[1] if performance[1] else 0,
                "total_processed": performance[2],
                "successful": performance[3],
                "errors": performance[4],
            },
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/events/<event_type>")
def get_events_by_type(event_type):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT id, payload, status, created_at, processed_at, processed_by
            FROM event_data
            WHERE data_type = %s
            ORDER BY created_at DESC
            LIMIT 50
        """, (event_type,))

        events = []
        for row in cursor.fetchall():
            events.append({
                "id": row[0],
                "payload": json.loads(row[1]) if row[1] else {},
                "status": row[2],
                "created_at": row[3].isoformat() if row[3] else None,
                "processed_at": row[4].isoformat() if row[4] else None,
                "processed_by": row[5],
            })

        conn.close()
        return jsonify(events)

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/consumers")
def get_consumer_stats():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT
                consumer_id,
                COUNT(*) as total_processed,
                COUNT(CASE WHEN status = 'SUCCESS' THEN 1 END) as successful,
                COUNT(CASE WHEN status = 'ERROR' THEN 1 END) as errors,
                AVG(processing_time_ms) as avg_processing_time,
                MAX(processed_at) as last_activity
            FROM event_logs
            GROUP BY consumer_id
            ORDER BY total_processed DESC
        """)

        consumers = []
        for row in cursor.fetchall():
            consumers.append({
                "consumer_id": row[0],
                "total_processed": row[1],
                "successful": row[2],
                "errors": row[3],
                "avg_processing_time_ms": float(row[4]) if row[4] else 0,
                "last_activity": row[5].isoformat() if row[5] else None,
            })

        conn.close()
        return jsonify(consumers)

    except Exception as e:
        return jsonify({"error": str(e)}), 500


PER_PAGE = 50


@app.route("/workflows")
def workflow_list():
    page = max(1, int(request.args.get("page", 1)))
    status = request.args.get("status", "")
    date_from = request.args.get("date_from", "")
    date_to = request.args.get("date_to", "")
    search = request.args.get("search", "")

    filters = dict(
        status=status or None,
        date_from=date_from or None,
        date_to=date_to or None,
        search=search or None,
    )

    db_error = False
    try:
        executions = workflow_service.get_workflow_executions(page=page, per_page=PER_PAGE, **filters)
        total = workflow_service.get_workflow_execution_count(**filters)
    except PgOperationalError:
        executions = []
        total = 0
        db_error = True
    except Exception:
        executions = []
        total = 0

    total_pages = max(1, -(-total // PER_PAGE))

    return render_template(
        "workflow_list.html",
        executions=executions,
        page=page,
        total_pages=total_pages,
        total=total,
        status=status,
        date_from=date_from,
        date_to=date_to,
        search=search,
        db_error=db_error,
    )


@app.route("/workflows/<execution_id>")
def workflow_detail(execution_id):
    try:
        execution = workflow_service.get_workflow_execution(execution_id)
        steps = workflow_service.get_workflow_steps(execution_id) if execution else []
    except PgOperationalError:
        return render_template("workflow_detail.html", execution=None, steps=[], db_error=True), 503
    except Exception:
        execution = None
        steps = []

    if execution is None:
        return render_template("workflow_detail.html", execution=None, steps=[]), 404

    return render_template("workflow_detail.html", execution=execution, steps=steps, db_error=False)


@app.route("/api/workflows")
def api_workflow_list():
    page = max(1, int(request.args.get("page", 1)))
    per_page = min(200, int(request.args.get("per_page", PER_PAGE)))
    filters = dict(
        status=request.args.get("status") or None,
        date_from=request.args.get("date_from") or None,
        date_to=request.args.get("date_to") or None,
        search=request.args.get("search") or None,
    )
    try:
        executions = workflow_service.get_workflow_executions(page=page, per_page=per_page, **filters)
        total = workflow_service.get_workflow_execution_count(**filters)
    except PgOperationalError:
        return jsonify({"error": "base_datos_no_disponible", "message": "No se pudo conectar con la base de datos"}), 503
    except Exception:
        return jsonify({"error": "Error al consultar ejecuciones"}), 500

    response = jsonify({"executions": executions, "total": total, "page": page, "per_page": per_page})
    response.headers["X-Total-Count"] = total
    return response


@app.route("/api/workflows/<execution_id>")
def api_workflow_detail(execution_id):
    try:
        execution = workflow_service.get_workflow_execution(execution_id)
        if execution is None:
            return jsonify({"error": "No encontrado"}), 404
        steps = workflow_service.get_workflow_steps(execution_id)
        return jsonify({"execution": execution, "steps": steps})
    except PgOperationalError:
        return jsonify({"error": "base_datos_no_disponible", "message": "No se pudo conectar con la base de datos"}), 503
    except Exception:
        return jsonify({"error": "Error al consultar detalle"}), 500


@app.route("/api/workflows/stream")
def workflow_stream():
    def event_stream():
        while True:
            try:
                rows = workflow_service.get_recent_status_changes()
                data = json.dumps(rows, default=str)
                yield f"event: status_update\ndata: {data}\n\n"
            except Exception:
                yield "event: error\ndata: {{}}\n\n"
            time.sleep(4)

    return Response(
        stream_with_context(event_stream()),
        content_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


if __name__ == "__main__":
    port = int(os.getenv("FLASK_PORT", "5000"))
    debug = os.getenv("FLASK_DEBUG", "0") == "1"
    app.run(host="0.0.0.0", port=port, debug=debug)
