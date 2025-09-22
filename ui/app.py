import os
import json
import psycopg2
from datetime import datetime, timedelta
from flask import Flask, render_template, jsonify

app = Flask(__name__)

def get_db_connection():
    """Obtiene una conexión a la base de datos PostgreSQL"""
    return psycopg2.connect(
        host=os.getenv('POSTGRES_HOST', 'postgres'),
        port=os.getenv('POSTGRES_PORT', 5432),
        database=os.getenv('POSTGRES_DB', 'eventdb'),
        user=os.getenv('POSTGRES_USER', 'eventuser'),
        password=os.getenv('POSTGRES_PASSWORD', 'eventpass')
    )

@app.route('/')
def dashboard():
    """Página principal del dashboard"""
    return render_template('index.html')

@app.route('/api/stats')
def get_stats():
    """API para obtener estadísticas del sistema"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Estadísticas generales
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
        
        # Estadísticas por tipo de evento
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
        
        # Eventos recientes
        cursor.execute("""
            SELECT ed.data_type, ed.status, ed.created_at, ed.processed_at, ed.processed_by
            FROM event_data ed
            ORDER BY ed.created_at DESC
            LIMIT 10
        """)
        recent_events = []
        for row in cursor.fetchall():
            recent_events.append({
                'data_type': row[0],
                'status': row[1],
                'created_at': row[2].isoformat() if row[2] else None,
                'processed_at': row[3].isoformat() if row[3] else None,
                'processed_by': row[4]
            })
        
        # Logs de procesamiento recientes
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
                'event_id': row[0],
                'consumer_id': row[1],
                'status': row[2],
                'processed_at': row[3].isoformat() if row[3] else None,
                'processing_time_ms': row[4],
                'error_message': row[5]
            })
        
        # Métricas de rendimiento
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
            'general_stats': {
                'total_events': stats[0],
                'pending': stats[1],
                'processed': stats[2],
                'failed': stats[3],
                'errors': stats[4]
            },
            'event_types': event_types,
            'recent_events': recent_events,
            'recent_logs': recent_logs,
            'performance': {
                'avg_processing_time_ms': float(performance[0]) if performance[0] else 0,
                'max_processing_time_ms': performance[1] if performance[1] else 0,
                'total_processed': performance[2],
                'successful': performance[3],
                'errors': performance[4]
            }
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/events/<event_type>')
def get_events_by_type(event_type):
    """API para obtener eventos por tipo"""
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
                'id': row[0],
                'payload': json.loads(row[1]) if row[1] else {},
                'status': row[2],
                'created_at': row[3].isoformat() if row[3] else None,
                'processed_at': row[4].isoformat() if row[4] else None,
                'processed_by': row[5]
            })
        
        conn.close()
        return jsonify(events)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/consumers')
def get_consumer_stats():
    """API para obtener estadísticas de consumidores"""
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
                'consumer_id': row[0],
                'total_processed': row[1],
                'successful': row[2],
                'errors': row[3],
                'avg_processing_time_ms': float(row[4]) if row[4] else 0,
                'last_activity': row[5].isoformat() if row[5] else None
            })
        
        conn.close()
        return jsonify(consumers)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)