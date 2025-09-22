-- Inicialización de la base de datos para el sistema event-driven

-- Tabla para almacenar los datos reales (payload)
CREATE TABLE IF NOT EXISTS event_data (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    data_type VARCHAR(50) NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    status VARCHAR(20) DEFAULT 'PENDING',
    processed_at TIMESTAMP WITH TIME ZONE,
    processed_by VARCHAR(50)
);

-- Tabla para logs de eventos procesados
CREATE TABLE IF NOT EXISTS event_logs (
    id SERIAL PRIMARY KEY,
    event_id VARCHAR(100) NOT NULL,
    consumer_id VARCHAR(50) NOT NULL,
    data_reference_id UUID REFERENCES event_data(id),
    status VARCHAR(20) NOT NULL,
    processed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    processing_time_ms INTEGER,
    error_message TEXT
);

-- Tabla para métricas del sistema
CREATE TABLE IF NOT EXISTS system_metrics (
    id SERIAL PRIMARY KEY,
    metric_name VARCHAR(50) NOT NULL,
    metric_value INTEGER NOT NULL,
    recorded_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Índices para optimizar consultas
CREATE INDEX IF NOT EXISTS idx_event_data_status ON event_data(status);
CREATE INDEX IF NOT EXISTS idx_event_data_created_at ON event_data(created_at);
CREATE INDEX IF NOT EXISTS idx_event_logs_processed_at ON event_logs(processed_at);

-- Datos de prueba inicial
INSERT INTO event_data (data_type, payload, status) VALUES
('order', '{"customer_id": 1001, "amount": 150.50, "items": ["item1", "item2"]}', 'PENDING'),
('user_registration', '{"email": "test@example.com", "username": "testuser"}', 'PENDING'),
('payment', '{"transaction_id": "txn_123", "amount": 99.99, "currency": "USD"}', 'PENDING');

-- Vista para estadísticas rápidas
CREATE OR REPLACE VIEW event_stats AS
SELECT 
    data_type,
    status,
    COUNT(*) as count,
    MAX(created_at) as last_created,
    AVG(EXTRACT(EPOCH FROM (processed_at - created_at))) as avg_processing_seconds
FROM event_data
GROUP BY data_type, status;