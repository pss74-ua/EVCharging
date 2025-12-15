CREATE TABLE audit_logs (
    id INT AUTO_INCREMENT PRIMARY KEY,
    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
    source_ip VARCHAR(45) NOT NULL,      -- Desde dónde (IP)
    entity_id VARCHAR(50),               -- Quién (CP0, Driver1, Admin)
    event_type VARCHAR(50) NOT NULL,     -- Qué acción (AUTH, ERROR, STATUS, etc.)
    details TEXT                         -- Parámetros o descripción estructurada
);