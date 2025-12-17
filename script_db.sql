-- 1. Crear la base de datos
CREATE DATABASE IF NOT EXISTS ev_charging;

USE ev_charging;

-- 2. Crear la tabla de Puntos de Recarga
CREATE TABLE IF NOT EXISTS charge_points (
    cp_id VARCHAR(10) PRIMARY KEY, -- Identificador único (Ej: CP1)
    location VARCHAR(255) NOT NULL,
    price_kwh DECIMAL(4, 2) NOT NULL, -- Precio por kWh (Ej: 0.54)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_registered BOOLEAN DEFAULT FALSE,
    symmetric_key VARCHAR(100) NULL,
    status VARCHAR(20) DEFAULT 'DISCONNECTED',
    weather_temperature DECIMAL(4, 1) NULL DEFAULT NULL
);

-- 3. Crear la tabla de auditoría de eventos
CREATE TABLE IF NOT EXISTS audit_logs (
    id INT AUTO_INCREMENT PRIMARY KEY,
    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
    source_ip VARCHAR(45) NOT NULL,      -- Desde dónde (IP)
    entity_id VARCHAR(50),               -- Quién (CP0, Driver1, Admin)
    event_type VARCHAR(50) NOT NULL,     -- Qué acción (AUTH, ERROR, STATUS, etc.)
    details TEXT                         -- Parámetros o descripción estructurada
);

-- 4. Insertar los datos de EVCharging/data.json
INSERT IGNORE INTO charge_points (cp_id, location, price_kwh) VALUES
('CP1', 'Madrid,ES', 0.54),
('CP2', 'Barcelona,ES', 0.56),
('CP3', 'Valencia,ES', 0.54),
('CP4', 'Bilbao,ES', 0.52),
('CP5', 'Malaga,ES', 0.58),
('CP6', 'Sevilla,ES', 0.46),
('CP7', 'Madrid,ES', 0.60),
('CP8', 'Barcelona,ES', 0.48),
('CP9', 'Zaragoza,ES', 0.50),
('CP10', 'Madrid,ES', 0.52),
('CP11', 'Granada,ES', 0.42),
('CP12', 'Sevilla,ES', 0.40),
('CP13', 'Alicante,ES', 0.54),
('CP14', 'Valencia,ES', 0.48),
('CP15', 'Sevilla,ES', 0.46),
('CP16', 'Murcia,ES', 0.48),
('CP17', 'Malaga,ES', 0.50),
('CP18', 'Madrid,ES', 0.52),
('CP19', 'Zaragoza,ES', 0.54),
('CP20', 'Barcelona,ES', 0.56);