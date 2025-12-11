-- 1. Crear la base de datos
CREATE DATABASE IF NOT EXISTS ev_charging;

USE ev_charging;

-- 2. Crear la tabla de Puntos de Recarga
CREATE TABLE IF NOT EXISTS charge_points (
    cp_id VARCHAR(10) PRIMARY KEY, -- Identificador único (Ej: CP1)
    location VARCHAR(255) NOT NULL,
    price_kwh DECIMAL(4, 2) NOT NULL, -- Precio por kWh (Ej: 0.54)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3. Insertar los datos de EVCharging/data.json
INSERT INTO charge_points (cp_id, location, price_kwh) VALUES
('CP1', 'C/Italia 5', 0.54),
('CP2', 'C/Miro 9', 0.56),
('CP3', 'Gran Via 2', 0.54),
('CP4', 'C/Bueno 13', 0.52),
('CP5', 'C/Cervantes 2', 0.58),
('CP6', 'C/Arguelles', 0.46),
('CP7', 'C/Serrano 10', 0.60),
('CP8', 'C/Pez 23', 0.48),
('CP9', 'C/Mayor 5', 0.50),
('CP10', 'C/Alcala 15', 0.52),
('CP11', 'Plaza Nueva', 0.42),
('CP12', 'Maestranza', 0.40),
('CP13', 'C/Sevilla 4', 0.54),
('CP14', 'C/Triana 8', 0.48),
('CP15', 'C/Betis 12', 0.46),
('CP16', 'San Javier', 0.48),
('CP17', 'Museo Arts', 0.50),
('CP18', 'C/Cid 7', 0.52),
('CP19', 'Gran Plaza', 0.54),
('CP20', 'C/Colon 3', 0.56);