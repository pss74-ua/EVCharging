USE ev_charging;

-- 1. Añadir el campo de estado de registro (para EV_Registry)
-- is_registered: Indica si el CP ha pasado por el proceso de alta en el sistema.
ALTER TABLE charge_points
ADD COLUMN is_registered BOOLEAN DEFAULT FALSE;

-- 2. Añadir el campo de credencial de autenticación (para EV_Central)
-- auth_credential: La contraseña o token que el CP usará para autenticarse en Central.
ALTER TABLE charge_points
ADD COLUMN auth_credential VARCHAR(100) NULL;

-- 3. Añadir el campo de clave de cifrado simétrico (para cifrado Kafka)
-- symmetric_key: La clave única que Central dará al CP para cifrar los mensajes de telemetría.
ALTER TABLE charge_points
ADD COLUMN symmetric_key VARCHAR(100) NULL;

-- (Opcional) Revisar la nueva estructura de la tabla
DESCRIBE charge_points;