# Ejecución: python EV_Registry.py
# Descripción: Módulo de Registro de CPs (vía API REST HTTP)
# Ejemplo POST: curl -X POST http://localhost:6000/api/v1/charge_point -H "Content-Type: application/json" -d "{\"cp_id\": \"CP0\", \"location\": \"Barcelona\", \"price_kwh\": 0.35}"
# Ejemplo DELETE: curl -X DELETE http://localhost:6000/api/v1/charge_point/CP0
# Ejemplo GET: curl -X GET http://localhost:6000/api/v1/charge_point/CP0

from flask import Flask, request, jsonify
from flask_mysqldb import MySQL # O usar mysql.connector si lo prefieres, mantengo consistencia con tu API anterior
import mysql.connector
import secrets

# --- CONFIGURACIÓN ---
app = Flask(__name__)

# Configuración de conexión a Base de Datos
# Ajusta 'host' a 'db' si estás usando docker-compose, o 'localhost' si ejecutas en local.
DB_CONFIG = {
    'host': 'localhost',  
    'user': 'root',
    'password': 'psusana', 
    'database': 'ev_charging'
}

def get_db_connection():
    """Establece conexión con la base de datos MySQL."""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        return conn
    except mysql.connector.Error as err:
        print(f"[Error Conexión BD] {err}")
        return None

# --- ENDPOINTS API REST ---

@app.route('/api/v1/charge_point', methods=['POST'])
def register_cp():
    """
    Registra un nuevo CP (Alta).
    Recibe JSON: { "cp_id": "...", "location": "...", "price_kwh": ... }
    Devuelve: JSON con la 'symmetric_key' generada.
    """
    data = request.get_json()
    
    # 1. Validar datos de entrada
    cp_id = data.get('cp_id')
    location = data.get('location', 'Unknown')
    try:
        price = float(data.get('price_kwh', 0.50))
    except:
        price = 0.50 # Valor por defecto ante error

    if not cp_id:
        return jsonify({'error': 'Falta el parámetro cp_id'}), 400

    # 2. Generar Clave Simétrica (Token)
    # Requisito: "EV_Registry devolverá las claves... ÚNICA POR CP" [cite: 82, 92]
    # Generamos un token hexadecimal de 16 bytes (32 caracteres)
    symmetric_key = secrets.token_hex(16)

    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500

    try:
        cursor = conn.cursor()

        # 3. Insertar o Actualizar (Upsert) en la BD
        # Usamos tu estructura: cp_id, location, price_kwh, symmetric_key, is_registered
        query = """
            INSERT INTO charge_points (cp_id, location, price_kwh, symmetric_key, is_registered, status)
            VALUES (%s, %s, %s, %s, 1, 'DISCONNECTED')
            ON DUPLICATE KEY UPDATE
                location = VALUES(location),
                price_kwh = VALUES(price_kwh),
                symmetric_key = VALUES(symmetric_key),
                is_registered = 1,
                status = 'DISCONNECTED'
        """
        cursor.execute(query, (cp_id, location, price, symmetric_key))
        conn.commit()
        cursor.close()

        print(f"[Registry] Alta de CP: {cp_id}. Clave asignada.")

        # 4. Responder al CP con la clave generada
        return jsonify({
            'message': 'CP Registered Successfully',
            'cp_id': cp_id,
            'symmetric_key': symmetric_key  # El CP guardará esto
        }), 201

    except mysql.connector.Error as err:
        print(f"[Error BD] {err}")
        return jsonify({'error': f'Database error: {err}'}), 500
    finally:
        if conn and conn.is_connected():
            conn.close()

@app.route('/api/v1/charge_point/<cp_id>', methods=['DELETE'])
def deregister_cp(cp_id):
    """
    Da de baja un CP.
    Marca is_registered = 0 y borra la symmetric_key.
    """
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
        
    try:
        cursor = conn.cursor()

        # Baja lógica: Quitamos registro y borramos credenciales
        query = "UPDATE charge_points SET is_registered = 0, symmetric_key = NULL, status = 'DISCONNECTED' WHERE cp_id = %s"
        
        cursor.execute(query, (cp_id,))
        conn.commit()
        
        rows_affected = cursor.rowcount
        cursor.close()

        if rows_affected == 0:
            return jsonify({'message': 'CP not found or already deregistered'}), 404

        print(f"[Registry] Baja de CP: {cp_id}")
        return jsonify({'message': f'CP {cp_id} deregistered successfully'}), 200

    except mysql.connector.Error as err:
        return jsonify({'error': str(err)}), 500
    finally:
        if conn and conn.is_connected():
            conn.close()

@app.route('/api/v1/charge_point/<cp_id>', methods=['GET'])
def get_cp_info(cp_id):
    """Devuelve la info del CP si existe."""
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'DB Error'}), 500
    
    try:
        cursor = conn.cursor(dictionary=True) # dictionary=True para acceder por nombre campo
        # Si usas mysql.connector sin dictionary=True, usa cursor = conn.cursor() y accede por índice
        
        query = "SELECT location, price_kwh FROM charge_points WHERE cp_id = %s"
        cursor.execute(query, (cp_id,))
        result = cursor.fetchone()
        cursor.close()
        
        if result:
            return jsonify(result), 200 # Devuelve {location: "...", price_kwh: ...}
        else:
            return jsonify({'message': 'Not found'}), 404
            
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

# --- ARRANQUE (HTTP) ---
if __name__ == "__main__":
    # Usamos puerto 6000 para diferenciarlo de Central (5000)
    print("--- INICIANDO EV_REGISTRY (HTTP) ---")
    print("Escuchando en http://0.0.0.0:6000 ...")
    app.run(host='0.0.0.0', port=6000, debug=True)