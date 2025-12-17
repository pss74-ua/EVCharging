from flask import Flask, request, jsonify, render_template
from flask_mysqldb import MySQL # Librería para la conexión a MySQL
from flask_cors import CORS
#import mysql.connector as db_connector# Conector de Python a MySQL
import time
import json
import mysql.connector

# --- 1. INICIALIZACIÓN DE FLASK ---
app = Flask(__name__)
CORS(app)  # Habilitar CORS para todas las rutas y orígenes

# --- 2. CONFIGURACIÓN DE LA CONEXIÓN A LA BASE DE DATOS ---
app.config['MYSQL_HOST'] = 'localhost'
app.config['MYSQL_USER'] = 'root'
app.config['MYSQL_PASSWORD'] = 'psusana'  # <--- ¡AJUSTAR!
app.config['MYSQL_DB'] = 'ev_charging'   
app.config['MYSQL_CURSORCLASS'] = 'DictCursor'      

mysql = MySQL(app)
print("Conexión a BD configurada. Lista para usarse.")

# Ruta para servir el dashboard HTML
@app.route('/')
def home():
    """Sirve el archivo HTML del dashboard"""
    return render_template('dashboard.html')

# Decorador para definir la ruta y el método HTTP
@app.route('/api/v1/status', methods=['GET'])
def get_system_status():
    """
    Expone el estado de los CPs y del sistema para el Front.
    """
    try:
        # 1. Creamos el cursor de la forma más simple y estable.
        cur = mysql.connection.cursor()
        
        # 2. Ejecutar la consulta SQL
        # query = 'SELECT cp_id, location, price_kwh, is_registered, symmetric_key FROM charge_points'
        query = 'SELECT cp_id, location, price_kwh, is_registered, symmetric_key, status, weather_temperature FROM charge_points'
        cur.execute(query)
        
        # 3. Obtener todos los resultados (AHORA SON DICCIONARIOS)
        cp_configs = cur.fetchall()
        cur.close()
        
        # 4. Construir la respuesta JSON (El Front necesita estos datos)
        response = {
            'timestamp': time.time(),
            'cp_list': cp_configs,
            # (Aquí se añadirían los datos en tiempo real de EV_Central)
            'error': False,
            'message': 'System Status Fetched Successfully'
        }
        
        # 5. Devolver la respuesta JSON con código 200 (OK)
        return jsonify(response), 200
        
    except Exception as e:
        # Manejo de errores
        print(f"[ERROR GET] {e}")
        return jsonify({'error': True, 'message': f'Error Occurred: {e}', 'data': None}), 500

@app.route('/api/v1/locations', methods=['GET'])
def get_all_locations():
    """
    Expone todos los CP_ID y sus ubicaciones (location) para EV_W.
    """
    try:
        cur = mysql.connection.cursor()
        query = 'SELECT cp_id, location FROM charge_points'
        cur.execute(query)
        locations = cur.fetchall()
        cur.close()
        
        return jsonify({'locations': locations}), 200
        
    except Exception as e:
        print(f"[ERROR GET LOCATIONS] {e}")
        return jsonify({'error': f'Error al obtener ubicaciones: {e}'}), 500   

@app.route('/api/v1/location/<cp_id>', methods=['PUT'])
def update_cp_location_api(cp_id):
    """
    Permite a EV_W modificar la ubicación de un CP en la base de datos.
    Payload: { "location": "Nueva Ciudad,CC" }
    """
    data = request.get_json()
    new_location = data.get('location')

    if not new_location:
        return jsonify({'error': 'Falta el parámetro "location"'}), 400

    cp_id_upper = cp_id.upper()
    try:
        cur = mysql.connection.cursor()
        query = "UPDATE charge_points SET location = %s WHERE cp_id = %s"
        cur.execute(query, (new_location, cp_id_upper))
        mysql.connection.commit()
        
        if cur.rowcount == 0:
            cur.close()
            return jsonify({'error': f'CP {cp_id_upper} no encontrado.'}), 404
            
        cur.close()

        print(f"[API W] Ubicación de CP {cp_id_upper} actualizada a: {new_location}")
        return jsonify({
            'message': f'Ubicación de {cp_id_upper} actualizada correctamente.',
            'new_location': new_location
        }), 200

    except Exception as e:
        print(f"[Error BD] No se pudo actualizar la ubicación para {cp_id_upper}: {e}")
        return jsonify({'error': f'Error al actualizar DB: {e}'}), 500

@app.route('/api/v1/audit', methods=['GET'])
def get_audit_logs():
    """Devuelve los últimos registros de auditoría."""
    try:
        cur = mysql.connection.cursor()

        query = 'SELECT * FROM audit_logs ORDER BY timestamp DESC LIMIT 10'        
        cur.execute(query)
        logs = cur.fetchall()
        cur.close()
        return jsonify(logs), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/v1/weather_alert/<cp_id>', methods=['PUT'])
def handle_weather_alert(cp_id):
    """
    Endpoint consumido por EV_W. Actualiza el campo weather_alert en la BD.
    """
    data = request.get_json()
    temperature = data.get('temperature') # <--- ESPERAMOS EL VALOR DE TEMPERATURA
    
    cp_id_upper = cp_id.upper()
    
    if temperature is None:
        return jsonify({'error': 'Falta el parámetro temperature'}), 400

    try:
        # Validación de que el valor es numérico
        temp_value = float(temperature)
    except ValueError:
        return jsonify({'error': 'El valor de temperature no es un número válido.'}), 400

    try:
        cur = mysql.connection.cursor()
        
        # 2. MODIFICACIÓN AQUÍ: Guardar el valor de temperatura en la columna 'weather_temperature'
        query = "UPDATE charge_points SET weather_temperature = %s WHERE cp_id = %s"
        cur.execute(query, (temp_value, cp_id_upper))
        mysql.connection.commit()
        cur.close()

        print(f"[API W] Temperatura de CP {cp_id_upper} actualizada a {temp_value}°C.")
        return jsonify({
            'message': f'Temperatura de {cp_id_upper} actualizada.',
            'temperature_in_db': temp_value
        }), 200

    except Exception as e:
        print(f"[Error BD] No se pudo actualizar temperatura para {cp_id_upper}: {e}")
        return jsonify({'error': f'Error al actualizar DB: {e}'}), 500

# --- 3. LÓGICA DE EJECUCIÓN (Punto de entrada) ---
if __name__ == "__main__":
    # Usamos host='0.0.0.0' para que la API sea accesible desde el Front en otros PCs
    # El puerto por defecto para Flask es 5000
    app.run(host='0.0.0.0', port=5000, debug=True, use_reloader=False)