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
# Habilitar CORS para permitir el acceso desde el navegador (file:///)
CORS(app)

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
        query = 'SELECT cp_id, location, price_kwh, is_registered, symmetric_key, status FROM charge_points'
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

@app.route('/api/v1/weather_alert/<cp_id>', methods=['PUT'])
def handle_weather_alert(cp_id):
    """
    Endpoint consumido por EV_W. Actualiza el campo weather_alert en la BD.
    """
    data = request.get_json()
    action = data.get('action') # Esperamos 'ALERT' o 'CANCEL'
    
    cp_id_upper = cp_id.upper()
    set_alert_value = None

    if action == 'ALERT':
        set_alert_value = True 
        log_msg = f"Alerta de Clima ACTIVADA para {cp_id_upper}"
    elif action == 'CANCEL':
        set_alert_value = False 
        log_msg = f"Alerta de Clima CANCELADA para {cp_id_upper}"
    else:
        return jsonify({'error': 'Acción no válida. Use ALERT o CANCEL.'}), 400

    try:
        cur = mysql.connection.cursor()
        # Modificamos el nuevo campo weather_alert en la BD
        query = "UPDATE charge_points SET weather_alert = %s WHERE cp_id = %s"
        cur.execute(query, (set_alert_value, cp_id_upper))
        mysql.connection.commit()
        cur.close()

        print(f"[API W] {log_msg}. BD actualizada.")
        return jsonify({
            'message': log_msg,
            'status_in_db': set_alert_value
        }), 200

    except Exception as e:
        print(f"[Error BD] No se pudo actualizar weather_alert para {cp_id_upper}: {e}")
        return jsonify({'error': f'Error al actualizar DB: {e}'}), 500

# --- 3. LÓGICA DE EJECUCIÓN (Punto de entrada) ---
if __name__ == "__main__":
    # Usamos host='0.0.0.0' para que la API sea accesible desde el Front en otros PCs
    # El puerto por defecto para Flask es 5000
    app.run(host='0.0.0.0', port=5000, debug=True, use_reloader=False)