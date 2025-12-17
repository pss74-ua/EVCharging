# Contenido de EV_W.py

# Ejecución: python EV_W.py <api_central_host:port> <openweather_api_key>
# Ejemplo: python EV_W.py localhost:5000 YOUR_API_KEY

import sys
import time
import requests
import json
import threading

# --- CONSTANTES DE CONFIGURACIÓN ---
OPENWEATHER_BASE_URL = "http://api.openweathermap.org/data/2.5/weather"
ALERT_THRESHOLD_C = 0 
CHECK_INTERVAL_SECONDS = 4 

# --- VARIABLES GLOBALES ---
# Localizaciones a monitorear (se cargarán desde la API Central)
CP_LOCATIONS = {} 

# cp_alert_status: Almacena el ÚLTIMO ESTADO DE ALERTA BOOLEANO (True/False)
# para detectar transiciones.
cp_alert_status = {} 

central_api_url = None
openweather_api_key = None

# --- FUNCIONES DE COMUNICACIÓN CON API CENTRAL (REINTRODUCIDAS) ---

def load_all_cp_locations_from_api():
    """Obtiene todas las ubicaciones de los CPs desde la API Central."""
    global central_api_url, CP_LOCATIONS
    url = f"http://{central_api_url}/api/v1/locations"
    
    try:
        response = requests.get(url, timeout=3)
        response.raise_for_status()
        data = response.json()
        
        CP_LOCATIONS.clear()
        
        for cp in data.get('locations', []):
            cp_id = cp['cp_id']
            location_str = cp['location']
            
            parts = location_str.split(',')
            if len(parts) == 2 and parts[0].strip() and parts[1].strip():
                CP_LOCATIONS[cp_id] = (parts[0].strip(), parts[1].strip())
            else:
                CP_LOCATIONS[cp_id] = (location_str, "??") 

        print(f"[API] {len(CP_LOCATIONS)} ubicaciones de CPs cargadas desde Central.")
        return True
        
    except requests.exceptions.RequestException as e:
        print(f"[Error API] Fallo al cargar ubicaciones de Central: {e}")
        return False

def update_cp_location_via_api(cp_id, new_location):
    """Actualiza la ubicación de un CP enviando un PUT a la API Central."""
    global central_api_url
    url = f"http://{central_api_url}/api/v1/location/{cp_id}"
    headers = {'Content-Type': 'application/json'}
    payload = {'location': new_location}
    
    parts = new_location.split(',')
    if len(parts) != 2 or not all(parts):
        print("[Error] Formato inválido. Use: Ciudad,CC (ej: Paris,FR)")
        return False
    
    try:
        response = requests.put(url, headers=headers, json=payload, timeout=3)
        response.raise_for_status()
        
        print(f"[API] Ubicación de CP {cp_id} actualizada a: {new_location}")
        
        # Recargar inmediatamente para el bucle de monitorización
        load_all_cp_locations_from_api() 
        
        # Esto fuerza al monitoring_loop a enviar la temperatura en el siguiente ciclo.
        if cp_id in cp_alert_status:
            del cp_alert_status[cp_id]
            print(f"  [INFO] Estado de clima interno para {cp_id} reseteado. Se forzará la actualización de temperatura.")
        
    except requests.exceptions.RequestException as e:
        print(f"[Error API] Fallo al actualizar ubicación: {e}")
        return False


# --- FUNCIONES AUXILIARES (MODIFICADAS) ---

def kelvin_to_celsius(k):
    """Convierte grados Kelvin a Celsius (K - 273.15)."""
    return k - 273.15

def get_weather_data(city, country_code):
    """Consulta la API de OpenWeather para obtener la temperatura actual."""
    global openweather_api_key
    
    params = {
        'q': f"{city},{country_code}",
        'appid': openweather_api_key
    }
    
    try:
        response = requests.get(OPENWEATHER_BASE_URL, params=params, timeout=3)
        response.raise_for_status() 
        data = response.json()
        
        temp_k = data['main']['temp']
        temp_c = kelvin_to_celsius(temp_k)
        
        return temp_c
        
    except requests.exceptions.RequestException as e:
        print(f"  [Error Clima] FALLO en la API para {city}: {e}") 
        return None
    except KeyError:
        print("  [Error JSON] Formato de respuesta inesperado de OpenWeather.")
        return None
        
    return None

def notify_central(cp_id, temperature): # <--- CORREGIDO: Recibe 'temperature'
    """
    Notifica a Central (vía API Central) la temperatura actual del CP.
    """
    global central_api_url
    
    url = f"http://{central_api_url}/api/v1/weather_alert/{cp_id}"
    headers = {'Content-Type': 'application/json'}
    # Uso correcto del argumento 'temperature' para enviar el payload
    payload = {'temperature': temperature} 
    
    try:
        response = requests.put(url, headers=headers, json=payload, timeout=2)
        response.raise_for_status()
        return True
    except requests.exceptions.RequestException as e:
        print(f"  [Error Central API] No se pudo notificar a Central: {e}")
        return False


# --- BUCLE PRINCIPAL DE MONITORIZACIÓN ---

def monitoring_loop():
    """
    Bucle principal que chequea el clima y SOLO notifica a Central en los cruces de umbral (0°C).
    """
    global cp_alert_status
    
    print("\n[INFO] Iniciando bucle de monitorización. Presiona Ctrl+C para salir.")
    
    try:
        while True:
            # Recargamos la lista en cada ciclo
            load_all_cp_locations_from_api() 
            
            print("\n" + "="*50)
            print(f"CHEQUEANDO CLIMA ({time.strftime('%H:%M:%S')})")
            print("="*50)

            for cp_id, (city, country) in CP_LOCATIONS.items():
                
                if country == "??":
                    print(f"  [CP {cp_id}] Ubicación en formato incorrecto ('{city}'). Saltar.")
                    continue

                temp_c = get_weather_data(city, country)
                
                if temp_c is None:
                    continue 
                
                temp_rounded = round(temp_c, 1)

                # 1. Determinar el estado de alerta ACTUAL
                is_alert_active_now = (temp_rounded <= ALERT_THRESHOLD_C)
                
                # 2. Comprobar si el CP es "nuevo" (fue reseteado o es un arranque)
                is_initial_check = cp_id not in cp_alert_status 
                
                # 3. Obtener el último estado de alerta enviado
                # Usamos False como estado por defecto para la comparación si es nuevo
                last_alert_status = cp_alert_status.get(cp_id, False) 
                
                # 4. Notificar si: a) Es la primera comprobación (is_initial_check), O b) Hubo un cruce de umbral.
                if is_initial_check or (is_alert_active_now != last_alert_status):
                    
                    if is_initial_check:
                        action_desc = "PRIMERA NOTIFICACIÓN (tras cambio de ubicación/arranque)"
                    elif is_alert_active_now:
                        action_desc = "¡ALERTA! (T<=0) Notificando STOP"
                    else:
                        action_desc = "¡Alerta Finalizada! (T>0) Notificando RESUME"

                    print(f"  >>> [ENVÍO FORZADO] CP {cp_id} en {city} a {temp_rounded}°C. Razón: {action_desc}.")
                        
                    # Enviar la temperatura
                    if notify_central(cp_id, temp_rounded):
                        # Actualizar el estado interno para que solo se vuelva a enviar en un cruce
                        cp_alert_status[cp_id] = is_alert_active_now
                    
                else:
                    # Si la alerta está estable, no notifica
                    print(f"  [CP {cp_id} - {city}] T: {temp_rounded}°C. Alerta estable ({'ON' if is_alert_active_now else 'OFF'}). No notificar.")
                        
            time.sleep(CHECK_INTERVAL_SECONDS)
            
    except KeyboardInterrupt:
        print("\n[Monitor Clima] Bucle detenido por el usuario.")
    except Exception as e:
        print(f"\n[Monitor Clima] Error fatal en el bucle: {e}")


# --- FUNCIÓN PRINCIPAL (CON MENÚ) ---

def main():
    global central_api_url, openweather_api_key
    
    if len(sys.argv) != 3:
        print("Uso: python EV_W.py <api_central_host:port> <openweather_api_key>")
        print("Ejemplo: python EV_W.py localhost:5000 YOUR_API_KEY")
        sys.exit(1)
        
    central_api_url = sys.argv[1]
    openweather_api_key = sys.argv[2]
    
    if openweather_api_key == 'YOUR_API_KEY':
        print("\n[ADVERTENCIA] Por favor, obtenga una clave de OpenWeather válida y reemplace 'YOUR_API_KEY'.")
        time.sleep(2)
    
    print(f"--- Iniciando EV Weather Control Office (EV_W) ---")
    
    load_all_cp_locations_from_api()
    
    while True:
        print("\n--- MENÚ EV WEATHER CONTROL ---")
        print("1. Iniciar Monitorización de Clima (Bucle)")
        print("2. Modificar Ubicación de CP (Vía API Central)")
        print("3. Salir")
        
        choice = input("Seleccione una opción: ").strip()
        
        if choice == '1':
            monitoring_loop()
        
        elif choice == '2':
            cp_id = input("  > ID del CP a modificar (ej: CP1): ").strip().upper()
            new_loc = input("  > Nueva Ubicación (Formato: Ciudad,CC, ej: Paris,FR): ").strip()
            if cp_id and new_loc:
                update_cp_location_via_api(cp_id, new_loc)
            else:
                print("[Error] ID y Ubicación no pueden estar vacíos.")
                
        elif choice == '3':
            print("Apagando EV_W...")
            break
            
        else:
            print("Opción no válida.")

if __name__ == "__main__":
    main()