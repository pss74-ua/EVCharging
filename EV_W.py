# Contenido de EV_W.py modificado para monitorización automática

import sys
import time
import requests
import json
import threading

# --- CONSTANTES DE CONFIGURACIÓN ---
OPENWEATHER_BASE_URL = "http://api.openweathermap.org/data/2.5/weather"
ALERT_THRESHOLD_C = 0 
CHECK_INTERVAL_SECONDS = 4 # Tiempo automático entre chequeos (ajusta según necesites)

# --- VARIABLES GLOBALES ---
CP_LOCATIONS = {} 
cp_alert_status = {} 
central_api_url = None
openweather_api_key = None

# --- FUNCIONES DE COMUNICACIÓN CON API CENTRAL ---

def load_all_cp_locations_from_api():
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
        return True
    except Exception as e:
        print(f"[Error API] Carga de ubicaciones: {e}")
        return False

def update_cp_location_via_api(cp_id, new_location):
    global central_api_url, cp_alert_status
    url = f"http://{central_api_url}/api/v1/location/{cp_id}"
    headers = {'Content-Type': 'application/json'}
    payload = {'location': new_location}
    try:
        response = requests.put(url, headers=headers, json=payload, timeout=3)
        response.raise_for_status()
        print(f"\n[API] Ubicación de CP {cp_id} actualizada a: {new_location}")
        load_all_cp_locations_from_api() 
        # Forzar envío de temperatura inmediata al borrar estado previo
        if cp_id in cp_alert_status:
            del cp_alert_status[cp_id]
        return True
    except Exception as e:
        print(f"[Error API] Actualización ubicación: {e}")
        return False

# --- FUNCIONES CLIMA ---

def get_weather_data(city, country_code):
    global openweather_api_key
    params = {'q': f"{city},{country_code}", 'appid': openweather_api_key}
    try:
        response = requests.get(OPENWEATHER_BASE_URL, params=params, timeout=3)
        response.raise_for_status() 
        data = response.json()
        return data['main']['temp'] - 273.15 # Kelvin a Celsius
    except:
        return None

def notify_central(cp_id, temperature):
    global central_api_url
    url = f"http://{central_api_url}/api/v1/weather_alert/{cp_id}"
    try:
        requests.put(url, json={'temperature': temperature}, timeout=2)
        return True
    except:
        return False

# --- BUCLE AUTOMÁTICO (HILO SEPARADO) ---

def monitoring_loop():
    global cp_alert_status
    print("\n[SISTEMA] Monitorización automática iniciada en segundo plano.")
    while True:
        load_all_cp_locations_from_api() 
        for cp_id, (city, country) in CP_LOCATIONS.items():
            if country == "??": continue
            temp_c = get_weather_data(city, country)
            if temp_c is None: continue 
            
            temp_rounded = round(temp_c, 1)
            is_alert_active_now = (temp_rounded <= ALERT_THRESHOLD_C)
            
            # Notificar si es cambio de umbral (STOP/RESUME) o si es un CP nuevo/cambiado
            is_initial_check = cp_id not in cp_alert_status 
            last_alert_status = cp_alert_status.get(cp_id, False) 
            
            if is_initial_check or (is_alert_active_now != last_alert_status):
                if notify_central(cp_id, temp_rounded):
                    cp_alert_status[cp_id] = is_alert_active_now
        
        time.sleep(CHECK_INTERVAL_SECONDS)

# --- MENÚ PRINCIPAL ---

def main():
    global central_api_url, openweather_api_key
    if len(sys.argv) != 3:
        sys.exit(1)
    central_api_url = sys.argv[1]
    openweather_api_key = sys.argv[2]
    
    print(f"--- EV Weather Control (EV_W) ---")
    load_all_cp_locations_from_api()
    
    # LANZAR MONITORIZACIÓN EN HILO SEPARADO
    daemon_thread = threading.Thread(target=monitoring_loop, daemon=True)
    daemon_thread.start()
    
    while True:
        print("\n--- MENÚ CONFIGURACIÓN ---")
        print("1. Cambiar ubicación de CP")
        print("2. Ver estados actuales")
        print("3. Salir")
        
        choice = input("Seleccione opción: ").strip()
        
        if choice == '1':
            cp_id_input = input("ID CP: ").strip().upper()
            cp_id = f"CP{cp_id_input}" if cp_id_input.isdigit() else cp_id_input
            new_loc = input("Nueva Ubicación (Ciudad,CC): ").strip()
            update_cp_location_via_api(cp_id, new_loc)
        elif choice == '2':
            print("\nUbicaciones monitoreadas automáticamente:")
            for k, v in CP_LOCATIONS.items():
                print(f" - {k}: {v[0]},{v[1]}")
        elif choice == '3':
            break

if __name__ == "__main__":
    main()