# Contenido de EV_W.py

# Ejecución: python EV_W.py <api_central_host:port> <openweather_api_key>
# Ejemplo: python EV_W.py localhost:5000 YOUR_API_KEY
#
# Este script implementa "EV_W" (Weather Control Office).

import sys
import time
import requests
import json
import threading

# --- CONSTANTES DE CONFIGURACIÓN ---
OPENWEATHER_BASE_URL = "http://api.openweathermap.org/data/2.5/weather"
ALERT_THRESHOLD_C = 0 
CHECK_INTERVAL_SECONDS = 4 # Pollear OpenWeather cada 4 segundos

# --- VARIABLES GLOBALES ---
# Localizaciones a monitorear
CP_LOCATIONS = {
    "CP1": ("Madrid", "ES"), 
    "CP2": ("Barcelona", "ES"), 
    "CP7": ("Sevilla", "ES"),
    "CP11": ("London", "UK"), # Candidato a alerta
    "CP13": ("Alicante", "ES") 
}

# Estado interno de alerta de cada CP (para evitar spam a la API Central)
cp_alert_status = {} 

central_api_url = None
openweather_api_key = None

# --- FUNCIONES AUXILIARES ---

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
        print(f"  [Error Clima] FALLO en la API para {city}: {e}") # <--- AÑADIR/MODIFICAR
        return None
    except KeyError:
        print("  [Error JSON] Formato de respuesta inesperado de OpenWeather.")
        return None
        
    return None

def notify_central(cp_id, action):
    """
    Notifica a Central (vía API Central) de una alerta ('ALERT') o cancelación ('CANCEL').
    """
    global central_api_url
    
    # Llama al nuevo endpoint que actualiza la BD
    url = f"http://{central_api_url}/api/v1/weather_alert/{cp_id}"
    headers = {'Content-Type': 'application/json'}
    payload = {'action': action} 
    
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
    Bucle principal que chequea el clima y gestiona las alertas.
    """
    global cp_alert_status
    
    print("\n[INFO] Iniciando bucle de monitorización. Presiona Ctrl+C para salir.")
    
    try:
        while True:
            print("\n" + "="*50)
            print(f"CHEQUEANDO CLIMA ({time.strftime('%H:%M:%S')})")
            print("="*50)

            for cp_id, (city, country) in CP_LOCATIONS.items():
                
                temp_c = get_weather_data(city, country)
                
                if temp_c is None:
                    continue 

                current_alert_status = cp_alert_status.get(cp_id, 'OK')
                print(f"  [CP {cp_id} - {city}] T: {temp_c:.2f}°C. Estado interno: {current_alert_status}")

                # 2. Lógica de Alerta (T < 0°C)
                if temp_c < ALERT_THRESHOLD_C:
                    
                    if current_alert_status == 'OK':
                        print(f"  >>> ¡ALERTA! CP {cp_id} en {city} a {temp_c:.2f}°C. Notificando ALERTA.")
                        if notify_central(cp_id, 'ALERT'):
                            cp_alert_status[cp_id] = 'ALERT'
                        
                # 3. Lógica de Cancelación de Alerta (T >= 0°C)
                elif temp_c >= ALERT_THRESHOLD_C:
                    
                    if current_alert_status == 'ALERT':
                        print(f"  >>> ¡Alerta Finalizada! CP {cp_id} en {city} a {temp_c:.2f}°C. Notificando CANCELACIÓN.")
                        if notify_central(cp_id, 'CANCEL'):
                            cp_alert_status[cp_id] = 'OK'
                        
            
            time.sleep(CHECK_INTERVAL_SECONDS)
            
    except KeyboardInterrupt:
        print("\n[Monitor Clima] Bucle detenido por el usuario.")
    except Exception as e:
        print(f"\n[Monitor Clima] Error fatal en el bucle: {e}")


# --- FUNCIÓN PRINCIPAL ---

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
    
    for cp_id in CP_LOCATIONS.keys():
        cp_alert_status[cp_id] = 'OK'
    
    monitoring_loop()

if __name__ == "__main__":
    main()