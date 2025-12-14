# Ejecución: python EV_CP_M.py localhost:10001 localhost:9090 localhost:6000 CP1 (python EV_CP_M.py <ip_engine:puerto> <ip_central:puerto> <id_cp>)
#
# Este script implementa "EV_CP_M" (Monitor), el módulo de monitorización
# del punto de recarga.
#
# Funcionalidades principales:
# 1. Conectarse a EV_Central (vía Socket) para autenticarse y registrar el CP.
# 2. Conectarse a EV_CP_E (vía Socket) para pasarle el ID del CP.
# 3. Entrar en un bucle de "health check" que comprueba el estado
#    del Engine (EV_CP_E) cada segundo.
# 4. Reportar cualquier cambio de estado (avería 'KO' o recuperación 'OK')
#    inmediatamente a EV_Central .
#

import sys
import socket
import time
import json
import requests # Necesario para llamar al API REST del Registry
import base64
import hashlib
from cryptography.fernet import Fernet

# --- Variables Globales ---
engine_addr = None  # (host, puerto) del Engine (EV_CP_E)
central_addr = None # (host, puerto) de Central (EV_Central)
registry_addr = None # (host, puerto) del Registry HTTP
cp_id_global = None # ID de este CP (ej: "ALC1")
sock_central = None # Socket persistente para Central
sock_engine = None  # Socket persistente para Engine
current_status = "UNKNOWN" # Estado actual de salud (OK, FAULTED)
symmetric_key = None # Se obtiene del Registry y se usará para hablar con Central

# --- Funciones de Cifrado ---

def get_cipher(hex_key):
    """
    Convierte la clave hexadecimal del Registry en un objeto Fernet válido para cifrar/descifrar.
    """
    # 1. Hasheamos la clave para asegurar que tenga 32 bytes exactos
    key_bytes = hashlib.sha256(hex_key.encode('utf-8')).digest()
    # 2. Codificamos en Base64 URL-safe (requisito de Fernet)
    fernet_key = base64.urlsafe_b64encode(key_bytes)
    return Fernet(fernet_key)

# --- Funciones de Red ---

def robust_connect(host, port, component_name):
    """
    Función helper para intentar conectarse a un host:puerto.
    Devuelve el socket en caso de éxito o None si falla.
    """
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((host, port))
        print(f"[Socket] Conectado con éxito a {component_name} en {host}:{port}")
        return sock
    except socket.error as e:
        print(f"[Error Socket] No se pudo conectar a {component_name} en {host}:{port}. {e}")
        return None

"""
--- Funciones de Registro y Comunicación de la entrega 1 ---
def register_with_central():
    """"""
    Intenta conectarse y registrarse en EV_Central.
    Maneja la reconexión si es necesario.
    Implementa la parte de "autenticar y registrar a los CP en la central".
    """"""
    global sock_central, central_addr, cp_id_global
    
    if sock_central:
        # Si ya tenemos un socket, asumimos que estamos conectados y registrados.
        return True

    host, port = central_addr
    # Intenta establecer la conexión física
    sock_central = robust_connect(host, port, "EV_Central")
    
    if sock_central:
        try:
            # Una vez conectados, enviar mensaje de registro JSON
            register_msg = {
                "type": "REGISTER_MONITOR",
                "cp_id": cp_id_global
            }
            sock_central.sendall(json.dumps(register_msg).encode('utf-8'))
            
            # Esperar confirmación (ACK) de Central
            response = sock_central.recv(1024).decode('utf-8')
            if response == "ACK_REGISTER":
                print(f"[Central] Registro de Monitor {cp_id_global} aceptado por Central.")
                return True
            else:
                # Central rechazó el registro (ej: CP_ID desconocido)
                print(f"[Central] Registro rechazado por Central: {response}. Abortando.")
                sock_central.close()
                sock_central = None
                return False
        except socket.error as e:
            # Error durante el envío/recepción del registro
            print(f"[Error Central] Error durante el registro: {e}")
            sock_central.close()
            sock_central = None
            return False
    return False # No se pudo conectar

def register_with_engine():
    """"""
    Intenta conectarse y registrarse en EV_CP_E (Engine).
    El Monitor es quien "le dice" al Engine cuál es su ID.
    """"""
    global sock_engine, engine_addr, cp_id_global
    
    if sock_engine:
        # Si ya estamos conectados, no hacemos nada.
        return True
        
    host, port = engine_addr
    # Intenta establecer la conexión física con el Engine
    sock_engine = robust_connect(host, port, "EV_CP_E (Engine)")
    
    if sock_engine:
        try:
            # Enviar mensaje de registro con ID.
            # El Engine (EV_CP_E) espera este formato: "REGISTER_ID|{cp_id}"
            register_msg = f"REGISTER_ID|{cp_id_global}"
            sock_engine.sendall(register_msg.encode('utf-8'))

            # Esperar respuesta del Engine.
            # El Engine puede tardar si está esperando a Kafka, por eso el timeout es largo.
            sock_engine.settimeout(10.0) 
            response = sock_engine.recv(1024).decode('utf-8')
            sock_engine.settimeout(None) # Quitar timeout para el bucle de health check
            
            if response == "ACK_REGISTER":
                print(f"[Engine] Monitor registrado con éxito en Engine {cp_id_global}.")
                return True
            else:
                # El Engine rechazó el registro
                print(f"[Error Engine] Registro rechazado por Engine: {response}")
                sock_engine.close()
                sock_engine = None
                return False
            
        except socket.timeout:
            # Error común si el Engine no puede conectar con Kafka
            print("[Error Engine] Timeout. El Engine no respondió a tiempo (¿Problema de Kafka?).")
            sock_engine.close()
            sock_engine = None
            return False
        
        except socket.error as e:
            print(f"[Error Engine] Error durante el registro: {e}")
            sock_engine.close()
            sock_engine = None
            return False
    return False # No se pudo conectar

def send_status_to_central(status, info):
    """"""
    Envía una actualización de estado (avería o recuperación) a EV_Central.
    Implementa la notificación de "En caso de avería, notificará a CENTRAL".
    """"""
    global sock_central, cp_id_global, current_status
    
    # Asegurarse de que estamos conectados a Central. Si no, intenta reconectar.
    if not register_with_central():
        print("[Error Central] Imposible enviar estado. No se puede conectar/registrar con Central.")
        current_status = "UNKNOWN" # Perdimos conexión, no sabemos el estado
        return

    try:
        # Formato del mensaje de estado (avería/recuperación)
        status_msg = {
            "type": "MONITOR_STATUS",
            "cp_id": cp_id_global,
            "timestamp": time.time(),
            "status": status, # "OK" o "FAULTED"
            "info": info
        }
        sock_central.sendall(json.dumps(status_msg).encode('utf-8'))
        print(f"[Central->] Notificado a Central: {status} ({info})")
    except socket.error as e:
        # Si la conexión se pierde al enviar, se marca para reconexión
        print(f"[Error Central] Conexión perdida con Central al enviar estado: {e}. Reintentando...")
        sock_central.close()
        sock_central = None
        current_status = "UNKNOWN"
"""

# --- Funciones del Registry de la entrega 2 ---

def register_with_registry_http():
    """
    Consume el API REST del Registry para dar de alta el CP.
    Obtiene la symmetric_key necesaria para la seguridad.
    """
    global symmetric_key, registry_addr, cp_id_global
    
    # Construir URL (HTTP plano, sin SSL)
    host, port = registry_addr
    url = f"http://{host}:{port}/api/v1/charge_point"
    
    Location = "Unknown"
    Price = "0.50"
    ask_user = True # Por defecto preguntamos
    
    try:
        # Hacemos GET /api/v1/charge_point/<id>
        check_response = requests.get(f"{url}/{cp_id_global}", timeout=5)
        
        if check_response.status_code == 200:
            # ¡Existe! Usamos sus datos
            data = check_response.json()
            if data.get('location') and data.get('location') != "Unknown":
                Location = data.get('location')
                Price = str(data.get('price_kwh') or "0.50")
                ask_user = False # No molestamos al usuario
            
    except Exception as e:
        print(f"[Registry] No se pudo verificar existencia (se pedirán datosalian): {e}")

    if ask_user:
        Location = input("Ubicación del CP: ") or "Unknown"
        Price = input("Precio kWh: ") or "0.50"

    payload = {
        "cp_id": cp_id_global,
        "location": Location,
        "price_kwh": float(Price)
    }
    
    print(f"\n[Registry] Contactando a {url}...")
    
    try:
        response = requests.post(url, json=payload, timeout=5)
        
        if response.status_code == 201:
            data = response.json()
            symmetric_key = data.get('symmetric_key')
            print(f"[Registry] ¡ÉXITO! CP Registrado.")
            print(f"[Seguridad] Clave simétrica recibida: {symmetric_key}")
            return True
        else:
            print(f"[Registry] Error {response.status_code}: {response.text}")
            return False
            
    except requests.exceptions.ConnectionError:
        print("[Registry] Error: No se puede conectar al Registry. ¿Está encendido?")
        return False
    except Exception as e:
        print(f"[Registry] Error inesperado: {e}")
        return False
    
def deregister_from_registry_http():
    """
    Consume el API REST del Registry para dar de BAJA el CP (DELETE).
    Borra la symmetric_key de la memoria del Monitor.
    """
    global symmetric_key, registry_addr, cp_id_global, sock_central
    
    if not symmetric_key:
        print("[Registry] No estás registrado, no puedes darte de baja.")
        return

    # Construir URL: http://host:port/api/v1/charge_point/<id>
    host, port = registry_addr
    url = f"http://{host}:{port}/api/v1/charge_point/{cp_id_global}"
    
    print(f"\n[Registry] Solicitando BAJA a {url}...")
    
    try:
        response = requests.delete(url, timeout=5)
        
        if response.status_code in [200, 404]:
            if response.status_code == 200:
                print(f"[Registry] ¡ÉXITO! CP dado de baja correctamente.")
            else:
                print("[Registry] El CP no existía en el Registry. Limpiando datos locales...")

            # 1. Borrar clave de seguridad
            symmetric_key = None 
            print(f"[Seguridad] Clave simétrica borrada.")

            # 2. Desconectar de Central
            if sock_central:
                try:
                    sock_central.close()
                except:
                    pass
                sock_central = None # Reseteamos la variable para que el menú se actualice
                print("[Central] Desconectado.")
        else:
            print(f"[Registry] Error {response.status_code}: {response.text}")
            
    except requests.exceptions.ConnectionError:
        print("[Registry] Error: No se puede conectar al Registry.")
    except Exception as e:
        print(f"[Registry] Error inesperado: {e}")

# --- Funciones de Engine y Central ---

def connect_to_engine():
    """Conecta con el Engine (EV_CP_E)."""
    global sock_engine, engine_addr, cp_id_global
    
    if sock_engine:
        print("[Engine] Ya estás conectado.")
        return True
        
    host, port = engine_addr
    sock_engine = robust_connect(host, port, "EV_CP_E")
    
    if sock_engine:
        try:
            # Protocolo de saludo con Engine
            msg = f"REGISTER_ID|{cp_id_global}"
            sock_engine.sendall(msg.encode('utf-8'))
            
            sock_engine.settimeout(5.0)
            resp = sock_engine.recv(1024).decode('utf-8')
            sock_engine.settimeout(None)
            
            if resp == "ACK_REGISTER":
                print("[Engine] Engine listo y vinculado.")
                return True
            else:
                print(f"[Engine] Rechazado: {resp}")
                sock_engine.close()
                sock_engine = None
        except Exception as e:
            print(f"[Engine] Error en handshake: {e}")
            if sock_engine: sock_engine.close()
            sock_engine = None
            
    return False

def connect_to_central():
    """Conecta y se autentica con Central."""
    global sock_central, central_addr, cp_id_global, symmetric_key
    
    # REQUISITO: No conectar si no tenemos clave del Registry
    if not symmetric_key:
        print("[Central] ERROR: No tienes clave de seguridad. Regístrate primero en el Registry.")
        return False
        
    if sock_central:
        print("[Central] Ya estás conectado.")
        return True

    host, port = central_addr
    sock_central = robust_connect(host, port, "EV_Central")
    
    if sock_central:
        try:
            # Protocolo de autenticación
            # Enviamos ID y (en el futuro) usaremos la clave para cifrar
            auth_msg = {
                "type": "REGISTER_MONITOR",
                "cp_id": cp_id_global,
                # En la Release 2 completa, aquí se usaría la symmetric_key para firmar/cifrar
                # Por ahora, enviamos el registro básico.
            }
            sock_central.sendall(json.dumps(auth_msg).encode('utf-8'))
            
            resp = sock_central.recv(1024).decode('utf-8')
            if resp == "ACK_REGISTER":
                print("[Central] Autenticación correcta.")
                return True
            else:
                print(f"[Central] Rechazado: {resp}")
                sock_central.close()
                sock_central = None
        except Exception as e:
            print(f"[Central] Error al autenticar: {e}")
            if sock_central: sock_central.close()
            sock_central = None
            
    return False

"""
# --- Bucle Principal ---

def health_check_loop():
    """"""
    Bucle principal que consulta al Engine cada segundo .
    Reporta cambios de estado (avería/recuperación) a Central.
    """"""
    global sock_engine, current_status
    
    while True:
        time.sleep(1) # "comprobando el estado de salud del punto de recarga" (cada segundo)
        
        new_status = "UNKNOWN"
        info = ""

        # 1. Asegurarse de que estamos conectados al Engine
        if not register_with_engine():
            # Si no podemos reconectar con el Engine, es un fallo crítico.
            print("\n[Monitor] Conexión con Engine perdida o rechazada. Cerrando Monitor...")
            break # Salir del bucle y terminar el script
        else:
            # 2. Estamos conectados. Realizar el "health check".
            try:
                # Enviar el comando de comprobación
                sock_engine.sendall(b"HEALTH_CHECK")
                sock_engine.settimeout(2.0) # Esperar max 2s por respuesta
                
                # Leer respuesta del Engine
                response = sock_engine.recv(1024).decode('utf-8')
                
                if response == "OK":
                    # El Engine funciona
                    new_status = "OK"
                    info = "Engine operational"
                elif response == "KO":
                    # El Engine ha reportado una avería simulada 
                    new_status = "FAULTED"
                    info = "Engine reported KO (Simulated Fault)"
                else:
                    # Respuesta inesperada
                    raise socket.error(f"Invalid response from Engine: {response}")
                    
            except (socket.timeout, socket.error, ConnectionResetError, BrokenPipeError) as e:
                # Fallo en la comunicación con el Engine (Timeout, conexión rota, etc.)
                # Esto se considera una AVERÍA 
                print(f"[Error Engine] Health check fallido: {e}")
                sock_engine.close()
                sock_engine = None
                new_status = "FAULTED"
                info = "Engine unresponsive or connection lost"
        
        # 3. Comparar estado nuevo con el anterior
        if new_status != current_status:
            # Si hay un cambio (ej: de OK -> FAULTED), notificar a Central
            print(f"[Monitor] Cambio de estado detectado: {current_status} -> {new_status}")
            current_status = new_status
            
            # Mapear estado interno a estado de Central ("OK" o "FAULTED")
            central_status = "FAULTED" if new_status == "FAULTED" else "OK"
            send_status_to_central(central_status, info)
        else:
            # Si no hay cambios, solo imprimir un "latido" para saber que sigue vivo
            print(f"  ...Monitor {cp_id_global} status: {current_status}", end='\r')


def main():
    global engine_addr, central_addr, cp_id_global
    
    # 1. Validar argumentos de línea de comandos 
    if len(sys.argv) != 4:
        print("Uso: python EV_CP_M.py <ip_engine:puerto_engine> <ip_central:puerto_central> <id_cp>")
        sys.exit(1)
        
    try:
        # Arg 1: IP y puerto del EV_CP_E 
        engine_host, engine_port = sys.argv[1].split(':')
        engine_addr = (engine_host, int(engine_port))
        
        # Arg 2: IP y puerto del EV_Central 
        central_host, central_port = sys.argv[2].split(':')
        central_addr = (central_host, int(central_port))
        
        # Arg 3: ID del CP 
        cp_id_global = sys.argv[3]
    except ValueError:
        print("Error: Los argumentos de IP/Puerto deben estar en formato 'host:puerto'")
        sys.exit(1)
        
    print(f"--- Iniciando EV Charging Point MONITOR ---")
    print(f"  ID del CP:    {cp_id_global}")
    print(f"  Engine Addr:  {engine_addr}")
    print(f"  Central Addr: {central_addr}")
    print("-----------------------------------------")

    # 2. Conectar y registrarse en el Engine (EV_CP_E)
    # El Monitor se conecta al Engine 
    if not register_with_engine():
        print("[Error Fatal] No se pudo registrar en Engine. Abortando.")
        sys.exit(1)
    print("[Info] Registro en Engine OK (Engine confirma que está listo).")

    # 3. Conectar y registrarse en Central (EV_Central)
    # El Monitor se conecta a Central para autenticarse 
    if not register_with_central():
        print("[Error Fatal] No se pudo completar el registro inicial con Central. Abortando.")
        sys.exit(1)
    print("[Info] Registro inicial con Central OK.")

    # 4. Iniciar bucle de monitorización 
    print("[Info] Iniciando bucle de monitorización de salud...")
    health_check_loop()

if __name__ == "__main__":
    main()
"""

def health_check_loop():
    """Bucle de monitorización (Bloqueante)."""
    global sock_engine, sock_central, cp_id_global, current_status
    
    if not sock_engine or not sock_central:
        print("[Monitor] Error: Debes estar conectado a Engine y Central para monitorizar.")
        return

    print(f"\n[Monitor] INICIANDO MONITORIZACIÓN DE {cp_id_global}...")
    print("[Monitor] Presiona Ctrl+C para detener y volver al menú.\n")

    # Obtener el cifrador con la clave simétrica
    cipher = get_cipher(symmetric_key)
    
    try:
        while True:
            time.sleep(1)
            
            # 1. Ping al Engine
            new_status = "FAULTED"
            info = "Engine connection lost"
            
            try:
                sock_engine.sendall(b"HEALTH_CHECK")
                sock_engine.settimeout(2.0)
                resp = sock_engine.recv(1024).decode('utf-8')
                
                if resp == "OK":
                    new_status = "OK"
                    info = "Operational"
                elif resp == "KO":
                    new_status = "FAULTED"
                    info = "Engine Simulated Fault"
                    
            except Exception as e:
                print(f"[Monitor] Error comunicando con Engine: {e}")
                break # Salir del bucle si falla Engine

            # 2. Notificar a Central si hay cambio
            if new_status != current_status:
                print(f"[Estado] Cambio detectado: {current_status} -> {new_status}")
                current_status = new_status
                
                # Enviar a Central
                msg = {
                    "type": "MONITOR_STATUS",
                    "cp_id": cp_id_global,
                    "timestamp": time.time(),
                    "status": new_status,
                    "info": info
                }

                json_str = json.dumps(msg)                                  # 1. Convertir dict a string
                encrypted_data = cipher.encrypt(json_str.encode('utf-8'))   # 2. Cifrar

                try:
                    sock_central.sendall(encrypted_data)                    # 3. Enviar bytes cifrados a Central
                except:
                    print("[Monitor] Perdi conexión con Central.")
                    break
            else:
                print(f" ... Estado: {current_status}", end='\r')

    except KeyboardInterrupt:
        print("\n[Monitor] Deteniendo monitorización...")

# --- Menú Principal ---

def print_menu():
    global cp_id_global, symmetric_key, sock_engine, sock_central
    print("\n" + "="*40)
    print(f" EV CHARGING MONITOR - {cp_id_global}")
    print("="*40)
    print(f" Estado Registro: {'REGISTRADO' if symmetric_key else 'NO REGISTRADO'}")
    print("-" * 40)
    if not symmetric_key:
        print("1. REGISTRARSE en Registry (Obtener Clave)")
    else:
        print("1. DAR DE BAJA en Registry (Borrar Clave)")
    if sock_engine is None:
        # Paso 2a: Si no hay Engine, obligamos a conectar primero
        print("2. Conectar a Engine")
    elif sock_central is None:
        # Paso 2b: Si ya hay Engine pero no Central, ofrecemos Central
        print("2. Autenticarse con Central")
    else:
        # Paso 2c: Si tenemos todo
        print("2. [Conectado a ambos sistemas - Listo]")
    print("3. Iniciar Monitorización (Bucle)")
    print("4. Salir")
    print("="*40)

def main():
    global engine_addr, central_addr, registry_addr, cp_id_global, sock_engine, sock_central
    
    # Argumentos: IP_Engine IP_Central IP_Registry ID_CP
    if len(sys.argv) != 5:
        print("Uso: python EV_CP_M.py <eng_host:port> <cnt_host:port> <reg_host:port> <cp_id>")
        # Valores por defecto para facilitar pruebas si faltan argumentos
        print("Ej: python EV_CP_M.py localhost:10001 localhost:9090 localhost:6000 CP1")
        sys.exit(1)

    try:
        # Parsear Engine
        e_h, e_p = sys.argv[1].split(':')
        engine_addr = (e_h, int(e_p))
        
        # Parsear Central
        c_h, c_p = sys.argv[2].split(':')
        central_addr = (c_h, int(c_p))
        
        # Parsear Registry
        r_h, r_p = sys.argv[3].split(':')
        registry_addr = (r_h, int(r_p))
        
        cp_id_global = sys.argv[4]
        
    except ValueError:
        print("Error en formato de argumentos. Usa host:puerto")
        sys.exit(1)

    # Bucle del Menú
    while True:
        print_menu()
        choice = input("Opción: ")
        
        if choice == '1':
            if symmetric_key:
                deregister_from_registry_http()
            else:
                register_with_registry_http()
            
        elif choice == '2':
            if sock_engine is None:
                # Prioridad 1: Conectar al Engine
                connect_to_engine()
            elif sock_central is None:
                # Prioridad 2: Si ya tenemos Engine, intentamos Central
                # (connect_to_central ya verifica internamente si tenemos symmetric_key)
                connect_to_central()
            else:
                print("\n[Info] Ya estás conectado a Engine y Central. Puedes iniciar monitorización.")
            
        elif choice == '3':
            health_check_loop()
            
        elif choice == '4':
            print("Saliendo...")
            if sock_engine: sock_engine.close()
            if sock_central: sock_central.close()
            break
        else:
            print("Opción no válida.")

if __name__ == "__main__":
    main()