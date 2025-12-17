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

# --- Funciones del Registry de la entrega 2 ---

def check_initial_registration():
    """
    Consulta al Registry al arrancar para ver si ya estamos registrados.
    Si es así, recupera la clave simétrica automáticamente.
    """
    global symmetric_key, registry_addr, cp_id_global
    
    host, port = registry_addr
    url = f"http://{host}:{port}/api/v1/charge_point/{cp_id_global}"
        
    try:
        response = requests.get(url, timeout=2)
        
        if response.status_code == 200:
            data = response.json()
            # Verificamos si consta como registrado en la BD
            if data.get('is_registered') == 1:
                symmetric_key = data.get('symmetric_key')
                print(f"[Init] ¡CP ya registrado! Clave recuperada.")
            else:
                symmetric_key = None
            
    except Exception:
        print("[Init] No se pudo contactar con el Registry (continuamos como No Registrado).")

def register_with_registry_http():
    """
    Consume el API REST del Registry para dar de alta el CP.
    Obtiene la symmetric_key necesaria para la seguridad.
    """
    global symmetric_key, registry_addr, cp_id_global
    
    # Construir URL (HTTP plano, sin SSL)
    host, port = registry_addr
    url = f"http://{host}:{port}/api/v1/charge_point"

    Location = input("Ubicación del CP: ") or "Unknown"
    Price = input("Precio kWh: ") or "0.50"
    try:
        Price = float(Price) if Price.strip() else 0.50
    except ValueError:
        print("[Error] Precio inválido, usando 0.50")
        Price = 0.50

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
    global sock_engine, engine_addr, cp_id_global, symmetric_key
    
    if not symmetric_key:
        print("[Engine] ERROR: No tienes clave de seguridad. Regístrate primero en el Registry.")
        return False
    
    if sock_engine:
        print("[Engine] Ya estás conectado.")
        return True
        
    host, port = engine_addr
    sock_engine = robust_connect(host, port, "EV_CP_E")
    
    if sock_engine:
        try:
            # Protocolo de saludo con Engine
            msg = f"REGISTER_ID|{cp_id_global}|{symmetric_key}"
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

def health_check_loop():
    """Bucle de monitorización (Bloqueante)."""
    global sock_engine, sock_central, cp_id_global, current_status, symmetric_key
    
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
            new_status = "UNKNOWN" # Valor inicial
            info = ""
            
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
                # print(f"[Monitor] Conexión perdida con Engine: {e}")
                new_status = "DISCONNECTED"
                info = "Engine connection lost (Process stopped)"

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

def check_active_connections():
    """
    Verifica si los sockets siguen vivos antes de mostrar el menú.
    Usa el modo 'no bloqueante' para mirar si hay desconexión.
    """
    global sock_engine, sock_central
    
    # 1. Comprobar Engine
    if sock_engine:
        try:
            sock_engine.setblocking(False) # Modo no bloqueante
            try:
                # Intentamos leer. Si devuelve b'' (vacío), es que se cerró.
                # Si lanza BlockingIOError, es que está vivo pero no hay datos (todo OK).
                data = sock_engine.recv(16, socket.MSG_PEEK)
                if len(data) == 0:
                    raise ConnectionResetError
            except BlockingIOError:
                pass # Está vivo y esperando
            except (ConnectionResetError, ConnectionAbortedError):
                raise
            finally:
                sock_engine.setblocking(True) # Volver a modo normal
        except:
            sock_engine = None

            if sock_central: # Si perdemos Engine, también cerramos Central
                try:
                    sock_central.close()
                except:
                    pass
                sock_central = None

    # 2. Comprobar Central (Opcional, misma lógica)
    if sock_central:
        try:
            sock_central.setblocking(False)
            try:
                data = sock_central.recv(16, socket.MSG_PEEK)
                if len(data) == 0:
                    raise ConnectionResetError
            except BlockingIOError:
                pass
            except (ConnectionResetError, ConnectionAbortedError):
                raise
            finally:
                sock_central.setblocking(True)
        except:
            sock_central = None

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
    try:
        while True:
            check_initial_registration()
            check_active_connections()
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
                break
            else:
                print("Opción no válida.")
    finally:
        cipher = get_cipher(symmetric_key) if symmetric_key else None
        msg = {
                "type": "MONITOR_STATUS",
                "cp_id": cp_id_global,
                "timestamp": time.time(),
                "status": "DISCONNECTED",
                "info": "Monitor shutting down"
            }

        json_str = json.dumps(msg)                                  # 1. Convertir dict a string
        encrypted_data = cipher.encrypt(json_str.encode('utf-8'))   # 2. Cifrar
        sock_central.sendall(encrypted_data) 
        if sock_engine: sock_engine.close()
        if sock_central: sock_central.close()

if __name__ == "__main__":
    main()