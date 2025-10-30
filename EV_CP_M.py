# Ejecución: python EV_CP_M.py localhost:10001 localhost:9090 ALC1 (python EV_CP_M.py <ip_engine:puerto> <ip_central:puerto> <id_cp>)

import sys
import socket
import time
import json

# --- Variables Globales ---
engine_addr = None
central_addr = None
cp_id_global = None
sock_central = None # Socket persistente para Central
sock_engine = None  # Socket persistente para Engine
current_status = "UNKNOWN" # Estado actual de salud (OK, FAULTED)

# --- Funciones de Red ---

def robust_connect(host, port, component_name):
    """
    Intenta conectarse a un host:puerto. Devuelve el socket o None.
    """
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((host, port))
        print(f"[Socket] Conectado con éxito a {component_name} en {host}:{port}")
        return sock
    except socket.error as e:
        print(f"[Error Socket] No se pudo conectar a {component_name} en {host}:{port}. {e}")
        return None

def register_with_central():
    """
    Intenta conectarse y registrarse en EV_Central.
    Maneja la reconexión si es necesario.
    """
    global sock_central, central_addr, cp_id_global
    
    if sock_central:
        # Ya conectado, asumir registrado.
        return True

    host, port = central_addr
    sock_central = robust_connect(host, port, "EV_Central")
    
    if sock_central:
        try:
            # Enviar mensaje de registro/autenticación 
            register_msg = {
                "type": "REGISTER_MONITOR",
                "cp_id": cp_id_global
            }
            sock_central.sendall(json.dumps(register_msg).encode('utf-8'))
            
            # Esperar confirmación
            response = sock_central.recv(1024).decode('utf-8')
            if response == "ACK_REGISTER":
                print(f"[Central] Registro de Monitor {cp_id_global} aceptado por Central.")
                return True
            else:
                print(f"[Central] Registro rechazado por Central: {response}. Abortando.")
                sock_central.close()
                sock_central = None
                return False
        except socket.error as e:
            print(f"[Error Central] Error durante el registro: {e}")
            sock_central.close()
            sock_central = None
            return False
    return False

def register_with_engine():
    """
    Intenta conectarse y registrarse en EV_CP_E.
    """
    global sock_engine, engine_addr, cp_id_global
    
    if sock_engine:
        return True
        
    host, port = engine_addr
    sock_engine = robust_connect(host, port, "EV_CP_E (Engine)")
    
    if sock_engine:
        try:
            # Enviar mensaje de registro con ID (según EV_CP_E.py)
            register_msg = f"REGISTER_ID|{cp_id_global}"
            sock_engine.sendall(register_msg.encode('utf-8'))

            sock_engine.settimeout(10.0) # Esperar max 10s por respuesta
            response = sock_engine.recv(1024).decode('utf-8')
            sock_engine.settimeout(None) # Quitar timeout
            if response == "ACK_REGISTER":
                print(f"[Engine] Monitor registrado con éxito en Engine {cp_id_global}.")
                return True
            else:
                print(f"[Error Engine] Registro rechazado por Engine: {response}")
                sock_engine.close()
                sock_engine = None
                return False
            
        except socket.timeout:
            print("[Error Engine] Timeout. El Engine no respondió a tiempo (¿Problema de Kafka?).")
            sock_engine.close()
            sock_engine = None
            return False
        
        except socket.error as e:
            print(f"[Error Engine] Error durante el registro: {e}")
            sock_engine.close()
            sock_engine = None
            return False
    return False

def send_status_to_central(status, info):
    """
    Envía una actualización de estado (avería o recuperación) a EV_Central.
    Intenta reconectar/re-registrar si la conexión se perdió.
    """
    global sock_central, cp_id_global, current_status
    
    if not register_with_central():
        print("[Error Central] Imposible enviar estado. No se puede conectar/registrar con Central.")
        current_status = "UNKNOWN" # Perdimos conexión, no sabemos el estado
        return

    try:
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
        print(f"[Error Central] Conexión perdida con Central al enviar estado: {e}. Reintentando...")
        sock_central.close()
        sock_central = None
        current_status = "UNKNOWN"

# --- Bucle Principal ---

def health_check_loop():
    """
    Bucle principal que consulta al Engine cada segundo 
    y reporta cambios de estado a Central.
    """
    global sock_engine, current_status
    
    while True:
        time.sleep(1) # Comprobación cada segundo 
        
        new_status = "UNKNOWN"
        info = ""

        if not register_with_engine():
            # No se pudo (re)conectar al Engine
            print("\n[Monitor] Conexión con Engine perdida o rechazada. Cerrando Monitor...")
            break # Salir del bucle
        else:
            # Conectado, realizar health check
            try:
                sock_engine.sendall(b"HEALTH_CHECK")
                sock_engine.settimeout(2.0) # Esperar max 2s por respuesta
                
                response = sock_engine.recv(1024).decode('utf-8')
                
                if response == "OK":
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
                # No se recibió respuesta o se rompió la conexión 
                print(f"[Error Engine] Health check fallido: {e}")
                sock_engine.close()
                sock_engine = None
                new_status = "FAULTED"
                info = "Engine unresponsive or connection lost"
        
        # Si el estado ha cambiado, notificar a Central
        if new_status != current_status:
            print(f"[Monitor] Cambio de estado detectado: {current_status} -> {new_status}")
            current_status = new_status
            
            # Mapear estado interno a estado de Central
            central_status = "FAULTED" if new_status == "FAULTED" else "OK"
            send_status_to_central(central_status, info)
        else:
            # Imprimir un "latido" para saber que sigue vivo
            print(f"  ...Monitor {cp_id_global} status: {current_status}", end='\r')


def main():
    global engine_addr, central_addr, cp_id_global
    
    # 1. Validar argumentos 
    if len(sys.argv) != 4:
        print("Uso: python EV_CP_M.py <ip_engine:puerto_engine> <ip_central:puerto_central> <id_cp>")
        sys.exit(1)
        
    try:
        engine_host, engine_port = sys.argv[1].split(':')
        engine_addr = (engine_host, int(engine_port))
        
        central_host, central_port = sys.argv[2].split(':')
        central_addr = (central_host, int(central_port))
        
        cp_id_global = sys.argv[3]
    except ValueError:
        print("Error: Los argumentos de IP/Puerto deben estar en formato 'host:puerto'")
        sys.exit(1)
        
    print(f"--- Iniciando EV Charging Point MONITOR ---")
    print(f"  ID del CP:    {cp_id_global}")
    print(f"  Engine Addr:  {engine_addr}")
    print(f"  Central Addr: {central_addr}")
    print("-----------------------------------------")

    # 2. Conectar con Engine
    if not register_with_engine():
        print("[Error Fatal] No se pudo registrar en Engine. Abortando.")
        sys.exit(1)
    print("[Info] Registro en Engine OK (Engine confirma que está listo).")

    # Paso 2: Registrar en Central
    if not register_with_central():
        print("[Error Fatal] No se pudo completar el registro inicial con Central. Abortando.")
        sys.exit(1)
    print("[Info] Registro inicial con Central OK.")

    # 4. Iniciar bucle de monitorización
    print("[Info] Iniciando bucle de monitorización de salud...")
    health_check_loop()

if __name__ == "__main__":
    main()