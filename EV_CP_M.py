# Ejecución: python EV_CP_M.py localhost:10001 localhost:9090 ALC1 (python EV_CP_M.py <ip_engine:puerto> <ip_central:puerto> <id_cp>)
#
# Este script implementa "EV_CP_M" (Monitor), el módulo de monitorización
# del punto de recarga.
#
# Funcionalidades principales:
# 1. Conectarse a EV_Central (vía Socket) para autenticarse y registrar el CP.
# 2. Conectarse a EV_CP_E (vía Socket) para pasarle el ID del CP.
# 3. Entrar en un bucle de "health check" (Punto 10) que comprueba el estado
#    del Engine (EV_CP_E) cada segundo.
# 4. Reportar cualquier cambio de estado (avería 'KO' o recuperación 'OK')
#    inmediatamente a EV_Central (Punto 10).
#

import sys
import socket
import time
import json

# --- Variables Globales ---
engine_addr = None  # (host, puerto) del Engine (EV_CP_E)
central_addr = None # (host, puerto) de Central (EV_Central)
cp_id_global = None # ID de este CP (ej: "ALC1")
sock_central = None # Socket persistente para Central
sock_engine = None  # Socket persistente para Engine
current_status = "UNKNOWN" # Estado actual de salud (OK, FAULTED)

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

def register_with_central():
    """
    Intenta conectarse y registrarse en EV_Central.
    Maneja la reconexión si es necesario.
    Implementa la parte de "autenticar y registrar a los CP en la central" (Punto 240).
    """
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
    """
    Intenta conectarse y registrarse en EV_CP_E (Engine).
    El Monitor es quien "le dice" al Engine cuál es su ID.
    """
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
    """
    Envía una actualización de estado (avería o recuperación) a EV_Central.
    Implementa la notificación de "En caso de avería, notificará a CENTRAL" (Punto 10).
    """
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

# --- Bucle Principal ---

def health_check_loop():
    """
    Bucle principal que consulta al Engine cada segundo (Punto 10).
    Reporta cambios de estado (avería/recuperación) a Central.
    """
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
                    # El Engine ha reportado una avería simulada (Punto 10 / 280)
                    new_status = "FAULTED"
                    info = "Engine reported KO (Simulated Fault)"
                else:
                    # Respuesta inesperada
                    raise socket.error(f"Invalid response from Engine: {response}")
                    
            except (socket.timeout, socket.error, ConnectionResetError, BrokenPipeError) as e:
                # Fallo en la comunicación con el Engine (Timeout, conexión rota, etc.)
                # Esto se considera una AVERÍA (Punto 10 / 279)
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
    
    # 1. Validar argumentos de línea de comandos (Punto 269)
    if len(sys.argv) != 4:
        print("Uso: python EV_CP_M.py <ip_engine:puerto_engine> <ip_central:puerto_central> <id_cp>")
        sys.exit(1)
        
    try:
        # Arg 1: IP y puerto del EV_CP_E (Punto 273)
        engine_host, engine_port = sys.argv[1].split(':')
        engine_addr = (engine_host, int(engine_port))
        
        # Arg 2: IP y puerto del EV_Central (Punto 274)
        central_host, central_port = sys.argv[2].split(':')
        central_addr = (central_host, int(central_port))
        
        # Arg 3: ID del CP (Punto 276)
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
    # El Monitor se conecta al Engine (Punto 278)
    if not register_with_engine():
        print("[Error Fatal] No se pudo registrar en Engine. Abortando.")
        sys.exit(1)
    print("[Info] Registro en Engine OK (Engine confirma que está listo).")

    # 3. Conectar y registrarse en Central (EV_Central)
    # El Monitor se conecta a Central para autenticarse (Punto 277)
    if not register_with_central():
        print("[Error Fatal] No se pudo completar el registro inicial con Central. Abortando.")
        sys.exit(1)
    print("[Info] Registro inicial con Central OK.")

    # 4. Iniciar bucle de monitorización (Punto 10)
    print("[Info] Iniciando bucle de monitorización de salud...")
    health_check_loop()

if __name__ == "__main__":
    main()