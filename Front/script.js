// URL de su API REST Central
const API_URL = 'http://localhost:5000/api/v1/status';

// Función para mapear el estado interno (IDLE, FAULTED) a texto descriptivo
function getStatusText(status) {
    switch(status) {
        case 'IDLE':
            return 'Activado / Libre';
        case 'AUTHORIZED':
            return 'Autorizado (Esperando Plug-in)';
        case 'CHARGING':
            return 'Suministrando Energía';
        case 'FAULTED':
            return '¡AVERIADO! 🔴';
        case 'STOPPED':
            return 'Parado por Central (Fuera de Servicio)';
        case 'DISCONNECTED':
            return 'Desconectado';
        default:
            return status;
    }
}

// Función principal para obtener y renderizar los datos
async function fetchAndRenderData() {
    const cpContainer = document.getElementById('cp-container');
    const requestsBody = document.querySelector('#requests-table tbody');
    const loadingMessage = document.getElementById('loading');

    try {
        const response = await fetch(API_URL);
        
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        
        const data = await response.json();
        
        // Ocultar mensaje de carga
        if (loadingMessage) loadingMessage.style.display = 'none';

        // 1. Renderizar la hora de actualización
        const timestamp = new Date(data.timestamp * 1000).toLocaleTimeString();
        document.getElementById('timestamp').textContent = `Actualizado: ${timestamp}`;

        // 2. Renderizar Puntos de Recarga (CPs)
        cpContainer.innerHTML = ''; // Limpiar bloques anteriores
        
        if (data.cp_list && data.cp_list.length > 0) {
            data.cp_list.forEach(cp => {
                // Usamos la propiedad 'status' que EV_Central debe exponer
                const status = cp.status || (cp.is_registered ? 'IDLE' : 'DISCONNECTED'); // Fallback de estado

                const cpBlock = document.createElement('div');
                // La clase CSS determina el color (IDLE, CHARGING, FAULTED, etc.)
                cpBlock.className = `cp-block ${status}`; 
                
                cpBlock.innerHTML = `
                    <h3>${cp.cp_id}</h3>
                    <p><strong>Estado:</strong> ${getStatusText(status)}</p>
                    <p><strong>Ubicación:</strong> ${cp.location}</p>
                    <p><strong>Precio:</strong> ${cp.price_kwh} €/kWh</p>
                    <p><strong>Registrado:</strong> ${cp.is_registered ? 'Sí' : 'No'}</p>
                    ${cp.driver_id ? `<p><strong>Driver:</strong> ${cp.driver_id}</p>` : ''}
                `;
                cpContainer.appendChild(cpBlock);
            });
        } else {
            cpContainer.innerHTML = '<p>No se encontraron Puntos de Recarga en la base de datos.</p>';
        }

        // 3. Renderizar Peticiones Recientes (simulado con datos de la API)
        requestsBody.innerHTML = '';
        if (data.on_going_requests) {
            data.on_going_requests.forEach(req => {
                const row = requestsBody.insertRow();
                row.insertCell().textContent = req.user_id;
                row.insertCell().textContent = req.cp_id;
                row.insertCell().textContent = req.time;
            });
        }
        
    } catch (error) {
        console.error('Error al obtener datos de la API:', error);
        cpContainer.innerHTML = `<p class="FAULTED">ERROR FATAL: No se pudo conectar con EV_API_Central (${error.message}).</p>`;
        if (loadingMessage) loadingMessage.style.display = 'none';
    }
}

// Iniciar la carga inicial de datos
fetchAndRenderData();

// Configurar la recarga automática cada 2 segundos
setInterval(fetchAndRenderData, 2000);