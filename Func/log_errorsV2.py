import logging
import socket
from datetime import datetime
import os

# Obtener nombre del equipo
nombre_equipo = socket.gethostname()

# Obtener fecha y hora actual formateada
fecha_hora = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

# Construir nombre del archivo
nombre_archivo_log = f"{nombre_equipo}_{fecha_hora}.txt"

# Ruta absoluta opcional (puede usar una carpeta "logs" si querés)
# ruta_log = os.path.join("logs", nombre_archivo_log)

# Configurar el archivo donde se guardarán los errores
os.makedirs("logs", exist_ok=True)
nombre_archivo_log = os.path.join("logs", f"{nombre_equipo}_{fecha_hora}.txt")

logging.basicConfig(
    filename=nombre_archivo_log,
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def log_error(error_message, function_name=None):
    if function_name:
        logging.error(f"{function_name}: {error_message}")
    else:
        logging.error(error_message)
