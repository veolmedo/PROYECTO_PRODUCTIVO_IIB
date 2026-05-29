import re
from pathlib import Path
import sys
from sqlalchemy import text
# Asegúrate de que las rutas ya estén en sys.path como lo haces en tu script
from db_connection import get_engine

_ANIO_PATTERN = re.compile(r"(19|20)\d{2}")


def extraer_anio_de_archivo(nombre_archivo: str) -> str:
    """Extrae el año (4 dígitos) del nombre de un archivo."""
    match = _ANIO_PATTERN.search(Path(nombre_archivo).stem)
    if not match:
        raise ValueError(f"No se encontró un año válido en el nombre: {nombre_archivo}")
    return match.group(0)


def nombre_proceso(base: str, anio: str) -> str:
    """Construye el nombre de proceso con sufijo de año (ej. carga_bronze_2017)."""
    return f"{base}_{anio}"


def verificar_estado_proceso(nombre_proceso, archivo_origen=None):
    """
    Verifica si un proceso específico ya terminó con éxito.
    Devuelve True si el estado es 'EXITO', de lo contrario False.
    """
    engine = get_engine()
    try:
        with engine.connect() as conn:
            sql = """ 
                SELECT estado FROM meta.pipeline_log
                WHERE proceso = :proceso
            """
            params = {"proceso": nombre_proceso}
            
            if archivo_origen:
                sql += " AND archivo_origen = :archivo "
                params["archivo"] = archivo_origen
                
            sql += " ORDER BY fecha_inicio DESC LIMIT 1 "
            
            query = text(sql)
            result = conn.execute(query, params)
            registro = result.first() # Obtenemos solo el más reciente
            
            if registro:
                if registro.estado == "EXITO":
                    msg = f"[{nombre_proceso}]"
                    if archivo_origen:
                        msg += f" para {archivo_origen}"
                    print(f"{msg}: Ya registrado con éxito anteriormente.")
                    return True
                else:
                    print(f"[{nombre_proceso}]: Estado actual -> {registro.estado}")
                    return False
            else:
                print(f"[{nombre_proceso}]: Sin registros previos. Iniciando...")
                return False
                
    except Exception as e:
        print(f"Error al verificar base de datos: {e}")
        return False # Ante la duda, intentamos procesar o manejamos el error