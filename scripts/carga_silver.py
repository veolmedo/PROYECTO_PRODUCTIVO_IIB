from pathlib import Path
import sys
import logging
from sqlalchemy import text

raiz = Path(__file__).resolve().parent.parent
secrets_dir = raiz / ".secrets"
sys.path.append(str(raiz))
sys.path.append(str(secrets_dir))

from db_connection import get_engine
from utils.logger import iniciar_proceso, finalizar_proceso
from utils.contexto_usuario import parse_args
from utils.filtro_carga import verificar_estado_proceso, nombre_proceso

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def ejecutar_carga_silver(anio: str, usuario: str) -> None:
    """Ejecuta sp_transform_bronze_to_silver para el año indicado."""
    engine = get_engine()
    proceso = nombre_proceso("carga_silver", anio)
    archivo_origen = f"atenciones_cajamarca_{anio}.csv"

    if verificar_estado_proceso(proceso, archivo_origen):
        return

    print("\n" + "=" * 50)
    print(f"Iniciando transformación Bronze -> Silver (año {anio})")
    print("=" * 50)

    log_id = iniciar_proceso(proceso, archivo_origen, usuario)

    try:
        with engine.connect() as conn:
            logger.info("Ejecutando silver.sp_transform_bronze_to_silver()...")
            result = conn.execute(text("SELECT silver.sp_transform_bronze_to_silver();"))
            conn.commit()
            print(result)
        finalizar_proceso(
            log_id,
            "EXITO",
            detalles=f"Transformación Bronze a Silver ejecutada para año {anio}.",
        )
        print(f"Transformación a Silver completada (año {anio}).")
    except Exception as e:
        error_msg = f"Error en transformación Silver ({anio}): {e}"
        finalizar_proceso(log_id, "error", detalles=error_msg)
        logger.error(error_msg)
        print(f"Error al ejecutar carga_silver: {e}")
        sys.exit(1)


if __name__ == "__main__":
    args = parse_args(require_anio=True)
    ejecutar_carga_silver(anio=args.anio, usuario=args.usuario)
