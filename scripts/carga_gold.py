from pathlib import Path
import sys
from sqlalchemy import text

raiz = Path(__file__).resolve().parent.parent
sys.path.append(str(raiz))
sys.path.append(str(raiz / ".secrets"))

from db_connection import get_engine
from utils.logger import iniciar_proceso, finalizar_proceso
from utils.contexto_usuario import parse_args
from utils.filtro_carga import verificar_estado_proceso, nombre_proceso


def ejecutar_carga_gold(anio: str, usuario: str) -> None:
    engine = get_engine()
    proceso = nombre_proceso("carga_gold", anio)
    archivo_origen = f"atenciones_cajamarca_{anio}.csv"

    if verificar_estado_proceso(proceso, archivo_origen):
        print(f"[{proceso}] El año {anio} ya fue procesado exitosamente.")
        return

    log_id = iniciar_proceso(proceso, archivo_origen, usuario)

    try:
        print(f"Ejecutando {proceso}...")
        with engine.begin() as conn:
            result = conn.execute(text("SELECT gold.sp_cargar_capa_gold();"))
            resultado_sp = result.scalar()
            print(f"Resultado DB: {resultado_sp}")

        finalizar_proceso(
            log_id,
            "EXITO",
            detalles=f"Carga Gold {anio} completada. Resultado: {resultado_sp}",
        )
        print(f"{proceso} finalizada con éxito.")
    except Exception as e:
        error_msg = f"Error en {proceso}: {e}"
        print(error_msg)
        finalizar_proceso(log_id, "error", detalles=error_msg)
        sys.exit(1)


if __name__ == "__main__":
    args = parse_args(require_anio=True)
    ejecutar_carga_gold(anio=args.anio, usuario=args.usuario)
