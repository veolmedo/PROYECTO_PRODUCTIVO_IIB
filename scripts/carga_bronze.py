from pathlib import Path
import pandas as pd
import sys
import logging
from sqlalchemy import text
from datetime import datetime

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

engine = get_engine()
processed_dir = raiz / "data" / "processed"


def get_db_size_mb():
    with engine.connect() as conn:
        return conn.execute(
            text("SELECT pg_database_size(current_database()) / (1024 * 1024);")
        ).scalar()


def ejecutar_carga_bronze(anio: str, usuario: str) -> None:
    proceso = nombre_proceso("carga_bronze", anio)
    ruta_csv = processed_dir / f"atenciones_cajamarca_{anio}.csv"

    if not ruta_csv.exists():
        raise FileNotFoundError(f"No existe el archivo procesado: {ruta_csv}")

    if verificar_estado_proceso(proceso, ruta_csv.name):
        return

    current_size = get_db_size_mb()
    if current_size > 400:
        logger.error(f"¡PELIGRO! Base de datos casi llena: {current_size}MB. Abortando.")
        sys.exit(1)

    print(f"\nProcesando: {ruta_csv.name} (DB Size: {current_size}MB)")
    log_id = iniciar_proceso(proceso, ruta_csv.name, usuario)

    try:
        df = pd.read_csv(ruta_csv)
        df.columns = [c.lower().replace(" ", "_").replace(".", "") for c in df.columns]
        df["fecha_carga"] = datetime.now()
        df["fuente_archivo"] = str(ruta_csv.name)

        with engine.connect() as conn:
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS bronze;"))
            df.to_sql(
                name="atenciones_sis_raw",
                con=engine,
                schema="bronze",
                if_exists="replace",
                index=False,
                chunksize=500,
            )
            conn.execute(
                text("ALTER TABLE bronze.atenciones_sis_raw ADD COLUMN id_bronze SERIAL PRIMARY KEY;")
            )
            conn.commit()

        finalizar_proceso(
            log_id,
            "EXITO",
            registros_procesados=len(df),
            detalles=f"Cargados {len(df)} registros desde {ruta_csv.name}",
        )
        print(f"Carga de {ruta_csv.name} completada.")
    except Exception as e:
        finalizar_proceso(log_id, "error", detalles=f"Error al cargar {ruta_csv.name}: {e}")
        print(f"Error al cargar {ruta_csv.name}: {e}")
        raise


if __name__ == "__main__":
    args = parse_args(require_anio=True)
    ejecutar_carga_bronze(anio=args.anio, usuario=args.usuario)
