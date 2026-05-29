import logging
import sys
import tomllib
from pathlib import Path

import pandas as pd

raiz = Path(__file__).resolve().parent.parent
sys.path.append(str(raiz))

from utils.logger import iniciar_proceso, finalizar_proceso
from utils.contexto_usuario import parse_args
from utils.filtro_carga import (
    verificar_estado_proceso,
    extraer_anio_de_archivo,
    nombre_proceso,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

CONFIG_PATH = raiz / ".secrets" / ".toml"
with open(CONFIG_PATH, "rb") as f:
    config = tomllib.load(f)

INPUT_DIR = Path(config["paths"]["data_raw"])
OUTPUT_DIR = Path(config["paths"]["processed_data"])


def filtrar_por_anio(anio: str, archivo_entrada: Path) -> tuple[Path, int]:
    """Filtra registros de Cajamarca para un año. Retorna (archivo_salida, total_registros)."""
    logger.info(f"Iniciando filtrado para el año {anio} desde {archivo_entrada.name}...")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    if not archivo_entrada.exists():
        raise FileNotFoundError(f"No se encontró el archivo: {archivo_entrada}")

    archivo = archivo_entrada
    output_file = OUTPUT_DIR / f"atenciones_cajamarca_{anio}.csv"

    if output_file.exists():
        output_file.unlink()

    chunks = pd.read_csv(
        archivo,
        chunksize=150000,
        sep=",",
        encoding="utf-8",
        on_bad_lines="skip",
        engine="c",
        low_memory=False,
        quoting=3,
    )

    total_records = 0
    for chunk in chunks:
        df_cajamarca = chunk[chunk["REGION"].astype(str).str.contains("CAJAMARCA", case=False, na=False)]
        if not df_cajamarca.empty:
            total_records += len(df_cajamarca)
            df_cajamarca.to_csv(
                output_file,
                mode="a",
                index=False,
                header=not output_file.exists(),
                encoding="utf-8",
            )

    return output_file, total_records


def _seleccionar_archivos(archivos: list[Path]) -> list[Path]:
    print("\nArchivos disponibles en data/raw:")
    for i, f in enumerate(archivos):
        print(f"  {i + 1}: {f.name}")

    seleccion = input("\nSeleccione archivos (ej: 1,2,3 o 'all'): ").strip()
    if seleccion.lower() == "all":
        return archivos
    return [archivos[int(i.strip()) - 1] for i in seleccion.split(",")]


def ejecutar_extraccion(usuario: str) -> list[str]:
    """
    Muestra CSV en data/raw, filtra por año y registra en pipeline_log.
    Retorna la lista de años a procesar en pasos downstream.
    """
    archivos = sorted(INPUT_DIR.glob("*.csv"))
    if not archivos:
        print(f"No hay archivos CSV en {INPUT_DIR}")
        return []

    archivos_seleccionados = _seleccionar_archivos(archivos)
    anios: list[str] = []

    for archivo in archivos_seleccionados:
        anio = extraer_anio_de_archivo(archivo.name)
        proceso = nombre_proceso("extraer_cajamarca", anio)

        if verificar_estado_proceso(proceso, archivo.name):
            anios.append(anio)
            continue

        log_id = iniciar_proceso(proceso, archivo.name, usuario)

        try:
            output_file, total_records = filtrar_por_anio(anio, archivo)
            finalizar_proceso(
                log_id,
                "EXITO",
                detalles=f"Año {anio}: {total_records} registros guardados en {output_file.name}",
                registros_procesados=total_records,
            )
            print(f"Extracción {anio} completada: {total_records} registros.")
            anios.append(anio)
        except Exception as e:
            finalizar_proceso(
                log_id,
                "error",
                detalles=f"Error al filtrar los datos {anio}: {e}",
            )
            logger.error(f"Fallo en año {anio}: {e}")

    return anios


if __name__ == "__main__":
    args = parse_args()
    ejecutar_extraccion(usuario=args.usuario)
