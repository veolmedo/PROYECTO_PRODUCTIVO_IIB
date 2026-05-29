import sys
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = RAIZ / "scripts"
sys.path.insert(0, str(RAIZ))
sys.path.insert(0, str(SCRIPTS_DIR))

from utils.contexto_usuario import obtener_usuario
from extraer_cajamarca import ejecutar_extraccion
from carga_bronze import ejecutar_carga_bronze
from carga_silver import ejecutar_carga_silver
from carga_gold import ejecutar_carga_gold
from limpiar_tablas import limpiar_tablas

def run_pipeline(usuario: str) -> None:
    anios = ejecutar_extraccion(usuario)
    if not anios:
        print("No hay años para procesar en las capas siguientes.")
        return

    for anio in anios:
        print(f"\n--- Pipeline año {anio} ---")
        ejecutar_carga_bronze(anio, usuario)
        ejecutar_carga_silver(anio, usuario)
        ejecutar_carga_gold(anio, usuario)
        limpiar_tablas()

    print("\nPipeline completado.")


if __name__ == "__main__":
    try:
        usuario = obtener_usuario()
        run_pipeline(usuario)
    except Exception as e:
        print(f"Error en orquestador: {e}")
        sys.exit(1)
