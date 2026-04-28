import sys
import subprocess
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(SCRIPT_DIR))

from utils.contexto_usuario import obtener_usuario


def run_script(script_name, usuario):
    script_path = SCRIPT_DIR / "scripts" / script_name
    result = subprocess.run([sys.executable, str(script_path), "--usuario", usuario])
    if result.returncode != 0:
        raise RuntimeError(f"Fallo la ejecución de {script_name}")

if __name__ == "__main__":
    try:
        usuario = obtener_usuario()
        print("Iniciando extracción de CAJAMARCA...")
        run_script("extraer_cajamarca.py", usuario)
        print("Extracción finalizada. Iniciando carga Bronze...")
        run_script("carga_bronze.py", usuario)
        print("carga a bronze finalizada")
        run_script("carga_silver.py", usuario)
        print("Pipeline completado.")
    except Exception as e:
        print(f"Error en orquestador: {e}")
        sys.exit(1)
