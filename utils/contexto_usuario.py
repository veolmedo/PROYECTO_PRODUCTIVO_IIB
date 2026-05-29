import argparse

def obtener_usuario() -> str:
    """Solicita un usuario no vacio para registrar la ejecucion."""
    while True:
        usuario = input("Ingrese su usuario: ").strip()
        if usuario:
            return usuario
        print("El usuario no puede estar vacio. Intente nuevamente.")


def parse_args(require_anio: bool = False):
    parser = argparse.ArgumentParser(description="Pipeline SIS - ejecución por año.")
    parser.add_argument("--usuario", required=True, help="Usuario que ejecuta el proceso.")
    parser.add_argument("--anio", required=require_anio, help="Año a procesar (ej. 2017).")
    return parser.parse_args()