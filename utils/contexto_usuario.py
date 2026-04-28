import argparse

def obtener_usuario() -> str:
    """Solicita un usuario no vacio para registrar la ejecucion."""
    while True:
        usuario = input("Ingrese su usuario: ").strip()
        if usuario:
            return usuario
        print("El usuario no puede estar vacio. Intente nuevamente.")


def parse_args():
    parser = argparse.ArgumentParser(description="Carga archivos procesados a la capa bronze.")
    parser.add_argument("--usuario", required=True, help="Usuario que ejecuta el proceso.")
    return parser.parse_args()