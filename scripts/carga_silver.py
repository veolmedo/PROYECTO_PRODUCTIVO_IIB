from pathlib import Path
import sys
import logging
from sqlalchemy import text

# Configurar rutas para imports
raiz = Path(__file__).resolve().parent.parent
secrets_dir = raiz / ".secrets"
sys.path.append(str(raiz))
sys.path.append(str(secrets_dir))

from db_connection import get_engine
from utils.logger import iniciar_proceso, finalizar_proceso
from utils.contexto_usuario import parse_args
from utils.filtro_carga import verificar_estado_proceso
# Configurar logging local
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def ejecutar_carga_silver(usuario):
    """
    Ejecuta el procedimiento almacenado sp_transform_bronze_to_silver
    en el esquema silver de Supabase.
    """
    engine = get_engine()
    
    if verificar_estado_proceso('carga_silver'):
        return

    print("\n" + "="*50)
    print("Iniciando transformación: Bronze -> Silver")
    print("="*50)
    
    log_id = iniciar_proceso('carga_silver', 'proceso_interno_db', usuario)
    
    try:
        with engine.connect() as conn:
            # Llamar a la función sp_transform_bronze_to_silver en el esquema silver
            logger.info("Ejecutando silver.sp_transform_bronze_to_silver()...")
            # En SQLAlchemy, para funciones que no devuelven un result set directamente 
            # o que queremos ejecutar como un comando de control, usamos text()
            result = conn.execute(text("SELECT silver.sp_transform_bronze_to_silver();"))
            conn.commit()
            print(result)
        finalizar_proceso(log_id, 'EXITO', detalles="Transformación Bronze a Silver ejecutada exitosamente.")
        print("\n🚀 ¡Transformación a Silver completada con éxito!")
        
    except Exception as e:
        error_msg = f"Error en transformación Silver: {str(e)}"
        finalizar_proceso(log_id, 'error', detalles=error_msg)
        logger.error(error_msg)
        print(f"\n❌ Error al ejecutar carga_silver: {e}")
        sys.exit(1)

if __name__ == "__main__":
    args = parse_args()
    ejecutar_carga_silver(usuario=args.usuario)
