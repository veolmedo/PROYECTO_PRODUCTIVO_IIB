
from pathlib import Path
from sqlalchemy import text
import sys

raiz = Path(__file__).resolve().parent.parent
sys.path.append(str(raiz / ".secrets"))

from db_connection import get_engine

engine = get_engine()


def limpiar_tablas():
    try:
        with engine.begin() as conn:
            # consulta truncate
            query = text(""" 
                TRUNCATE TABLE bronze.atenciones_sis_raw RESTART IDENTITY CASCADE;
                TRUNCATE TABLE silver.stg_atenciones_cajamarca RESTART IDENTITY CASCADE;
            """)
            result = conn.execute(query)
            print(result)
    except Exception as e:
        print(e)
        sys.exit(1)