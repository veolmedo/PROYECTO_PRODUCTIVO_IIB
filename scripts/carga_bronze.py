from pathlib import Path
import pandas as pd
import sys
from sqlalchemy import create_engine, text
from datetime import datetime

#importando coneccion db
ruta_env = Path(__file__).resolve().parent.parent / ".secrets"

raiz = Path(__file__).resolve().parent.parent / ".secrets"
sys.path.append(str(raiz))

from db_connection import get_engine

engine = get_engine()

# ruta data
ruta_csv = Path(__file__).resolve().parent.parent / "data" / "processed" / "cajamarca_raw.csv"

# Cargar los datos desde el CSV
df = pd.read_csv(ruta_csv)
df.columns = [c.lower().replace(" ", "_").replace(".", "") for c in df.columns]
df['fecha_carga'] = datetime.now()
df['fuente_archivo'] = str(ruta_csv.name)

try:
    with engine.connect() as conn:

        
        # Esto confirma que el "túnel" a la base de datos está abierto
        print("✅ Conexión a la base de datos exitosa (vía SQLAlchemy Engine).")
        # print(f"⏳ Iniciando carga de {len(df)} filas...")

        # --- ESPACIO PARA LA CARGA DE DATOS BRONZE ---
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS bronze;"))
        conn.commit()

        # 3. Cargar a la base de datos
        df.to_sql(
            name='atenciones_sis_raw', 
            con=engine, 
            schema='bronze', 
            if_exists='replace', # 'replace' sobrescribe la tabla, 'append' agrega datos
            index=False,         # No queremos que el índice de Pandas sea una columna
            chunksize=500        # Envía los datos en bloques de 500 para no saturar Supabase
        )
        print("🚀 ¡Carga a 'bronze.atenciones_sis_raw' completada con éxito!")

        
        # --- FIN ESPACIO PARA LA CARGA DE DATOS BRONZE ---

except Exception as e:
    print(f"❌ No se pudo conectar a la base de datos.")
    print(f"Error técnico: {e}")