import pandas as pd
import glob
import os

# Configuración de rutas
INPUT_DIR = "data/raw/"
OUTPUT_DIR = "data/processed/"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "cajamarca_raw.csv")

os.makedirs(OUTPUT_DIR, exist_ok=True)
archivos = glob.glob(os.path.join(INPUT_DIR, "*.csv"))

if not archivos:
    print(f" No se encontraron archivos en {INPUT_DIR}")
else:
    print(f" Iniciando extracción de registros de CAJAMARCA...")
    if os.path.exists(OUTPUT_FILE): os.remove(OUTPUT_FILE)

    for i, archivo in enumerate(archivos):
        nombre = os.path.basename(archivo)
        print(f"\n [%s/%s] Analizando: %s" % (i+1, len(archivos), nombre))
        
        try:
            # Forzamos separador por COMA y saltamos líneas con errores de comillas
            chunks = pd.read_csv(
                archivo, 
                chunksize=150000, 
                sep=',', 
                encoding='utf-8', 
                on_bad_lines='skip', 
                engine='c',
                low_memory=False,
                quoting=3  # Esto le dice a Python que no se confunda con las comillas internas
            )
            
            for num_chunk, chunk in enumerate(chunks):
                # Filtramos por la columna REGION (basado en tu captura)
                # 'case=False' por si acaso viene como 'Cajamarca' o 'CAJAMARCA'
                df_cajamarca = chunk[chunk['REGION'].astype(str).str.contains('CAJAMARCA', case=False, na=False)]
                
                if not df_cajamarca.empty:
                    df_cajamarca.to_csv(
                        OUTPUT_FILE, 
                        mode='a', 
                        index=False, 
                        header=not os.path.exists(OUTPUT_FILE),
                        encoding='utf-8'
                    )
                
                if num_chunk % 5 == 0:
                    print(f"   ... procesando bloque {num_chunk}")

        except Exception as e:
            print(f" Error al procesar {nombre}: {e}")

    print("\n" + "="*40)
    if os.path.exists(OUTPUT_FILE):
        print(f" ¡PROCESO COMPLETADO!")
        print(f" Tu nueva data está en: {OUTPUT_FILE}")
    else:
        print(" No se pudo generar el archivo. Verifica los nombres de las columnas.")
    print("="*40)