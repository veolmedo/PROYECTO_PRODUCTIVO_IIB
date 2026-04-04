# Pipeline de Datos - Arquitectura Medallion (End-to-End)

Este proyecto implementa un flujo completo de ingeniería de datos en un entorno **Data Lakehouse**, transformando datos brutos de salud (SIS) en un modelo analítico optimizado (Star Schema) mediante la **Estructura Medallion** (Bronze, Silver y Gold).

## Arquitectura del Proyecto

El pipeline garantiza la trazabilidad y calidad del dato a través de tres capas lógicas procesadas con **Python** y **PL/pgSQL**:

### 1. Capa Bronze (Raw / Ingesta)

- **Tabla:** `bronze.atenciones_sis_raw`
- **Proceso:** Carga desde archivos CSV mediante Python (`carga_bronze.py`).
- **Auditoría:** Inyección automática de `fecha_carga` y `fuente_archivo`.
- **Identidad:** Se añade `id_bronze SERIAL PRIMARY KEY` para asegurar la unicidad técnica de cada registro bruto.

### 2. Capa Silver (Clean / Staging)

- **Tabla:** `silver.stg_atenciones_cajamarca`
- **Proceso:** Transformación automatizada mediante el SP `silver.sp_transform_bronze_to_silver()`.
- **Lógica:** \* Limpieza y normalización de textos (`UPPER`, `TRIM`).
  - Creación de `fecha_periodo` (YYYY-MM-01) para análisis temporal.
  - Filtrado regional por Ubigeo (rango 06xxxx para Cajamarca).
  - **Linaje:** Mantiene `id_bronze_ref` para trazabilidad hacia el origen.

### 3. Capa Gold (Analytics / Star Schema)

- **Objetivo:** Modelo multidimensional optimizado para BI y Machine Learning.
- **Tablas de Dimensiones:** `dim_establecimiento`, `dim_geografia`, `dim_servicio_paciente`.
- **Tabla de Hechos:** `fact_atenciones` (contiene la métrica `atenciones_count`).
- **Técnica:** Uso de `DENSE_RANK()` para la creación de llaves subrogadas en dimensiones complejas.

---

## 🛠️ Estructura del Repositorio

```text
PROYECTO_PRODUCTIVO_IIB/
├── .secrets/              # Credenciales y .env (Excluido de Git)
├── .SIS/                  # Entorno virtual Python (Excluido de Git)
├── data/                  # Almacenamiento local de CSVs (Excluido de Git)
│   ├── raw/               # Fuentes originales
│   └── processed/         # Archivos procesados
├── scripts/               # Lógica de ejecución del Pipeline
│   ├── 01_extraer_cajamarca.py
│   └── carga_bronze.py    # Ingesta masiva con SQLAlchemy
├── .gitignore             # Configuración de exclusiones de Git
├── requirements.txt       # Dependencias del proyecto
└── README.md              # Documentación técnica
```

# Gobernanza y Control (Esquema Meta)

Se implementó un sistema de Carga Incremental (Watermarking) gestionado en la tabla **meta.pipeline_config**.

- Control: El campo **last_watermark** registra la última fecha procesada con éxito.
- Eficiencia: El Pipeline solo procesa registros en Bronze cuya fecha sea posterior al Watermark, evitando duplicados y reduciendo el consumo de recursos en la base de datos.

# Instalación y Ejecución

# 1. Preparar el Entorno

```bash
# Crear carpetas necesarias
mkdir -p data/raw data/processed .secrets

# Configurar entorno virtual
python -m venv .SIS
source .SIS/bin/activate  # (ó .SIS\Scripts\activate en Windows)

# Instalar dependencias
pip install -r requirements.txt
```

# 2. Configuración de Secretos

Crear .**secrets/.env** con la cadena de conexión de **Supabase**:

```
DB_URL=postgresql://postgres.[ID]:[PASS]@[aws-0-us-west-2.pooler.supabase.com:5432/postgres](https://aws-0-us-west-2.pooler.supabase.com:5432/postgres)
```

# 3. Ejecución del Flujo

1. Colocar el CSV en **data/raw/.**
2. Ejecutar Ingesta: **python scripts/carga_bronze.py.**
3. Ejecutar Transformación Silver/Gold: **SELECT silver.sp_transform_bronze_to_silver();** (desde la consola SQL).

# Tecnologías Utilizadas

- Lenguaje: Python 3.8+ (Pandas, SQLAlchemy).
- Base de Datos: PostgreSQL (Supabase Cloud).
- Modelado: Estructura Medallion & Star Schema.
- Control de Versiones: Git / GitHub.
