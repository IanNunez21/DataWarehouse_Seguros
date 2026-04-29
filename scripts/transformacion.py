import pandas as pd
import logging
from config import engine_staging

# Configuración de Logging para trazabilidad de calidad
logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

def limpiar_y_transformar_clientes():
    log.info("═══ PASO 2: Transformando Clientes ═══")
    
    # 1. Extracción desde Staging (ya no desde CSV)
    df = pd.read_sql("SELECT * FROM clientes", engine_staging)
    total_inicial = len(df)

    # 2. Eliminación de Duplicados
    df = df.drop_duplicates(subset=['id_cliente'], keep='last')
    log.info(f"  ✔ Duplicados eliminados: {total_inicial - len(df)}")

    # 3. Limpieza de datos (Manejo de Nulos)
    df = df.dropna(subset=['id_cliente', 'apellido', 'provincia'])

    # 4. Conversión de tipos
    df['fecha_nacimiento'] = pd.to_datetime(df['fecha_nacimiento'], errors='coerce')
    df['ingreso_anual'] = pd.to_numeric(df['ingreso_anual'], errors='coerce').fillna(0.0)
    df = df.dropna(subset=['fecha_nacimiento'])

    # 5. Normalización de formatos
    df['nombre'] = df['nombre'].str.strip().str.title()
    df['apellido'] = df['apellido'].str.strip().str.title()
    df['localidad'] = df['localidad'].str.strip().str.title()
    
    # 6. Lógica de Negocio (Atributos para el DW)
    hoy = pd.Timestamp.now()
    df['edad'] = df['fecha_nacimiento'].apply(lambda x: hoy.year - x.year)
    df['segmento_persona'] = df['edad'].apply(
        lambda e: 'Joven' if e < 35 else ('Mayor' if e >= 60 else 'Adulto')
    )

    log.info(f"  ✔ Registros limpios y listos: {len(df)}")
    return df

# Aca vamos a agregar el resto, por ejemplo: limpiar_y_transformar_polizas(), etc.