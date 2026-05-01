import pandas as pd
import logging
from config import engine_staging

# Configuración de Logging para trazabilidad de calidad
logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

def limpiar_y_transformar_garantias():
    log.info("═══ PASO 3: Transformando Garantías ═══")
    
    # 1. Extracción desde Staging
    df_g = pd.read_sql("SELECT * FROM garantias", engine_staging)
    total_inicial = len(df_g)

    # 2. Eliminación de Duplicados
    df_g = df_g.drop_duplicates(keep='last')
    log.info(f"  ✔ Duplicados eliminados exactos: {total_inicial - len(df_g)}")

    # 3. Limpieza de datos (Filtros y Nulos)
    # Filtramos para quedarnos solo con las activas. Ojo con el tipo de dato (puede ser string 'True' o booleano True)
    df_g = df_g[df_g['activa'].astype(str).str.lower() == 'true'].copy()
    df_g.drop(columns=['activa'], inplace=True)
    
    # Limpiamos valores nulos en columnas críticas
    df_g = df_g.dropna(subset=['id_poliza', 'tipo_garantia', 'suma_garantizada'])

    # 4. Conversión de tipos y normalización
    df_g['suma_garantizada'] = pd.to_numeric(df_g['suma_garantizada'], errors='coerce').fillna(0.0)
    df_g['tipo_garantia'] = df_g['tipo_garantia'].str.strip().str.capitalize()

    # 5. Lógica de Negocio (Atributos para el DW)
    # A. Extracción para Dim_TipoSiniestro (Daños) 
    dim_danio = df_g[['tipo_garantia']].drop_duplicates().rename(columns={
        'tipo_garantia': 'Nombre_Siniestro'
    }).reset_index(drop=True)
    
    # B. Agrupación para Fact_Poliza (Sumarizamos la garantía total por póliza) 
    fact_garantia_poliza = df_g.groupby('id_poliza', as_index=False)['suma_garantizada'].sum()

    # 6. Verificación de constraints (suma_garantizada > 0) [cite: 180]
    invalidos = fact_garantia_poliza[fact_garantia_poliza['suma_garantizada'] <= 0]
    if len(invalidos) > 0:
        log.warning(f"  ⚠ Atención: Hay {len(invalidos)} pólizas con suma <= 0. Serán omitidas.")
        fact_garantia_poliza = fact_garantia_poliza[fact_garantia_poliza['suma_garantizada'] > 0]

    log.info(f"  ✔ Pólizas agrupadas listas para integrar a Fact_Poliza: {len(fact_garantia_poliza)}")
    log.info(f"  ✔ Tipos de siniestros listos para Dim_TipoSiniestro: {len(dim_danio)}")
    
    return fact_garantia_poliza, dim_danio