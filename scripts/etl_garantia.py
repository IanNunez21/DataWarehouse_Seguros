import pandas as pd
import logging
from config import engine_staging

# Configuración de Logging para trazabilidad
logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

def limpiar_y_transformar_garantias():
    log.info("═══ PASO 1: Transformando Garantías ═══")
    
    # Asegurate de que la ruta al archivo sea correcta. 
# Si tu csv está en la carpeta principal de tu proyecto poné esto:
    df_g = pd.read_csv("SourcesSeguros/garantias.csv") 

# (Si lo tenés en una carpeta llamada 'data', sería "data/garantias.csv")
    total_inicial = len(df_g)

    df_g = df_g.drop_duplicates(keep='last')
    log.info(f"  ✔ Duplicados eliminados exactos: {total_inicial - len(df_g)}")

    df_g = df_g[df_g['activa'].astype(str).str.lower() == 'true'].copy()
    df_g.drop(columns=['activa'], inplace=True)
    
    df_g = df_g.dropna(subset=['id_poliza', 'tipo_garantia', 'suma_garantizada'])

    df_g['suma_garantizada'] = pd.to_numeric(df_g['suma_garantizada'], errors='coerce').fillna(0.0)
    df_g['tipo_garantia'] = df_g['tipo_garantia'].str.strip().str.capitalize()

    dim_danio = df_g[['tipo_garantia']].drop_duplicates().rename(columns={
        'tipo_garantia': 'Nombre_Siniestro'
    }).reset_index(drop=True)
    
    fact_garantia_poliza = df_g.groupby('id_poliza', as_index=False)['suma_garantizada'].sum()

    invalidos = fact_garantia_poliza[fact_garantia_poliza['suma_garantizada'] <= 0]
    if len(invalidos) > 0:
        log.warning(f"  ⚠ Atención: Hay {len(invalidos)} pólizas con suma <= 0. Serán omitidas.")
        fact_garantia_poliza = fact_garantia_poliza[fact_garantia_poliza['suma_garantizada'] > 0]

    return fact_garantia_poliza, dim_danio

def cargar_staging_garantias(fact_garantia_poliza, dim_danio):
    log.info("═══ PASO 2: Cargando en Tablas Intermedias (Staging) ═══")
    try:
        fact_garantia_poliza.to_sql('stg_fact_garantia_poliza', con=engine_staging, if_exists='replace', index=False)
        log.info("  ✔ Tabla 'stg_fact_garantia_poliza' cargada con éxito en Staging.")

        dim_danio.to_sql('stg_dim_danio', con=engine_staging, if_exists='replace', index=False)
        log.info("  ✔ Tabla 'stg_dim_danio' cargada con éxito en Staging.")
    except Exception as e:
        log.error(f"  ❌ Error al cargar en la base de datos: {e}")

# Bloque principal de ejecución
if __name__ == "__main__":
    fact_gp, dim_d = limpiar_y_transformar_garantias()
    cargar_staging_garantias(fact_gp, dim_d)