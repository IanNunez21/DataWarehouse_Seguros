import os
import pandas as pd
from sqlalchemy import create_engine

# ─────────────────────────────────────────────
#  CONFIGURACIÓN DE CONEXIONES
# ─────────────────────────────────────────────

USER = "root"
PASSWORD = "root"
HOST = "localhost"

# Conexión a la zona de laboratorio (Staging)
URL_STAGING = f"mysql+mysqlconnector://{USER}:{PASSWORD}@{HOST}/dw_staging"
engine_staging = create_engine(URL_STAGING)

# Conexión al Data Warehouse final (para los siguientes pasos)
URL_DW = f"mysql+mysqlconnector://{USER}:{PASSWORD}@{HOST}/data_warehouse_seguros"
engine_dw = create_engine(URL_DW)

# Carpeta donde se encuentran los archivos fuente
CSV_DIR = "./SourcesSeguros"

# ─────────────────────────────────────────────
#  PROCESO DE CARGA AL STAGING AREA
# ─────────────────────────────────────────────

def cargar_staging_area():
    print("═══ Iniciando Carga de Staging Area ═══")
    
    # Definimos los archivos que vamos a procesar
    fuentes = {
        "clientes": "clientes.csv",
        "agentes": "agentes.csv",
        "peritos": "peritos.csv",
        "polizas": "polizas.csv",
        "objetos": "objetos_asegurados.csv",
        "indicadores": "indicadores_fraude.csv",
        "healthinsurance": "HealthInsurance.csv",
        "garantias": "garantias.csv",
        "partes": "partes_accidente.csv",
        "autoinsurance": "AutoInsurance.csv",
        "evaluaciones": "evaluaciones_perito.csv",
        "pagos": "pagos_siniestro.csv"
    }

    for nombre_tabla, nombre_archivo in fuentes.items():
        ruta = os.path.join(CSV_DIR, nombre_archivo)
        
        if not os.path.exists(ruta):
            print(f"  ⚠  No se encontró el archivo: {nombre_archivo}")
            continue

        # Lectura del archivo
        df = pd.read_csv(ruta, low_memory=False)
        
        # Normalización básica de nombres de columnas
        # (minúsculas, sin espacios, sin caracteres especiales)
        df.columns = [c.strip().lower().replace(" ", "_").replace(".", "") for c in df.columns]

        # Carga en el esquema dw_staging
        # if_exists="replace" permite que el proceso sea repetible (idempotente)
        df.to_sql(nombre_tabla, engine_staging, if_exists="replace", index=False)
        
        print(f"  ✔  Tabla '{nombre_tabla}' cargada en dw_staging ({len(df)} filas)")

if __name__ == "__main__":
    cargar_staging_area()
    print("\nProceso de Staging finalizado correctamente.")