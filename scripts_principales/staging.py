import os
import pandas as pd
from config import engine_staging

CSV_DIR = "./SourcesSeguros"

def cargar_staging_area():
    print("═══ PASO 1: Cargando Staging Area (Datos Crudos) ═══")
    
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
            print(f"  ⚠ Archivo omitido: {nombre_archivo}")
            continue

        df = pd.read_csv(ruta, low_memory=False)
        # Normalización técnica de columnas
        df.columns = [c.strip().lower().replace(" ", "_").replace(".", "") for c in df.columns]

        # Carga idempotente (replace) en dw_staging
        df.to_sql(nombre_tabla, engine_staging, if_exists="replace", index=False)
        print(f"  ✔ Tabla '{nombre_tabla}' cargada ({len(df)} filas)")