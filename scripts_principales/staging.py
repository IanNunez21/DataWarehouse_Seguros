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


def crear_indices_staging():
    """
    Crea índices sobre las columnas de clave natural en las tablas curadas (val_*)
    de dw_staging. Se llama después del paso de transformación, una vez que las
    tablas ya fueron creadas por to_sql().

    Beneficio: acelera los lookups y JOINs que hace carga_hechos.py al resolver
    las Surrogate Keys, y los filtros isin() que se resuelven en MySQL.
    """
    from sqlalchemy import text

    indices = [
        # tabla                      columna a indexar
        ("val_clientes_validados",   "id_cliente"),
        ("val_agentes_validados",    "id_agente"),
        ("val_peritos_validados",    "id_perito"),
        ("val_polizas_validadas",    "id_poliza"),
        ("val_polizas_validadas",    "id_objeto_asegurado"),  # usado en lookup objetos
        ("val_objetos_validados",    "id_objeto"),
        ("val_partes_validados",     "id_parte"),
        ("val_partes_validados",     "id_poliza_fk"),         # JOIN con pólizas
        ("val_garantias_validadas",  "id_poliza"),            # GROUP BY en fact_poliza
        ("val_pagos_validados",      "id_siniestro_fk"),      # JOIN con siniestros
    ]

    with engine_staging.connect() as conn:
        for tabla, columna in indices:
            nombre_idx = f"idx_{tabla}_{columna}"
            sql = f"CREATE INDEX IF NOT EXISTS {nombre_idx} ON {tabla} ({columna})"
            try:
                conn.execute(text(sql))
            except Exception:
                # MySQL no soporta IF NOT EXISTS en CREATE INDEX (versiones < 8.0.x)
                # Si ya existe, simplemente ignoramos el error
                pass
        conn.commit()

    print("  ✔ Índices de staging creados/verificados")