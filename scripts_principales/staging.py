import os
import logging
import pandas as pd
from config import engine_staging
from sqlalchemy import text

log = logging.getLogger(__name__)

CSV_DIR = "./SourcesSeguros"

def cargar_staging_area():
    fuentes = {
        "clientes":       "clientes.csv",
        "agentes":        "agentes.csv",
        "peritos":        "peritos.csv",
        "polizas":        "polizas.csv",
        "objetos":        "objetos_asegurados.csv",
        "indicadores":    "indicadores_fraude.csv",
        "healthinsurance":"HealthInsurance.csv",
        "garantias":      "garantias.csv",
        "partes":         "partes_accidente.csv",
        "autoinsurance":  "AutoInsurance.csv",
        "evaluaciones":   "evaluaciones_perito.csv",
        "pagos":          "pagos_siniestro.csv"
    }

    for nombre_tabla, nombre_archivo in fuentes.items():
        ruta = os.path.join(CSV_DIR, nombre_archivo)
        
        if not os.path.exists(ruta):
            log.warning(f"  {nombre_tabla:<18}: archivo no encontrado, omitido")
            continue

        df = pd.read_csv(ruta, low_memory=False, encoding='latin-1')
        df.columns = [c.strip().lower().replace(" ", "_").replace(".", "") for c in df.columns]

        df.to_sql(nombre_tabla, engine_staging, if_exists="replace", index=False)
        log.info(f"  {nombre_tabla:<18}: {len(df):>6,} filas")


def crear_indices_staging():
    #Crea índices sobre las columnas de clave natural en las tablas curadas (val_*). Acelera los lookups y JOINs que hace carga_hechos.py.
    indices = [
        ("val_clientes_validados",   "id_cliente"),
        ("val_agentes_validados",    "id_agente"),
        ("val_peritos_validados",    "id_perito"),
        ("val_polizas_validadas",    "id_poliza"),
        ("val_polizas_validadas",    "id_objeto_asegurado"),
        ("val_objetos_validados",    "id_objeto"),
        ("val_partes_validados",     "id_parte"),
        ("val_partes_validados",     "id_poliza_fk"),
        ("val_garantias_validadas",  "id_poliza"),
        ("val_pagos_validados",      "id_siniestro_fk"),
    ]

    with engine_staging.connect() as conn:
        for tabla, columna in indices:
            nombre_idx = f"idx_{tabla}_{columna}"
            sql = f"CREATE INDEX IF NOT EXISTS {nombre_idx} ON {tabla} ({columna})"
            try:
                conn.execute(text(sql))
            except Exception:
                pass
        conn.commit()

    log.info("---Indices de staging verificados---")

