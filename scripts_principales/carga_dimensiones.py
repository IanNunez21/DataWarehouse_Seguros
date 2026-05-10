import pandas as pd
import logging
from config import engine_staging, engine_dw
 
logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)
 
 
def cargar_dim_agente():
    log.info("═══ Cargando dim_agente ═══")
 
    # 1. Leer la tabla validada desde staging
    df = pd.read_sql("SELECT * FROM val_agentes_validados", engine_staging)
    total = len(df)
 
    # 2. Seleccionar y renombrar solo las columnas que existen en dim_agente
    #    id_agente_sk  → surrogate key, se genera por AUTO_INCREMENT en MySQL (no la insertamos)
    #    nombre_agente → viene de nombre_completo en la tabla validada
    df_dim = df[["id_agente", "nombre_completo"]].copy()
    df_dim = df_dim.rename(columns={"nombre_completo": "nombre_agente"})

 
    # 3. Insertar en dim_agente
    #    if_exists="append" para no destruir registros existentes.
    #    index=False porque la SK la genera MySQL con AUTO_INCREMENT.
    df_dim.to_sql(
        name="dim_agente",
        con=engine_dw,
        if_exists="append",
        index=False,
    )
 
    log.info(f"  ✔ dim_agente cargada: {len(df_dim)} registros de {total} validados")

def cargar_dim_perito():
    log.info("═══ Cargando dim_perito ═══")
 
    # 1. Leer la tabla validada desde staging
    df = pd.read_sql("SELECT * FROM val_peritos_validados", engine_staging)
    total = len(df)
 
    # 2. Seleccionar y renombrar solo las columnas que existen en dim_perito
    df_dim = df[["id_perito", "nombre_completo"]].copy()
    df_dim = df_dim.rename(columns={"nombre_completo": "Nombre_Perito"})
 
    # 3. Insertar en dim_perito
    df_dim.to_sql(
        name="dim_perito",
        con=engine_dw,
        if_exists="append",
        index=False,
    )
 
    log.info(f"  ✔ dim_perito cargada: {len(df_dim)} registros de {total} validados") 

def cargar_dim_objeto():
    log.info("═══ Cargando dim_objeto ═══")
 
    # 1. Leer la tabla validada desde staging
    df = pd.read_sql("SELECT * FROM val_objetos_validados", engine_staging)
    total = len(df)
 
    # 2. Seleccionar y renombrar solo las columnas que existen en dim_objeto
    df_dim = df[["id_objeto", "tipo_objeto", "valor_asegurado"]].copy()
    df_dim = df_dim.rename(columns={"valor_asegurado": "valor_objeto"})
 
    # 3. Insertar en dim_objeto
    df_dim.to_sql(
        name="dim_objeto",
        con=engine_dw,
        if_exists="append",
        index=False,
    )
 
    log.info(f"  ✔ dim_objeto cargada: {len(df_dim)} registros de {total} validados")

def cargar_dim_tipo_seguro():
    log.info("═══ Cargando dim_tipo_seguro ═══")
 
    # 1. Leer los valores únicos desde staging
    df = pd.read_sql("SELECT DISTINCT cobertura FROM val_polizas_validadas", engine_staging)
 
    # 2. Mapear valores a los aceptados por el ENUM en la base de datos
    mapeo = {
        'EXTENDIDA': 'Estandar',
        'BASICA': 'Basico',
        'PREMIUM': 'Premium'
    }
    df['categoria_plan'] = df['cobertura'].map(mapeo)
    
    # Nos quedamos solo con la columna objetivo
    df_dim = df[['categoria_plan']].dropna().copy()
 
    # 3. Insertar en dim_tipo_seguro
    df_dim.to_sql(
        name="dim_tipo_seguro",
        con=engine_dw,
        if_exists="append",
        index=False,
    )
 
    log.info(f"  ✔ dim_tipo_seguro cargada: {len(df_dim)} registros únicos de planes")