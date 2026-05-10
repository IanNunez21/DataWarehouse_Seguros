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

 
    # 4. Insertar en dim_agente
    #    if_exists="append" para no destruir registros existentes.
    #    index=False porque la SK la genera MySQL con AUTO_INCREMENT.
    df_dim.to_sql(
        name="dim_agente",
        con=engine_dw,
        if_exists="append",
        index=False,
    )
 
    log.info(f"  ✔ dim_agente cargada: {len(df_dim)} registros de {total} validados")
 
def cargar_dim_tiempo():
    log.info("═══ Cargando dim_tiempo ═══")
 
    # 1. Rango definido por las fechas reales de negocio en los CSVs:
    #    - Más antigua útil: 2005-02-23 (fecha_ingreso agentes)
    #    - Más reciente:     2026-07-10 (fecha_pago pagos)
    #    Se usa margen completo de año para no truncar ningún dato.
    fecha_inicio = pd.Timestamp("2005-01-01")
    fecha_fin    = pd.Timestamp("2026-12-31")
 
    # 2. Generar una fila por cada día del rango
    rango = pd.date_range(start=fecha_inicio, end=fecha_fin, freq="D")
 
    df_dim = pd.DataFrame({
        # id_tiempo como entero YYYYMMDD — es la FK que usarán las tablas de hechos
        "id_tiempo": rango.strftime("%Y%m%d").astype(int),
        "Dia":     rango.day,
        "Mes":     rango.month,
        "Anio":    rango.year,
    })
 
    # 3. Insertar en dim_tiempo
    df_dim.to_sql(
        name="dim_tiempo",
        con=engine_dw,
        if_exists="append",
        index=False,
    )
 
    log.info(f"  ✔ dim_tiempo cargada: {len(df_dim)} días ({fecha_inicio.date()} → {fecha_fin.date()})")

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

def cargar_dim_tiposiniestro():
    log.info("═══ Cargando dim_tiposiniestro ═══")
 
    # 1. Leer tipo_siniestro desde val_partes_validados
    #    Es la única fuente que tiene los tipos de siniestro ya normalizados
    df = pd.read_sql("SELECT tipo_siniestro FROM val_partes_validados", engine_staging)
 
    # 2. Extraer valores únicos y descartar nulos
    df_dim = (
        df["tipo_siniestro"]
        .dropna()
        .str.strip()
        .unique()
    )
    df_dim = pd.DataFrame({"Nombre_Siniestro": sorted(df_dim)})
 
    # 3. Insertar en dim_tiposiniestro
    #    id_tipo_siniestro_sk lo genera MySQL con AUTO_INCREMENT
    df_dim.to_sql(
        name="dim_tiposiniestro",
        con=engine_dw,
        if_exists="append",
        index=False,
    )
 
    log.info(f"  ✔ dim_tiposiniestro cargada: {len(df_dim)} tipos únicos")