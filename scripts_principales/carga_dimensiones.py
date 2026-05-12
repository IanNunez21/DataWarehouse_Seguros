import pandas as pd
import logging
from config import engine_staging, engine_dw
from sqlalchemy import text
 
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
    with engine_dw.connect() as conn:
        conn.execute(text("SET FOREIGN_KEY_CHECKS=0"))
        conn.execute(text("TRUNCATE TABLE dim_agente"))
        conn.execute(text("SET FOREIGN_KEY_CHECKS=1"))
        conn.commit()
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
    with engine_dw.connect() as conn:
        conn.execute(text("SET FOREIGN_KEY_CHECKS=0"))
        conn.execute(text("TRUNCATE TABLE dim_tiempo"))
        conn.execute(text("SET FOREIGN_KEY_CHECKS=1"))
        conn.commit()
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
    with engine_dw.connect() as conn:
        conn.execute(text("SET FOREIGN_KEY_CHECKS=0"))
        conn.execute(text("TRUNCATE TABLE dim_perito"))
        conn.execute(text("SET FOREIGN_KEY_CHECKS=1"))
        conn.commit()
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
    with engine_dw.connect() as conn:
        conn.execute(text("SET FOREIGN_KEY_CHECKS=0"))
        conn.execute(text("TRUNCATE TABLE dim_objeto"))
        conn.execute(text("SET FOREIGN_KEY_CHECKS=1"))
        conn.commit()
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
    with engine_dw.connect() as conn:
        conn.execute(text("SET FOREIGN_KEY_CHECKS=0"))
        conn.execute(text("TRUNCATE TABLE dim_tipo_seguro"))
        conn.execute(text("SET FOREIGN_KEY_CHECKS=1"))
        conn.commit()
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
    with engine_dw.connect() as conn:
        conn.execute(text("SET FOREIGN_KEY_CHECKS=0"))
        conn.execute(text("TRUNCATE TABLE dim_tiposiniestro"))
        conn.execute(text("SET FOREIGN_KEY_CHECKS=1"))
        conn.commit()
    df_dim.to_sql(
        name="dim_tiposiniestro",
        con=engine_dw,
        if_exists="append",
        index=False,
    )
 
    log.info(f"  ✔ dim_tiposiniestro cargada: {len(df_dim)} tipos únicos")



def _limpiar(serie: pd.Series) -> pd.Series:
    """Quita espacios, tabs, caracteres invisibles, puntos finales y valores basura."""
    BASURA = {"-", ".", "..", "000", "999", "?", "n/d", "s/d", "test", "xxx", "pendiente"}
    limpio = (
        serie
        .astype(str)
        .str.replace("\u200b", "", regex=False)  # zero-width space invisible
        .str.replace(r"\t", "", regex=True)       # tabs incrustados
        .str.strip()
        .str.rstrip(".")                          # "Buenos Aires." → "Buenos Aires"
        .str.strip()
        .str.title()
    )
    return limpio.where(
        ~limpio.str.lower().isin(BASURA | {"", "none", "nan"}),
        other=None,
    )


def cargar_dim_ubicacion():
    log.info("═══ Cargando dim_ubicacion ═══")

    # 1. Leer las dos fuentes con datos completos de ubicación.
    #    Los peritos solo tienen zona_cobertura (provincia sin ciudad ni país),
    #    no se incluyen porque generarían filas incompletas que no sirven como FK.
    clientes = pd.read_sql("SELECT pais, provincia, localidad FROM val_clientes_validados", engine_staging)
    objetos  = pd.read_sql("SELECT provincia, localidad FROM val_objetos_validados", engine_staging)

    # 2. Normalizar ANTES del concat para que "  Buenos Aires  " y "Buenos Aires"
    #    no generen dos filas distintas (el CSV de clientes tiene espacios extra).
    for df in [clientes, objetos]:
        for col in df.columns:
            df[col] = _limpiar(df[col])

    # 3. Renombrar al esquema de dim_ubicacion y completar columnas faltantes
    ub_clientes = clientes.rename(columns={"localidad": "ciudad"})

    ub_objetos = objetos.rename(columns={"localidad": "ciudad"})
    ub_objetos["pais"] = "Argentina"   # todos los objetos son nacionales

    # 4. Concatenar y deduplicar
    df_dim = pd.concat(
        [ub_clientes, ub_objetos],
        ignore_index=True,
    )[["pais", "provincia", "ciudad"]]

    df_dim = (
        df_dim
        .drop_duplicates(subset=["pais", "provincia", "ciudad"])
        .dropna(subset=["provincia", "ciudad"])
        .reset_index(drop=True)
    )

    # 5. Insertar en dim_ubicacion
    with engine_dw.connect() as conn:
        conn.execute(text("SET FOREIGN_KEY_CHECKS=0"))
        conn.execute(text("TRUNCATE TABLE dim_ubicacion"))
        conn.execute(text("SET FOREIGN_KEY_CHECKS=1"))
        conn.commit()
    df_dim.to_sql(
        name="dim_ubicacion",
        con=engine_dw,
        if_exists="append",
        index=False,
    )

    log.info(f"  ✔ dim_ubicacion cargada: {len(df_dim)} filas únicas")


def cargar_dim_personas():
    log.info("═══ Cargando dim_personas (Clientes + Terceros) ═══")

    # --- 1. PROCESAR CLIENTES PROPIOS ---
    df = pd.read_sql(
        "SELECT id_cliente, segmento_persona, sexo, situacion_laboral FROM val_clientes_validados",
        engine_staging
    )

    df_dim = df.rename(columns={
        "id_cliente":        "id_persona",
        "segmento_persona":  "segmento_persona",
        "sexo":              "sexo",
        "situacion_laboral": "ocupacion",
    }).copy()

    df_dim["es_tercero"] = False
    df_dim["fecha_desde"] = pd.Timestamp.today().normalize()
    df_dim["fecha_hasta"] = None
    df_dim["es_actual"]   = 1

    # Resolver id_ubicacion_fk haciendo JOIN con dim_ubicacion
    dim_ub = pd.read_sql("SELECT id_ubicacion_sk, provincia, ciudad FROM dim_ubicacion", engine_dw)
    dim_ub["provincia"] = dim_ub["provincia"].str.strip().str.title()
    dim_ub["ciudad"]    = dim_ub["ciudad"].str.strip().str.title()

    df_clientes = pd.read_sql("SELECT id_cliente, provincia, localidad FROM val_clientes_validados", engine_staging)
    df_clientes["provincia"] = df_clientes["provincia"].str.strip().str.title()
    df_clientes["localidad"] = df_clientes["localidad"].str.strip().str.title()

    df_clientes = df_clientes.merge(
        dim_ub, left_on=["provincia", "localidad"], right_on=["provincia", "ciudad"], how="left"
    )[["id_cliente", "id_ubicacion_sk"]]

    df_dim = df_dim.merge(
        df_clientes.rename(columns={"id_cliente": "id_persona"}), on="id_persona", how="left"
    )
    df_dim = df_dim.rename(columns={"id_ubicacion_sk": "id_ubicacion_fk"})

    # --- 2. PROCESAR TERCEROS ---
    partes = pd.read_sql("SELECT DISTINCT id_receptor_pago FROM val_partes_validados", engine_staging)
    
    # Nos aseguramos de no duplicar si el script se vuelve a correr
    try:
        personas_db = pd.read_sql("SELECT id_persona FROM dim_personas", engine_dw)
        clientes_existentes = set(personas_db["id_persona"]).union(set(df_dim["id_persona"]))
    except Exception:
        clientes_existentes = set(df_dim["id_persona"])

    terceros = partes[~partes["id_receptor_pago"].isin(clientes_existentes)].copy()
    terceros = terceros.rename(columns={"id_receptor_pago": "id_persona"})

    if not terceros.empty:
        terceros["ocupacion"]        = None
        terceros["segmento_persona"] = None
        terceros["sexo"]             = None
        terceros["id_ubicacion_fk"]  = None
        terceros["es_tercero"]       = True
        terceros["fecha_desde"]      = pd.Timestamp.today().normalize()
        terceros["fecha_hasta"]      = None
        terceros["es_actual"]        = 1
        
        # --- 3. UNIFICAR ---
        df_dim_final = pd.concat([df_dim, terceros], ignore_index=True)
    else:
        df_dim_final = df_dim

    # --- 4. INSERTAR EN LA BASE DE DATOS ---
    with engine_dw.connect() as conn:
        conn.execute(text("SET FOREIGN_KEY_CHECKS=0"))
        conn.execute(text("TRUNCATE TABLE dim_personas"))
        conn.execute(text("SET FOREIGN_KEY_CHECKS=1"))
        conn.commit()
    df_dim_final.to_sql(
        name="dim_personas",
        con=engine_dw,
        if_exists="append",
        index=False,
    )

    log.info(f"  ✔ dim_personas cargada: {len(df_dim)} clientes y {len(terceros) if not terceros.empty else 0} terceros ({len(df_dim_final)} total)")

