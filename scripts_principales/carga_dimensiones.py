import pandas as pd
import logging
from config import engine_staging, engine_dw
from sqlalchemy import text
import hashlib
 
logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)
 
 
def cargar_dim_agente():
 
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
    # 4. Identificar nuevos e Insertar
    try:
        existentes = pd.read_sql("SELECT id_agente FROM dim_agente", engine_dw)["id_agente"].tolist()
    except Exception:
        existentes = []
        
    df_dim = df_dim[~df_dim["id_agente"].isin(existentes)]

    if not df_dim.empty:
        df_dim.to_sql(name="dim_agente", con=engine_dw, if_exists="append", index=False)
 
    log.info(f"  ✔ dim_agente: {len(df_dim)} registros nuevos (de {total} validados)")
 
def cargar_dim_tiempo():
 
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
    # 3. Insertar en dim_tiempo (Solo nuevas fechas)
    try:
        existentes = pd.read_sql("SELECT id_tiempo FROM dim_tiempo", engine_dw)["id_tiempo"].tolist()
    except Exception:
        existentes = []
        
    df_dim = df_dim[~df_dim["id_tiempo"].isin(existentes)]

    if not df_dim.empty:
        df_dim.to_sql(name="dim_tiempo", con=engine_dw, if_exists="append", index=False)
 
    log.info(f"  ✔ dim_tiempo: {len(df_dim)} fechas nuevas ({fecha_inicio.date()} → {fecha_fin.date()})")

def cargar_dim_perito():
 
    # 1. Leer la tabla validada desde staging
    df = pd.read_sql("SELECT * FROM val_peritos_validados", engine_staging)
    total = len(df)
 
    # 2. Seleccionar y renombrar solo las columnas que existen en dim_perito
    df_dim = df[["id_perito", "nombre_completo"]].copy()
    df_dim = df_dim.rename(columns={"nombre_completo": "Nombre_Perito"})
 
    # 3. Insertar en dim_perito
    # 3. Identificar e Insertar en dim_perito
    try:
        existentes = pd.read_sql("SELECT id_perito FROM dim_perito", engine_dw)["id_perito"].tolist()
    except Exception:
        existentes = []
        
    df_dim = df_dim[~df_dim["id_perito"].isin(existentes)]

    if not df_dim.empty:
        df_dim.to_sql(name="dim_perito", con=engine_dw, if_exists="append", index=False)
 
    log.info(f"  ✔ dim_perito: {len(df_dim)} registros nuevos (de {total} validados)")


def cargar_dim_objeto():
 
    # 1. Leer la tabla validada desde staging
    df = pd.read_sql("SELECT * FROM val_objetos_validados", engine_staging)
    total = len(df)
 
    # 2. Seleccionar y renombrar solo las columnas que existen en dim_objeto
    df_dim = df[["id_objeto", "tipo_objeto", "valor_asegurado"]].copy()
    df_dim = df_dim.rename(columns={"valor_asegurado": "valor_objeto"})
 
    # 3. Insertar en dim_objeto
    # 3. Insertar en dim_objeto
    try:
        existentes = pd.read_sql("SELECT id_objeto FROM dim_objeto", engine_dw)["id_objeto"].tolist()
    except Exception:
        existentes = []
        
    df_dim = df_dim[~df_dim["id_objeto"].isin(existentes)]

    if not df_dim.empty:
        df_dim.to_sql(name="dim_objeto", con=engine_dw, if_exists="append", index=False)
 
    log.info(f"  ✔ dim_objeto: {len(df_dim)} registros nuevos (de {total} validados)")

def cargar_dim_tipo_seguro():
 
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
    # 3. Insertar en dim_tipo_seguro
    try:
        existentes = pd.read_sql("SELECT categoria_plan FROM dim_tipo_seguro", engine_dw)["categoria_plan"].tolist()
    except Exception:
        existentes = []
        
    df_dim = df_dim[~df_dim["categoria_plan"].isin(existentes)]

    if not df_dim.empty:
        df_dim.to_sql(name="dim_tipo_seguro", con=engine_dw, if_exists="append", index=False)
 
    log.info(f"  ✔ dim_tipo_seguro: {len(df_dim)} registros nuevos")

def cargar_dim_tiposiniestro():
 
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
    # 3. Insertar en dim_tiposiniestro
    try:
        existentes = pd.read_sql("SELECT Nombre_Siniestro FROM dim_tiposiniestro", engine_dw)["Nombre_Siniestro"].tolist()
    except Exception:
        existentes = []
        
    df_dim = df_dim[~df_dim["Nombre_Siniestro"].isin(existentes)]

    if not df_dim.empty:
        df_dim.to_sql(name="dim_tiposiniestro", con=engine_dw, if_exists="append", index=False)
 
    log.info(f"  ✔ dim_tiposiniestro: {len(df_dim)} tipos nuevos")



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
    if not terceros.empty:
        df_dim_final = pd.concat([df_dim, terceros], ignore_index=True)
    else:
        df_dim_final = df_dim

    # --- 4. APLICAR SCD TIPO 2 (CARGA INCREMENTAL) ---
    def calc_hash(row):
        s = f"{row.get('ocupacion','')}|{row.get('segmento_persona','')}|{row.get('sexo','')}|{row.get('id_ubicacion_fk','')}"
        return hashlib.md5(s.encode('utf-8')).hexdigest()
        
    df_dim_final['hash_val'] = df_dim_final.apply(calc_hash, axis=1)

    try:
        df_db = pd.read_sql("SELECT id_persona_sk, id_persona, ocupacion, segmento_persona, sexo, id_ubicacion_fk FROM dim_personas WHERE es_actual = 1", engine_dw)
        df_db['hash_db'] = df_db.apply(calc_hash, axis=1)
        dict_hash_db = dict(zip(df_db['id_persona'], df_db['hash_db']))
        dict_sk_db = dict(zip(df_db['id_persona'], df_db['id_persona_sk']))
    except Exception:
        dict_hash_db = {}
        dict_sk_db = {}
        
    nuevos = []
    modificados = []
    
    for _, row in df_dim_final.iterrows():
        id_pers = row['id_persona']
        h_val = row['hash_val']
        
        # Si NO está en la BD (o es la primera corrida), es nuevo
        if id_pers not in dict_hash_db:
            nuevos.append(row)
        # Si está en la BD pero el hash es distinto, se modificó
        elif dict_hash_db[id_pers] != h_val:
            modificados.append(row)
            
    df_nuevos = pd.DataFrame(nuevos)
    df_modificados = pd.DataFrame(modificados)
    
    if not df_nuevos.empty:
        df_nuevos = df_nuevos.drop(columns=['hash_val'])
        df_nuevos.to_sql(name="dim_personas", con=engine_dw, if_exists="append", index=False)
        
    if not df_modificados.empty:
        df_modificados = df_modificados.drop(columns=['hash_val'])
        # 1. Desactivar registros viejos
        hoy = pd.Timestamp.today().strftime('%Y-%m-%d')
        ids_a_desactivar = [str(dict_sk_db[r['id_persona']]) for _, r in df_modificados.iterrows()]
        
        if ids_a_desactivar:
            ids_str = ",".join(ids_a_desactivar)
            sql_update = f"UPDATE dim_personas SET es_actual = 0, fecha_hasta = '{hoy}' WHERE id_persona_sk IN ({ids_str})"
            with engine_dw.connect() as conn:
                conn.execute(text(sql_update))
                conn.commit()
                
        # 2. Insertar nuevas versiones
        df_modificados.to_sql(name="dim_personas", con=engine_dw, if_exists="append", index=False)
        
    log.info(f"  ✔ dim_personas: {len(df_nuevos)} nuevos, {len(df_modificados)} modificados (total evaluados: {len(df_dim_final)})")

def asegurar_registros_desconocidos():
    log.info("--- Insertando/Verificando registros Desconocidos (SK = -1) en Dimensiones ---")
    
    queries = [
        "INSERT IGNORE INTO dim_agente (id_agente_sk, id_agente, nombre_agente) VALUES (-1, 'N/A', 'Desconocido');",
        "INSERT IGNORE INTO dim_tiempo (id_tiempo_sk, id_tiempo, Dia, Mes, Anio) VALUES (-1, -1, -1, -1, -1);",
        "INSERT IGNORE INTO dim_perito (id_perito_sk, id_perito, Nombre_Perito) VALUES (-1, 'N/A', 'Desconocido');",
        "INSERT IGNORE INTO dim_objeto (id_objeto_sk, id_objeto, tipo_objeto, valor_objeto) VALUES (-1, 'N/A', 'Desconocido', 0);",
        "INSERT IGNORE INTO dim_tipo_seguro (id_tipo_seguro_sk, categoria_plan) VALUES (-1, 'Desconocido');",
        "INSERT IGNORE INTO dim_tiposiniestro (id_tipo_siniestro_sk, Nombre_Siniestro) VALUES (-1, 'Desconocido');",
        "INSERT IGNORE INTO dim_ubicacion (id_ubicacion_sk, pais, provincia, ciudad) VALUES (-1, 'N/A', 'Desconocido', 'Desconocido');",
        "INSERT IGNORE INTO dim_personas (id_persona_sk, id_persona, ocupacion, segmento_persona, sexo, id_ubicacion_fk, es_tercero, fecha_desde, fecha_hasta, es_actual) VALUES (-1, 'N/A', 'Desconocido', 'Desconocido', 'O', -1, 0, '1900-01-01', NULL, 1);"
    ]
    
    with engine_dw.connect() as conn:
        # Desactivamos checks de foreign keys para poder meter la FK de ubicacion en dim_personas si fuera necesario
        conn.execute(text("SET FOREIGN_KEY_CHECKS=0"))
        for query in queries:
            try:
                conn.execute(text(query))
            except Exception as e:
                log.warning(f"  ⚠ Error insertando -1: {e}")
        conn.execute(text("SET FOREIGN_KEY_CHECKS=1"))
        conn.commit()
    
    log.info("  ✔ Registros -1 asegurados en todas las dimensiones")
