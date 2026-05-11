import pandas as pd
import logging
from config import engine_staging, engine_dw
 
logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

def cargar_fact_poliza():
    log.info("═══ Cargando fact_poliza ═══")

    # 1. Leer las tablas validadas desde staging
    log.info("  → Leyendo val_polizas_validadas...")
    df_pol = pd.read_sql("SELECT * FROM val_polizas_validadas", engine_staging)
    log.info("  → Leyendo val_garantias_validadas...")
    df_gar = pd.read_sql("SELECT id_poliza, SUM(suma_garantizada) AS suma_garantizada FROM val_garantias_validadas GROUP BY id_poliza", engine_staging)

    total = len(df_pol)
    log.info(f"     Total pólizas: {total}")

    # 2. Calcular suma_garantizada por póliza (viene de val_garantias_validadas)
    log.info("  → Merge con garantías...")
    df = df_pol.merge(df_gar, on="id_poliza", how="left")
    log.info(f"     Después de merge garantías: {len(df)} filas")

    # 3. Lookup de surrogate keys desde el DW
    # --- dim_agente ---
    log.info("  → Lookup dim_agente...")
    dim_agente = pd.read_sql("SELECT id_agente_sk, id_agente FROM dim_agente", engine_dw)
    dim_agente = dim_agente.drop_duplicates(subset=['id_agente'])
    df = df.merge(dim_agente, on="id_agente", how="left")
    log.info(f"     ✔ id_agente_sk | nulos: {df['id_agente_sk'].isna().sum()}")

    # --- dim_objeto ---
    log.info("  → Lookup dim_objeto...")
    dim_objeto = pd.read_sql("SELECT id_objeto_sk, id_objeto FROM dim_objeto", engine_dw)
    dim_objeto = dim_objeto.drop_duplicates(subset=['id_objeto'])
    df = df.merge(dim_objeto, left_on="id_objeto_asegurado", right_on="id_objeto", how="left")
    log.info(f"     ✔ id_objeto_sk | nulos: {df['id_objeto_sk'].isna().sum()}")

    # --- dim_tipo_seguro ---
    log.info("  → Lookup dim_tipo_seguro...")
    mapeo_cobertura = {
        'EXTENDIDA': 'Estandar',
        'BASICA':    'Basico',
        'PREMIUM':   'Premium',
    }
    df['categoria_plan'] = df['cobertura'].str.upper().map(mapeo_cobertura)
    dim_tipo_seguro = pd.read_sql("SELECT id_tipo_seguro_sk, categoria_plan FROM dim_tipo_seguro", engine_dw)
    dim_tipo_seguro = dim_tipo_seguro.drop_duplicates(subset=['categoria_plan'])
    df = df.merge(dim_tipo_seguro, on="categoria_plan", how="left")
    log.info(f"     ✔ id_tipo_seguro_sk | nulos: {df['id_tipo_seguro_sk'].isna().sum()}")

   
    # --- dim_ubicacion (Optimizado) ---
    log.info("  → Optimizando lookup de dim_ubicacion...")
    
    # 1. Traer dimensiones y limpiar de una vez
    dim_ub = pd.read_sql("SELECT id_ubicacion_sk, provincia, ciudad FROM dim_ubicacion", engine_dw)
    
    # Crear una llave única: "PROVINCIA|CIUDAD" para búsqueda rápida
    dim_ub['lookup_key'] = (
        dim_ub['provincia'].astype(str).str.strip().str.upper() + "|" + 
        dim_ub['ciudad'].astype(str).str.strip().str.upper()
    )
    
    # Crear el diccionario de búsqueda (esto es instantáneo en memoria)
    dict_ub = dict(zip(dim_ub['lookup_key'], dim_ub['id_ubicacion_sk']))

    # 2. Traer info de objetos desde staging
    obj_ub = pd.read_sql("SELECT id_objeto, provincia, localidad FROM val_objetos_validados", engine_staging)
    
    # Crear la misma llave única en los objetos
    obj_ub['lookup_key'] = (
        obj_ub['provincia'].astype(str).str.strip().str.upper() + "|" + 
        obj_ub['localidad'].astype(str).str.strip().str.upper()
    )

    # 3. Mapear el SK a los objetos usando el diccionario
    obj_ub['id_ubicacion_sk'] = obj_ub['lookup_key'].map(dict_ub)

    # 4. Pasar el SK a la tabla principal 'df' mediante el id_objeto
    # Usamos un mapeo de id_objeto -> id_ubicacion_sk
    map_obj_to_ub = dict(zip(obj_ub['id_objeto'], obj_ub['id_ubicacion_sk']))
    df['id_ubicacion_sk'] = df['id_objeto_asegurado'].map(map_obj_to_ub)

    log.info(f"     ✔ id_ubicacion_sk | nulos: {df['id_ubicacion_sk'].isna().sum()}")

    # --- dim_tiempo (Corrección de Formato) ---
    log.info("  → Lookup dim_tiempo...")
    dim_tiempo = pd.read_sql("SELECT id_tiempo_sk, id_tiempo FROM dim_tiempo", engine_dw)
    dim_tiempo = dim_tiempo.drop_duplicates(subset=['id_tiempo'])
    lookup_tiempo = dict(zip(dim_tiempo['id_tiempo'], dim_tiempo['id_tiempo_sk']))
    
    # Convertimos a datetime por si acaso vienen como strings
    df['fecha_alta'] = pd.to_datetime(df['fecha_alta'], errors='coerce')
    
    # Transformamos: 2011-01-31 -> 20110131 (Integer)
    df["id_tiempo_join"] = (
        df["fecha_alta"].dt.strftime('%Y%m%d')
        .fillna(0)
        .astype(int)
    )
    df["id_fecha_venta_sk"] = df["id_tiempo_join"].map(lookup_tiempo)
    df = df.drop(columns=["id_tiempo_join"])
    
    # Si en tu DW el valor para fechas nulas es -1 o un ID específico, cámbialo:
    # df["id_fecha_venta_sk"] = df["id_fecha_venta_sk"].replace(0, -1)

    log.info(f"     ✔ id_fecha_venta_sk listo | Ejemplo: {df['id_fecha_venta_sk'].iloc[0]}")

    # Crear un diccionario de búsqueda: {id_natural: id_surrogate}
    log.info("  → Lookup dim_personas (Tomador y Receptor)...")
    dim_personas = pd.read_sql("SELECT id_persona_sk, id_persona FROM dim_personas", engine_dw)
    dim_personas = dim_personas.drop_duplicates(subset=['id_persona'])
    lookup_personas = dict(zip(dim_personas['id_persona'], dim_personas['id_persona_sk']))

    df['id_persona_tomador_sk'] = df['id_cliente'].map(lookup_personas)
    df['id_persona_receptor_sk'] = df['id_asegurado'].map(lookup_personas)

    log.info(f"✔ Mapeo completado. Nulos Tomador: {df['id_persona_tomador_sk'].isna().sum()}")

    # 4. Seleccionar solo las columnas de fact_poliza
    df_fact = df[[
        "id_poliza",
        "id_persona_tomador_sk",
        "id_persona_receptor_sk",
        "id_fecha_venta_sk",
        "id_objeto_sk",
        "id_agente_sk",
        "id_tipo_seguro_sk",
        "id_ubicacion_sk",
        "prima_mensual",       # → monto_prima
        "suma_garantizada",
    ]].copy()

    df_fact = df_fact.rename(columns={"prima_mensual": "monto_prima"})

    # 5. Advertir filas con SKs nulas (lookup fallido)
    nulos = df_fact.isnull().any(axis=1).sum()
    if nulos:
        log.warning(f"  ⚠ {nulos} filas con al menos una SK nula — revisar dimensiones")

    # 6. Insertar en fact_poliza
    #    id_poliza_sk → generada por AUTO_INCREMENT en MySQL
    df_fact.to_sql(
        name="fact_poliza",
        con=engine_dw,
        if_exists="append",
        index=False,
    )

    log.info(f"  ✔ fact_poliza cargada: {len(df_fact)} registros de {total} validados")