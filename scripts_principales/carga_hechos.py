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
    df = df.merge(dim_agente, on="id_agente", how="left")
    log.info(f"     ✔ id_agente_sk | nulos: {df['id_agente_sk'].isna().sum()}")

    # --- dim_objeto ---
    log.info("  → Lookup dim_objeto...")
    dim_objeto = pd.read_sql("SELECT id_objeto_sk, id_objeto FROM dim_objeto", engine_dw)
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
    df = df.merge(dim_tipo_seguro, on="categoria_plan", how="left")
    log.info(f"     ✔ id_tipo_seguro_sk | nulos: {df['id_tipo_seguro_sk'].isna().sum()}")

   
    # --- dim_ubicacion (via val_objetos_validados: provincia + localidad) ---
    log.info("  → Lookup dim_ubicacion (via objetos)...")
    objetos_ub = pd.read_sql(
        "SELECT id_objeto, provincia, localidad FROM val_objetos_validados",
        engine_staging,
    )
    dim_ubicacion = pd.read_sql(
        "SELECT id_ubicacion_sk, provincia AS Nombre_Provincia, ciudad AS Nombre_Ciudad FROM dim_ubicacion",
        engine_dw,
    )

    # Limpiar para hacer el join
    for col in ["Nombre_Provincia", "Nombre_Ciudad"]:
        dim_ubicacion[col] = dim_ubicacion[col].str.strip().str.title()

    objetos_ub = objetos_ub.rename(columns={
        "provincia": "Nombre_Provincia",
        "localidad": "Nombre_Ciudad",
    })
    for col in ["Nombre_Provincia", "Nombre_Ciudad"]:
        objetos_ub[col] = objetos_ub[col].astype(str).str.strip().str.title()

    objetos_ub = objetos_ub.merge(dim_ubicacion, on=["Nombre_Provincia", "Nombre_Ciudad"], how="left")
    log.info(f"     Objetos sin ubicacion_sk: {objetos_ub['id_ubicacion_sk'].isna().sum()} de {len(objetos_ub)}")
    df = df.merge(
        objetos_ub[["id_objeto", "id_ubicacion_sk"]],
        left_on="id_objeto_asegurado",
        right_on="id_objeto",
        how="left",
    )
    log.info(f"     ✔ id_ubicacion_sk | nulos: {df['id_ubicacion_sk'].isna().sum()}")

    # --- dim_tiempo (fecha_alta → lookup por id_tiempo YYYYMMDD → id_tiempo_sk) ---
    log.info("  → Lookup dim_tiempo...")
    dim_tiempo = pd.read_sql(
        "SELECT id_tiempo_sk, Dia, Mes, Anio FROM dim_tiempo", engine_dw
    )

    df["_dia"]  = pd.to_datetime(df["fecha_alta"], errors="coerce").dt.day
    df["_mes"]  = pd.to_datetime(df["fecha_alta"], errors="coerce").dt.month
    df["_anio"] = pd.to_datetime(df["fecha_alta"], errors="coerce").dt.year

    df = df.merge(
        dim_tiempo.rename(columns={"id_tiempo_sk": "id_fecha_venta_sk"}),
        left_on=["_dia", "_mes", "_anio"],
        right_on=["Dia", "Mes", "Anio"],
        how="left",
    ).drop(columns=["_dia", "_mes", "_anio", "Dia", "Mes", "Anio"])

    # --- dim_persona para tomador y receptor (id_cliente) ---
    log.info("  → Lookup dim_personas...")
    dim_personas = pd.read_sql("SELECT id_persona_sk, id_persona FROM dim_personas", engine_dw)

    df = df.merge(
        dim_personas.rename(columns={"id_persona_sk": "id_persona_tomador_sk"}),
        left_on="id_cliente",
        right_on="id_persona",
        how="left",
    )
    log.info(f"     ✔ id_persona_tomador_sk | nulos: {df['id_persona_tomador_sk'].isna().sum()}")
    df = df.merge(
        dim_personas.rename(columns={"id_persona_sk": "id_persona_receptor_sk"}),
        left_on="id_asegurado",
        right_on="id_persona",
        how="left",
        suffixes=("", "_receptor"),
    )
    log.info(f"     ✔ id_persona_receptor_sk | nulos: {df['id_persona_receptor_sk'].isna().sum()}")

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