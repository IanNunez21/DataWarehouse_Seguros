import pandas as pd
import logging
from config import engine_staging, engine_dw
from sqlalchemy import text
 
logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

def cargar_fact_poliza():

    # 1. Leer las tablas validadas desde staging
    df_pol = pd.read_sql("SELECT * FROM val_polizas_validadas", engine_staging)
    df_gar = pd.read_sql("SELECT id_poliza, SUM(suma_garantizada) AS suma_garantizada FROM val_garantias_validadas GROUP BY id_poliza", engine_staging)

    total = len(df_pol)

    # 2. Calcular suma_garantizada por póliza (viene de val_garantias_validadas)
    df = df_pol.merge(df_gar, on="id_poliza", how="left")

    # 3. Lookup de surrogate keys desde el DW
    # --- dim_agente ---
    dim_agente = pd.read_sql("SELECT id_agente_sk, id_agente FROM dim_agente", engine_dw)
    dim_agente = dim_agente.drop_duplicates(subset=['id_agente'])
    df = df.merge(dim_agente, on="id_agente", how="left")

    # --- dim_objeto ---
    dim_objeto = pd.read_sql("SELECT id_objeto_sk, id_objeto FROM dim_objeto", engine_dw)
    dim_objeto = dim_objeto.drop_duplicates(subset=['id_objeto'])
    df = df.merge(dim_objeto, left_on="id_objeto_asegurado", right_on="id_objeto", how="left")

    # --- dim_tipo_seguro ---
    mapeo_cobertura = {
        'EXTENDIDA': 'Estandar',
        'BASICA':    'Basico',
        'PREMIUM':   'Premium',
    }
    df['categoria_plan'] = df['cobertura'].str.upper().map(mapeo_cobertura)
    dim_tipo_seguro = pd.read_sql("SELECT id_tipo_seguro_sk, categoria_plan FROM dim_tipo_seguro", engine_dw)
    dim_tipo_seguro = dim_tipo_seguro.drop_duplicates(subset=['categoria_plan'])
    df = df.merge(dim_tipo_seguro, on="categoria_plan", how="left")

    # --- dim_ubicacion (Optimizado) ---
    
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

    # --- dim_tiempo (Corrección de Formato) ---
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

    # Crear un diccionario de búsqueda: {id_natural: id_surrogate}
    dim_personas = pd.read_sql("SELECT id_persona_sk, id_persona FROM dim_personas", engine_dw)
    dim_personas = dim_personas.drop_duplicates(subset=['id_persona'])
    lookup_personas = dict(zip(dim_personas['id_persona'], dim_personas['id_persona_sk']))

    df['id_persona_tomador_sk'] = df['id_cliente'].map(lookup_personas)
    df['id_persona_receptor_sk'] = df['id_asegurado'].map(lookup_personas)

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
    with engine_dw.connect() as conn:
        conn.execute(text("SET FOREIGN_KEY_CHECKS=0"))
        conn.execute(text("TRUNCATE TABLE fact_poliza"))
        conn.execute(text("SET FOREIGN_KEY_CHECKS=1"))
        conn.commit()
    df_fact.to_sql(
        name="fact_poliza",
        con=engine_dw,
        if_exists="append",
        index=False,
    )

    log.info(f"  ✔ fact_poliza cargada: {len(df_fact)} registros de {total} validados")

def cargar_fact_siniestro():
 
    # ── 1. TABLA BASE: partes ────────────────────────────────────────────────
    log.info("  → Leyendo val_partes_validados...")
    df = pd.read_sql(
        """
        SELECT id_parte, id_poliza, id_perito, id_receptor_pago,
               id_objeto_asegurado, tipo_siniestro,
               fecha_apertura, fecha_cierre, monto_reclamado
        FROM val_partes_validados
        """,
        engine_staging,
    )
    total = len(df)
    log.info(f"     Total partes: {total}")
 
    # ── 2. INCORPORAR monto_evaluado (evaluaciones agregadas por parte) ──────
    log.info("  → Leyendo val_evaluaciones_validadas...")
    df_eval = pd.read_sql(
        "SELECT id_parte, SUM(monto_estimado_dano) AS monto_evaluado FROM val_evaluaciones_validadas GROUP BY id_parte",
        engine_staging,
    )
    df = df.merge(df_eval, on="id_parte", how="left")
    log.info(f"     Partes sin evaluación (monto_evaluado nulo): {df['monto_evaluado'].isna().sum()}")
 
    # ── 3. INCORPORAR monto_pagado (pagos agregados por parte) ───────────────
    log.info("  → Leyendo val_pagos_validados...")
    df_pag = pd.read_sql(
        "SELECT id_parte, SUM(monto_pagado) AS monto_pagado FROM val_pagos_validados GROUP BY id_parte",
        engine_staging,
    )
    df = df.merge(df_pag, on="id_parte", how="left")
    log.info(f"     Partes sin pago (monto_pagado nulo): {df['monto_pagado'].isna().sum()}")
 
    # ── 4. INCORPORAR fraude_flag ────────────────────────────────────────────
    log.info("  → Leyendo val_indicadores_fraude_validados...")
    df_fraude = pd.read_sql(
        "SELECT id_parte, fraude_flag FROM val_indicadores_fraude_validados",
        engine_staging,
    )
    df = df.merge(df_fraude, on="id_parte", how="left")
    # Partes sin indicador → fraude_flag = False
    df["fraude_flag"] = df["fraude_flag"].fillna(False).astype(bool)
    log.info(f"     Partes con fraude_flag=True: {df['fraude_flag'].sum()}")
 
    # ── 5. LOOKUPS DE SURROGATE KEYS ─────────────────────────────────────────
 
    # --- id_poliza_sk (desde fact_poliza ya cargada en el DW) ---
    log.info("  → Lookup id_poliza_sk...")
    dim_poliza = pd.read_sql("SELECT id_poliza_sk, id_poliza FROM fact_poliza", engine_dw)
    dim_poliza = dim_poliza.drop_duplicates(subset=["id_poliza"])
    lookup_poliza = dict(zip(dim_poliza["id_poliza"], dim_poliza["id_poliza_sk"]))
    df["id_poliza_sk"] = df["id_poliza"].map(lookup_poliza)
    log.info(f"     ✔ id_poliza_sk | nulos: {df['id_poliza_sk'].isna().sum()}")
 
    # --- PeritoKey (desde dim_perito) ---
    log.info("  → Lookup PeritoKey...")
    dim_perito = pd.read_sql("SELECT id_perito_sk, id_perito FROM dim_perito", engine_dw)
    dim_perito = dim_perito.drop_duplicates(subset=["id_perito"])
    lookup_perito = dict(zip(dim_perito["id_perito"], dim_perito["id_perito_sk"]))
    df["PeritoKey"] = df["id_perito"].map(lookup_perito)
    log.info(f"     ✔ PeritoKey | nulos: {df['PeritoKey'].isna().sum()}")
 
    # --- CobradorKey (desde dim_personas por id_receptor_pago) ---
    log.info("  → Lookup CobradorKey...")
    dim_personas = pd.read_sql("SELECT id_persona_sk, id_persona FROM dim_personas", engine_dw)
    dim_personas = dim_personas.drop_duplicates(subset=["id_persona"])
    lookup_personas = dict(zip(dim_personas["id_persona"], dim_personas["id_persona_sk"]))
    df["CobradorKey"] = df["id_receptor_pago"].map(lookup_personas)
    log.info(f"     ✔ CobradorKey | nulos: {df['CobradorKey'].isna().sum()}")
 
    # --- TipoSiniestroKey (desde dim_tiposiniestro) ---
    log.info("  → Lookup TipoSiniestroKey...")
    dim_tipo = pd.read_sql("SELECT id_tipo_siniestro_sk, Nombre_Siniestro FROM dim_tiposiniestro", engine_dw)
    dim_tipo = dim_tipo.drop_duplicates(subset=["Nombre_Siniestro"])
    lookup_tipo = dict(zip(dim_tipo["Nombre_Siniestro"], dim_tipo["id_tipo_siniestro_sk"]))
    df["TipoSiniestroKey"] = df["tipo_siniestro"].map(lookup_tipo)
    log.info(f"     ✔ TipoSiniestroKey | nulos: {df['TipoSiniestroKey'].isna().sum()}")
 
    # --- UbicacionKey (via id_objeto_asegurado → val_objetos → dim_ubicacion) ---
    log.info("  → Lookup UbicacionKey...")
    obj_ub = pd.read_sql(
        "SELECT id_objeto, provincia, localidad FROM val_objetos_validados",
        engine_staging,
    )
    dim_ub = pd.read_sql(
        "SELECT id_ubicacion_sk, provincia, ciudad FROM dim_ubicacion",
        engine_dw,
    )
    # Clave compuesta para lookup rápido
    dim_ub["lookup_key"] = (
        dim_ub["provincia"].astype(str).str.strip().str.upper() + "|" +
        dim_ub["ciudad"].astype(str).str.strip().str.upper()
    )
    obj_ub["lookup_key"] = (
        obj_ub["provincia"].astype(str).str.strip().str.upper() + "|" +
        obj_ub["localidad"].astype(str).str.strip().str.upper()
    )
    lookup_ub = dict(zip(dim_ub["lookup_key"], dim_ub["id_ubicacion_sk"]))
    obj_ub["id_ubicacion_sk"] = obj_ub["lookup_key"].map(lookup_ub)
    lookup_obj_ub = dict(zip(obj_ub["id_objeto"], obj_ub["id_ubicacion_sk"]))
    df["UbicacionKey"] = df["id_objeto_asegurado"].map(lookup_obj_ub)
    log.info(f"     ✔ UbicacionKey | nulos: {df['UbicacionKey'].isna().sum()}")
 
    # --- FechaAperturaKey y FechaCierreKey (desde dim_tiempo) ---
    log.info("  → Lookup FechaAperturaKey y FechaCierreKey...")
    dim_tiempo = pd.read_sql("SELECT id_tiempo_sk, id_tiempo FROM dim_tiempo", engine_dw)
    dim_tiempo = dim_tiempo.drop_duplicates(subset=["id_tiempo"])
    lookup_tiempo = dict(zip(dim_tiempo["id_tiempo"], dim_tiempo["id_tiempo_sk"]))
 
    df["fecha_apertura"] = pd.to_datetime(df["fecha_apertura"], errors="coerce")
    df["fecha_cierre"]   = pd.to_datetime(df["fecha_cierre"],   errors="coerce")
 
    df["FechaAperturaKey"] = (
        df["fecha_apertura"].dt.strftime("%Y%m%d")
        .astype("Int64", errors="ignore")
        .map(lookup_tiempo)
    )
    df["FechaCierreKey"] = (
        df["fecha_cierre"].dt.strftime("%Y%m%d")
        .astype("Int64", errors="ignore")
        .map(lookup_tiempo)
    )
    log.info(f"     ✔ FechaAperturaKey | nulos: {df['FechaAperturaKey'].isna().sum()}")
    log.info(f"     ✔ FechaCierreKey   | nulos: {df['FechaCierreKey'].isna().sum()} (esperado: partes abiertos)")
 
    # ── 6. ARMAR LA TABLA FINAL ──────────────────────────────────────────────
    df_fact = df[[
        "id_parte",          # → id_siniestro (clave natural)
        "id_poliza_sk",      # → id_poliza_sk
        "FechaAperturaKey",
        "FechaCierreKey",
        "CobradorKey",
        "PeritoKey",
        "TipoSiniestroKey",
        "UbicacionKey",
        "fraude_flag",       # → Fraude_flag
        "monto_reclamado",   # → monto_declarado
        "monto_evaluado",
        "monto_pagado",
    ]].copy()
 
    df_fact = df_fact.rename(columns={
        "id_parte":        "id_siniestro",
        "id_poliza_sk":    "id_poliza_sk",
        "monto_reclamado": "monto_declarado",
    })
 
    # ── 7. ADVERTENCIA DE NULOS ──────────────────────────────────────────────
    # FechaCierreKey y montos pueden ser nulos legítimamente (partes abiertos)
    # Las SKs de dimensiones NO deberían tener nulos
    cols_criticas = ["id_poliza_sk", "FechaAperturaKey", "CobradorKey",
                     "PeritoKey", "TipoSiniestroKey", "UbicacionKey"]
    for col in cols_criticas:
        nulos = df_fact[col].isna().sum()
        if nulos > 0:
            log.warning(f"  ⚠ {col} tiene {nulos} nulos — revisar lookup de dimensión")
 
    # Partes sin pago registrado → 0 (NOT NULL en MySQL, semánticamente correcto)
    partes_sin_pago = df_fact["monto_pagado"].isna().sum()
    if partes_sin_pago > 0:
        log.warning(f"  ⚠ {partes_sin_pago} partes sin pago registrado — monto_pagado se fija en 0")
        df_fact["monto_pagado"] = df_fact["monto_pagado"].fillna(0)
 
    # ── 8. VALIDACIÓN DE NEGOCIO: monto_pagado vs suma_garantizada ───────────
    # Si el pago supera la cobertura garantizada, es anómalo.
    # No se descarta: se conserva el registro pero se eleva fraude_flag a True
    # para que quede marcado para revisión manual.
    log.info("  → Validando monto_pagado vs suma_garantizada de la póliza...")
    df_garantia = pd.read_sql(
        "SELECT id_poliza_sk, suma_garantizada FROM fact_poliza", engine_dw
    )
    df_garantia = df_garantia.drop_duplicates(subset=["id_poliza_sk"])
    df_fact = df_fact.merge(df_garantia, on="id_poliza_sk", how="left")
 
    mask_excede = (
        df_fact["monto_pagado"].notna() &
        df_fact["suma_garantizada"].notna() &
        (df_fact["monto_pagado"] > df_fact["suma_garantizada"])
    )
 
    cantidad_excedidos = mask_excede.sum()
    if cantidad_excedidos > 0:
        log.warning(
            f"  ⚠ REGLA DE NEGOCIO: {cantidad_excedidos} siniestros con monto_pagado "
            f"mayor a suma_garantizada — se marcan con fraude_flag=True"
        )
        # Detalle de los primeros 5 para trazabilidad
        for _, row in df_fact[mask_excede].head(5).iterrows():
            log.warning(
                f"     {row['id_siniestro']} | "
                f"pagado={row['monto_pagado']:,.2f} | "
                f"garantizado={row['suma_garantizada']:,.2f}"
            )
        # Marcar como fraude en lugar de descartar
        df_fact.loc[mask_excede, "fraude_flag"] = True
 
    df_fact = df_fact.drop(columns=["suma_garantizada"])
    log.info(
        f"     ✔ Total fraude_flag=True tras validación: {df_fact['fraude_flag'].sum()} "
        f"({cantidad_excedidos} por exceso de pago, resto por indicadores)"
    )
 
    # ── 9. INSERTAR EN fact_siniestro ────────────────────────────────────────
    # SiniestroKey → AUTO_INCREMENT en MySQL, no se inserta
    with engine_dw.connect() as conn:
            conn.execute(text("SET FOREIGN_KEY_CHECKS=0"))
            conn.execute(text("TRUNCATE TABLE fact_siniestro"))
            conn.execute(text("SET FOREIGN_KEY_CHECKS=1"))
            conn.commit()
    df_fact.to_sql(
        name="fact_siniestro",
        con=engine_dw,
        if_exists="append",
        index=False,
    )
 
    log.info(f"  ✔ fact_siniestro cargada: {len(df_fact)} registros de {total} partes")
 