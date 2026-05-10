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

    BASURA = {"-", ".", "..", "000", "999", "?", "n/d", "s/d", "test", "xxx", "pendiente"}


def _limpiar(serie: pd.Series) -> pd.Series:
    """Quita espacios, tabs, caracteres invisibles, puntos finales y valores basura."""
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

    # 1. Leer las tablas validadas desde staging
    clientes = pd.read_sql("SELECT pais, provincia, localidad FROM val_clientes_validados", engine_staging)
    objetos  = pd.read_sql("SELECT provincia, localidad FROM val_objetos_asegurados_validados", engine_staging)
    peritos  = pd.read_sql("SELECT zona_cobertura FROM val_peritos_validados", engine_staging)

    total_filas = len(clientes) + len(objetos) + len(peritos)

    # 2. Renombrar columnas al esquema de dim_ubicacion
    ub_clientes = clientes.rename(columns={
        "pais":      "Nombre_Pais",
        "provincia": "Nombre_Provincia",
        "localidad": "Nombre_Ciudad",
    })

    ub_objetos = objetos.rename(columns={
        "provincia": "Nombre_Provincia",
        "localidad": "Nombre_Ciudad",
    })
    ub_objetos["Nombre_Pais"] = None

    ub_peritos = peritos.rename(columns={"zona_cobertura": "Nombre_Provincia"})
    ub_peritos["Nombre_Pais"]   = None
    ub_peritos["Nombre_Ciudad"] = None

    # 3. Unir las tres fuentes
    df_dim = pd.concat(
        [ub_clientes, ub_objetos, ub_peritos],
        ignore_index=True,
    )[["Nombre_Pais", "Nombre_Provincia", "Nombre_Ciudad"]]

    # 4. Limpiar cada columna
    for col in ["Nombre_Pais", "Nombre_Provincia", "Nombre_Ciudad"]:
        df_dim[col] = _limpiar(df_dim[col])

    # 5. Deduplicar por la combinación de las tres columnas
    df_dim = (
        df_dim
        .drop_duplicates(
            subset=["Nombre_Pais", "Nombre_Provincia", "Nombre_Ciudad"],
            keep="first",
        )
        .reset_index(drop=True)
    )

    # 6. Eliminar filas donde los tres campos sean nulos a la vez
    df_dim = df_dim.dropna(
        subset=["Nombre_Pais", "Nombre_Provincia", "Nombre_Ciudad"],
        how="all",
    )

    # 7. Insertar en dim_ubicacion
    #    id_ubicacion y UbicacionKey → generados por AUTO_INCREMENT en MySQL
    #    if_exists="append" para no destruir registros existentes
    #    index=False porque las SKs las genera MySQL
    df_dim.to_sql(
        name="dim_ubicacion",
        con=engine_dw,
        if_exists="append",
        index=False,
    )

    log.info(
        f"  ✔ dim_ubicacion cargada: {len(df_dim)} combinaciones únicas "
        f"de {total_filas} filas fuente"
    )