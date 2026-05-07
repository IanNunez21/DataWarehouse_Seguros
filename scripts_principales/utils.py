import pandas as pd
import logging
import unicodedata
import os
from config import engine_staging

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

def normalizar_texto(texto):
    """Limpia tildes, acentos y normaliza a mayúsculas para evitar errores de matching."""
    if not isinstance(texto, str):
        return str(texto)
    texto_nfd = unicodedata.normalize('NFD', texto)
    texto_limpio = "".join(c for c in texto_nfd if unicodedata.category(c) != 'Mn')
    return texto_limpio.upper().strip()

def validar_geografia(df, ruta_maestro=None):
    """
    Realiza la validación geográfica cruzando el DataFrame contra el maestro de Provincias/Localidades.
    Si no se proporciona ruta_maestro, la calcula automáticamente.
    """
    if ruta_maestro is None:
        BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        ruta_maestro = os.path.join(BASE_DIR, "scripts_adicionales", "SourcesAdicionales", "provincias_localidades.csv")
        
    try:
        df_maestro = pd.read_csv(ruta_maestro)
        df_maestro = normalizar_columnas_texto(df_maestro, ['provincia', 'localidad'])
        
        df = normalizar_columnas_texto(df, ['provincia', 'localidad'])

        df = df.merge(df_maestro, on=['provincia', 'localidad'], how='inner')
        
    except FileNotFoundError:
        log.warning("  ⚠ Maestro no encontrado en la ruta. Se omite validación geo profunda.")
        if 'pais' in df.columns:
            df = df[df['pais'].astype(str).str.upper().str.strip() == 'ARGENTINA']
            
    return df

def normalizar_columnas_texto(df, columnas):
    """
    Aplica la normalización de texto a una o más columnas del DataFrame.
    """
    if isinstance(columnas, str):
        columnas = [columnas]
        
    for col in columnas:
        if col in df.columns:
            df[col] = df[col].apply(normalizar_texto)
    return df

def crear_nombre_completo(df, col_nombre='nombre', col_apellido='apellido', col_destino='nombre_completo'):
    """
    Crea una nueva columna combinando apellido y nombre en formato 'Apellido, Nombre'.
    """
    if col_nombre in df.columns and col_apellido in df.columns:
        df[col_destino] = df[col_apellido].fillna('') + ", " + df[col_nombre].fillna('')
        # Limpiar comas huérfanas si alguno era nulo
        df[col_destino] = df[col_destino].str.strip(", ").replace('', None)
    return df

def limpiar_ids(df, columnas_id, columnas_dropna=None, id_principal=None):
    """
    Realiza la limpieza de columnas de identificadores:
    - Elimina nulos en las columnas críticas (columnas_dropna).
    - Convierte los IDs a string y elimina espacios vacíos.
    - Elimina duplicados basándose en id_principal (conservando el último registro).
    """
    if isinstance(columnas_id, str):
        columnas_id = [columnas_id]
        
    if columnas_dropna is None:
        columnas_dropna = columnas_id
    elif isinstance(columnas_dropna, str):
        columnas_dropna = [columnas_dropna]
        
    # 1. Eliminar nulos
    columnas_validas_dropna = [col for col in columnas_dropna if col in df.columns]
    if columnas_validas_dropna:
        df = df.dropna(subset=columnas_validas_dropna)
        
    # 2. Formatear como string y quitar espacios
    for col in columnas_id:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
            
    # 3. Eliminar duplicados
    if id_principal and id_principal in df.columns:
        df = df.drop_duplicates(subset=[id_principal], keep='last')
        
    return df

def convertir_fechas(df, columnas, formato=None):
    """
    Convierte una o más columnas a formato datetime.
    Maneja errores convirtiéndolos a NaT (Not a Time).
    """
    if isinstance(columnas, str):
        columnas = [columnas]
        
    for col in columnas:
        if col in df.columns:
            if formato:
                df[col] = pd.to_datetime(df[col], format=formato, errors='coerce')
            else:
                df[col] = pd.to_datetime(df[col], errors='coerce')
    return df

def limpiar_numericos(df, columnas, valor_defecto=0.0):
    """
    Convierte una o más columnas a tipo numérico, reemplazando los errores con NaN
    y luego rellenándolos con un valor por defecto (ej. 0.0 o 0).
    """
    if isinstance(columnas, str):
        columnas = [columnas]
        
    for col in columnas:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(valor_defecto)
    return df

def guardar_datos_curados(df, nombre_tabla):
    """
    Guarda el DataFrame transformado en la base de datos Staging.
    """
    log.info(f"📥 Guardando tabla curada ({nombre_tabla}) en Staging...")
    df.to_sql(name=nombre_tabla, con=engine_staging, if_exists="replace", index=False)
    return df
