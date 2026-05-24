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

# Diccionarios de mapeo para correcciones ortográficas y abreviaturas comunes de geografía argentina
MAPEO_PROVINCIAS = {
    'TUCUM': 'TUCUMAN',
    'TUC': 'TUCUMAN',
    'BS AS': 'BUENOS AIRES',
    'BSAS': 'BUENOS AIRES',
    'BUENOS A': 'BUENOS AIRES',
    'BUENOS AIRE': 'BUENOS AIRES',
    'CABA': 'CIUDAD AUTONOMA DE BUENOS AIRES',
    'CAPITAL FEDERAL': 'CIUDAD AUTONOMA DE BUENOS AIRES',
    'CORDOB': 'CORDOBA',
    'STA FE': 'SANTA FE',
    'SFE': 'SANTA FE',
    'SNTG DEL ESTERO': 'SANTIAGO DEL ESTERO',
    'SANT ESTERO': 'SANTIAGO DEL ESTERO',
    'SANTIAGO DEL EST': 'SANTIAGO DEL ESTERO',
    'ENTRE R': 'ENTRE RIOS',
    'MEND': 'MENDOZA',
    'MENDOZ': 'MENDOZA',
}

MAPEO_LOCALIDADES = {
    'CAPITAL': 'CIUDAD AUTONOMA DE BUENOS AIRES',
    'TUCUM': 'SAN MIGUEL DE TUCUMAN',
    'S. M. DE TUCUMAN': 'SAN MIGUEL DE TUCUMAN',
    'SAN MIGUEL TUCUMAN': 'SAN MIGUEL DE TUCUMAN',
    'SM TUCUMAN': 'SAN MIGUEL DE TUCUMAN',
    'CORDOB': 'CORDOBA',
    'MENDOZ': 'MENDOZA',
}

def validar_geografia(df, ruta_maestro=None):
    """
    Realiza la validación geográfica cruzando el DataFrame contra el maestro de Provincias/Localidades.
    Si no se proporciona ruta_maestro, la calcula automáticamente.
    Retorna una tupla (df_validos, df_rechazados_geo):
      - df_validos: registros que hicieron match o tenían geografía nula.
      - df_rechazados_geo: registros con datos geográficos que no matchearon contra el maestro.
    """
    df_rechazados_geo = pd.DataFrame()

    if ruta_maestro is None:
        BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        ruta_maestro = os.path.join(BASE_DIR, "scripts_adicionales", "SourcesAdicionales", "provincias_localidades.csv")
        
    try:
        df_maestro = pd.read_csv(ruta_maestro)
        df_maestro = normalizar_columnas_texto(df_maestro, ['provincia', 'localidad'])
        
        # Identificamos si hay nulos (o cadenas vacías) antes de normalizar
        es_nulo = df['provincia'].isna() | df['localidad'].isna() | (df['provincia'] == '') | (df['localidad'] == '')
        
        # Separamos los nulos para no perderlos en el inner join
        df_nulos = df[es_nulo].copy()
        df_no_nulos = df[~es_nulo].copy()
        
        # Solo normalizamos y cruzamos los que sí tienen datos geográficos
        if not df_no_nulos.empty:
            df_no_nulos_norm = normalizar_columnas_texto(df_no_nulos.copy(), ['provincia', 'localidad'])
            
            # Aplicar correcciones geográficas ortográficas y abreviaturas
            df_no_nulos_norm['provincia'] = df_no_nulos_norm['provincia'].replace(MAPEO_PROVINCIAS)
            df_no_nulos_norm['localidad'] = df_no_nulos_norm['localidad'].replace(MAPEO_LOCALIDADES)
            
            df_matched = df_no_nulos_norm.merge(df_maestro, on=['provincia', 'localidad'], how='inner')
            
            # Detectar rechazados: registros con datos geográficos que no hicieron match
            indices_matched = set(df_matched['_original_index']) if '_original_index' in df_matched.columns else set(df_matched.index)
            col_idx = '_original_index' if '_original_index' in df_no_nulos.columns else None
            if col_idx:
                df_rechazados_geo = df_no_nulos[~df_no_nulos[col_idx].isin(indices_matched)].copy()
            else:
                df_rechazados_geo = df_no_nulos[~df_no_nulos.index.isin(indices_matched)].copy()
            
            # Estrategia Híbrida: Reincorporamos los rechazados geográficos como DESCONOCIDO
            if not df_rechazados_geo.empty:
                df_recuperados = df_rechazados_geo.copy()
                df_recuperados['provincia'] = 'DESCONOCIDO'
                df_recuperados['localidad'] = 'DESCONOCIDO'
                df_no_nulos = pd.concat([df_matched, df_recuperados], ignore_index=True)
            else:
                df_no_nulos = df_matched
            
        # Unimos los que hicieron match con los que originalmente eran nulos (también marcados como DESCONOCIDO)
        if not df_nulos.empty:
            df_nulos['provincia'] = 'DESCONOCIDO'
            df_nulos['localidad'] = 'DESCONOCIDO'
            
        df = pd.concat([df_no_nulos, df_nulos], ignore_index=True)
        
    except FileNotFoundError:
        log.warning("  ⚠ Maestro no encontrado en la ruta. Se omite validación geo profunda.")
        if 'pais' in df.columns:
            df = df[df['pais'].astype(str).str.upper().str.strip() == 'ARGENTINA']
            
    return df, df_rechazados_geo

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

def registrar_auditoria_calidad(df_registros, tabla_origen, motivo, tipo_evento='RECHAZADO'):
    """
    Guarda registros con observaciones de calidad (RECHAZADOS o SANEADOS) en dw_staging.bitacora_calidad.
    """
    if df_registros is None or df_registros.empty:
        return
        
    try:
        # Convertir a json cada fila para mayor flexibilidad
        registros_json = df_registros.apply(lambda row: row.to_json(default_handler=str), axis=1)
        
        df_insert = pd.DataFrame({
            'tabla_origen': tabla_origen,
            'tipo_evento': tipo_evento,
            'motivo_observacion': motivo,
            'datos_registro': registros_json,
            'fecha_registro': pd.Timestamp.now()
        })
        
        df_insert.to_sql('bitacora_calidad', con=engine_staging, if_exists='append', index=False)
    except Exception as e:
        log.error(f"  ❌ Error al guardar bitacora_calidad: {e}")


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
    # Guarda el DataFrame transformado en la base de datos Staging.
    df.to_sql(name=nombre_tabla, con=engine_staging, if_exists="replace", index=False)
    return df
