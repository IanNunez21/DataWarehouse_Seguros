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
    
    # 1. Capa previa de reemplazos para reparar codificaciones UTF-8 rotas leídas en Latin-1
    reparaciones_previas = {
        "\u200b": "", "\u200c": "",
        "Ã¡": "A", "Ã³": "O", "Ã­": "I", "Ã©": "E", "Ãº": "U", "Ã±": "N", "Ã‘": "N",
        "Ã\x81": "A", "Ã\x93": "O", "Ã\x8d": "I", "Ã\x89": "E", "Ã\x9a": "U",
        "A¡": "A", "A³": "O", "A­": "I", "A©": "E", "Aº": "U", "A±": "N",
        "A°": "U", "Aº": "U", "A±": "N", "A­OS": "RIOS", "A­O": "RIO",
        "Ã\x81NGELA": "ANGELA", "A NGELA": "ANGELA",
    }
    
    texto = texto.upper()
    for rota, corregida in reparaciones_previas.items():
        texto = texto.replace(rota.upper(), corregida)
        
    # 2. Normalización NFD y eliminación de marcas de acento (Mn)
    texto_nfd = unicodedata.normalize('NFD', texto)
    texto_limpio = "".join(c for c in texto_nfd if unicodedata.category(c) != 'Mn')
    
    # 3. Capa posterior a la normalización para reparar transcripciones literales de codificación rota
    reparaciones_post = {
        "TUCUMAIN": "TUCUMAN",
        "LUJAIN DE CUYO": "LUJAN DE CUYO",
        "OBERAI": "OBERA",
        "ORAIN": "ORAN",
        "PARANAI": "PARANA",
        "ENTRE RA-OS": "ENTRE RIOS",
        "RA-O CUARTO": "RIO CUARTO",
        "VILLA A NGELA": "VILLA ANGELA"
    }
    
    for rota, corregida in reparaciones_post.items():
        texto_limpio = texto_limpio.replace(rota, corregida)
        
    # Normalizar a mayúsculas, quitar espacios y remover signos de puntuación iniciales/finales
    return texto_limpio.strip().strip(". ,-*_")

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
    'YERBA BUENA': 'YERBA BUENA - MARCOS PAZ',
    'ORAN': 'SAN RAMON DE LA NUEVA ORAN',
    'SAN TELMO': 'CIUDAD AUTONOMA DE BUENOS AIRES',
    'PALERMO': 'CIUDAD AUTONOMA DE BUENOS AIRES',
    'RECOLETA': 'CIUDAD AUTONOMA DE BUENOS AIRES',
    'BELGRANO': 'CIUDAD AUTONOMA DE BUENOS AIRES',
}

def validar_geografia(df, ruta_maestro=None):
    """
    Realiza la validación geográfica cruzando el DataFrame contra el maestro de Provincias/Localidades.
    Aplica una estrategia de auto-curación inteligente (Self-Healing) para:
      - Corregir provincias erróneas/vacías si la localidad es válida y única.
      - Preservar la provincia si es válida pero la localidad es nula/inválida (poniendo 'DESCONOCIDO' en la localidad).
      - Normalizar formatos, quitar caracteres inválidos (como puntos finales) y mapear sinónimos comunes.
    Retorna una tupla (df_resultado, df_rechazados_geo):
      - df_resultado: DataFrame original con geografía normalizada y corregida.
      - df_rechazados_geo: registros que no pudieron ser curados (se marcan como DESCONOCIDO pero se registran en bitácora).
    """
    df_rechazados_geo = pd.DataFrame()

    if ruta_maestro is None:
        BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        ruta_maestro = os.path.join(BASE_DIR, "scripts_adicionales", "SourcesAdicionales", "provincias_localidades.csv")
        
    try:
        # 1. Cargar y normalizar el maestro
        df_maestro = pd.read_csv(ruta_maestro)
        df_maestro = normalizar_columnas_texto(df_maestro, ['provincia', 'localidad'])
        
        # Estructuras para búsquedas rápidas y mapeos
        set_maestro = set(zip(df_maestro['provincia'], df_maestro['localidad']))
        set_provincias_validas = set(df_maestro['provincia'].unique())
        set_localidades_validas = set(df_maestro['localidad'].unique())
        
        # Mapeo de localidad -> provincias en las que existe
        loc_to_prov_map = df_maestro.groupby('localidad')['provincia'].apply(list).to_dict()
        
        # 2. Hacer una copia de trabajo de df y normalizar provincia y localidad
        df_work = df.copy()
        
        # Guardamos columnas originales para comparar/rechazar si es necesario
        df_work['_orig_provincia'] = df_work['provincia']
        df_work['_orig_localidad'] = df_work['localidad']
        
        # Normalizamos provisionalmente en la copia
        df_work = normalizar_columnas_texto(df_work, ['provincia', 'localidad'])
        
        # Aplicamos mapeos
        df_work['provincia'] = df_work['provincia'].replace(MAPEO_PROVINCIAS)
        df_work['localidad'] = df_work['localidad'].replace(MAPEO_LOCALIDADES)
        
        # Valores considerados "basura" o vacíos
        VALORES_VACIOS = {'', 'NAN', 'NONE', 'N/D', 'S/D', 'TEST', 'XXX', 'PENDIENTE', '000', '999', '.', '-'}
        
        # Listas para guardar las correcciones finales
        provincias_finales = []
        localidades_finales = []
        indices_rechazados = []
        
        for idx, row in df_work.iterrows():
            prov = str(row['provincia']).strip()
            loc = str(row['localidad']).strip()
            
            is_prov_empty = prov in VALORES_VACIOS
            is_loc_empty = loc in VALORES_VACIOS
            
            # Caso 1: Ambos vacíos
            if is_prov_empty and is_loc_empty:
                provincias_finales.append('DESCONOCIDO')
                localidades_finales.append('DESCONOCIDO')
                indices_rechazados.append(idx)
                continue
                
            # Caso 2: Coincidencia Exacta Directa
            if (prov, loc) in set_maestro:
                provincias_finales.append(prov)
                localidades_finales.append(loc)
                continue
                
            # Caso 3: Inconsistencias o valores inválidos. Auto-curación inteligente!
            # Intentamos curar basándonos en la localidad primero (es más específica)
            if not is_loc_empty and loc in set_localidades_validas:
                # La localidad existe en el maestro. Obtenemos las provincias en las que existe.
                provincias_posibles = loc_to_prov_map.get(loc, [])
                
                if len(provincias_posibles) == 1:
                    # Curación exitosa: la localidad es única y pertenece a una sola provincia
                    provincias_finales.append(provincias_posibles[0])
                    localidades_finales.append(loc)
                elif not is_prov_empty and prov in provincias_posibles:
                    # La provincia original (normalizada) es una de las posibles
                    provincias_finales.append(prov)
                    localidades_finales.append(loc)
                elif len(provincias_posibles) > 0:
                    # La localidad pertenece a múltiples provincias. Default al primero o mantenemos prov si es válida
                    if not is_prov_empty and prov in set_provincias_validas:
                        # Si la provincia original es válida por sí misma, la mantenemos pero ponemos localidad DESCONOCIDO
                        provincias_finales.append(prov)
                        localidades_finales.append('DESCONOCIDO')
                    else:
                        # Si la provincia no es válida, ponemos la primera provincia donde está esa localidad
                        provincias_finales.append(provincias_posibles[0])
                        localidades_finales.append(loc)
                else:
                    provincias_finales.append('DESCONOCIDO')
                    localidades_finales.append('DESCONOCIDO')
                    indices_rechazados.append(idx)
            else:
                # La localidad no es válida o está vacía. Evaluamos la provincia por separado
                if not is_prov_empty and prov in set_provincias_validas:
                    # Provincia válida, pero localidad inválida/vacía: conservamos provincia, localidad DESCONOCIDO
                    provincias_finales.append(prov)
                    localidades_finales.append('DESCONOCIDO')
                else:
                    # Ambos inválidos/no curables
                    provincias_finales.append('DESCONOCIDO')
                    localidades_finales.append('DESCONOCIDO')
                    indices_rechazados.append(idx)
                    
        # Asignamos las columnas finales curadas
        df_work['provincia'] = provincias_finales
        df_work['localidad'] = localidades_finales
        
        # Filtramos rechazados (los que terminaron siendo completamente DESCONOCIDO en ambos campos)
        mask_rechazados = df.index.isin(indices_rechazados)
        df_rechazados_geo = df[mask_rechazados].copy()
        
        # Limpiamos las columnas auxiliares y devolvemos
        df_work = df_work.drop(columns=['_orig_provincia', '_orig_localidad'])
        df = df_work
        
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
