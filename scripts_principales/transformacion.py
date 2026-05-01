import pandas as pd
import logging
import os
import unicodedata
from config import engine_staging

# Configuración de Logging
logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

def normalizar_texto(texto):
    """Limpia tildes, acentos y normaliza a mayúsculas para evitar errores de matching."""
    if not isinstance(texto, str):
        return str(texto)
    texto_nfd = unicodedata.normalize('NFD', texto)
    texto_limpio = "".join(c for c in texto_nfd if unicodedata.category(c) != 'Mn')
    return texto_limpio.upper().strip()

def limpiar_y_transformar_clientes():
    log.info("═══ Transformando Clientes ═══")
    
    # 1. Rutas para el Maestro Geográfico
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    ruta_maestro = os.path.join(BASE_DIR, "scripts_adicionales", "SourcesAdicionales", "provincias_localidades.csv")

    # 2. Extracción desde Staging
    df = pd.read_sql("SELECT * FROM clientes", engine_staging)
    total_inicial = len(df)

    # 3. Integridad de ID y Duplicados[cite: 5]
    df['id_cliente'] = pd.to_numeric(df['id_cliente'], errors='coerce')
    df = df.dropna(subset=['id_cliente'])
    df['id_cliente'] = df['id_cliente'].astype(int)
    df = df.drop_duplicates(subset=['id_cliente'], keep='last')
    
    # 4. Limpieza de Texto Agresiva (Normalización)
    # Aplicamos normalizar_texto para quitar tildes y caracteres raros en nombres y apellidos
    df['nombre'] = df['nombre'].apply(normalizar_texto)
    df['apellido'] = df['apellido'].apply(normalizar_texto)

    # 5. VALIDACIÓN GEOGRÁFICA: Cruce contra el Maestro CSV[
    log.info("  🔍 Validando consistencia Localidad/Provincia...")
    try:
        df_maestro = pd.read_csv(ruta_maestro)
        # Aseguramos que el maestro esté en el mismo formato de comparación
        df_maestro['provincia'] = df_maestro['provincia'].apply(normalizar_texto)
        df_maestro['localidad'] = df_maestro['localidad'].apply(normalizar_texto)
        
        # Normalizamos los datos de entrada antes del cruce
        df['provincia'] = df['provincia'].apply(normalizar_texto)
        df['localidad'] = df['localidad'].apply(normalizar_texto)

        # Realizamos el Inner Join: solo sobreviven las combinaciones reales[cite: 5]
        df = df.merge(df_maestro, on=['provincia', 'localidad'], how='inner')
        
    except FileNotFoundError:
        log.warning("  ⚠ Maestro no encontrado en la ruta. Se omite validación geo profunda.")
        # Como fallback, al menos filtramos por Argentina[cite: 5]
        df = df[df['pais'].str.upper().str.strip() == 'ARGENTINA']

    # 6. Conversión de Fechas y Cálculo de Edad[cite: 5]
    df['fecha_nacimiento'] = pd.to_datetime(df['fecha_nacimiento'], errors='coerce')
    df = df.dropna(subset=['fecha_nacimiento'])
    
    hoy = pd.Timestamp.now()
    df['edad'] = df['fecha_nacimiento'].apply(lambda x: hoy.year - x.year)

    # 7. Reglas de Negocio (Rango etario y Sexo)[cite: 5]
    df = df[(df['edad'] >= 18) & (df['edad'] <= 100)]
    
    mapeo_sexo = {
        'M': 'M', 'MASCULINO': 'M', 'HOMBRE': 'M', 'VARON': 'M',
        'F': 'F', 'FEMENINO': 'F', 'MUJER': 'F'
    }
    df['sexo'] = df['sexo'].astype(str).apply(normalizar_texto).map(mapeo_sexo).fillna('O')

    # 8. Segmentación[cite: 5]
    df['segmento_persona'] = df['edad'].apply(
        lambda e: 'Joven' if e < 35 else ('Mayor' if e >= 60 else 'Adulto')
    )

    log.info(f"  ✔ Registros finales tras validación cruzada: {len(df)}")

    # 9. Volcado a Staging (Zona de Validación)[cite: 3, 5]
    log.info("📥 Volcando datos curados a Staging (val_clientes_validados)...")
    df.to_sql(name="val_clientes_validados", con=engine_staging, if_exists="replace", index=False)   

    return df