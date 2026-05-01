import pandas as pd
import logging
import re
from config import engine_staging

# Configuración de Logging para trazabilidad de calidad
logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

def limpiar_y_transformar_clientes():
    log.info("═══ Transformando Clientes ═══")
    
    # 1. Extracción desde Staging
    df = pd.read_sql("SELECT * FROM clientes", engine_staging)
    total_inicial = len(df)

    # 2. Integridad de ID: Asegura formato numérico y elimina nulos
    df['id_cliente'] = pd.to_numeric(df['id_cliente'], errors='coerce')
    df = df.dropna(subset=['id_cliente'])
    df['id_cliente'] = df['id_cliente'].astype(int)

    # 3. Tratamiento de Duplicados: Mantiene la versión más reciente según el ID
    df = df.drop_duplicates(subset=['id_cliente'], keep='last')
    log.info(f"  ✔ Duplicados e IDs inválidos corregidos: {total_inicial - len(df)}")

    # 4. Limpieza de caracteres raros: Solo letras, espacios y tildes en nombres
    regex_letras = r'[^a-zA-ZáéíóúÁÉÍÓÚñÑ\s]'
    df['nombre'] = df['nombre'].str.replace(regex_letras, '', regex=True).str.strip().str.title()
    df['apellido'] = df['apellido'].str.replace(regex_letras, '', regex=True).str.strip().str.title()

    # 6. Filtro Geográfico: Solo registros de Argentina
    df = df[df['pais'].str.upper().str.strip() == 'ARGENTINA']

    # 7. Validación de Provincias: Cruce contra lista oficial
    provincias_reales = [
        'BUENOS AIRES', 'CATAMARCA', 'CHACO', 'CHUBUT', 'CÓRDOBA', 'CORRIENTES', 
        'ENTRE RÍOS', 'FORMOSA', 'JUJUY', 'LA PAMPA', 'LA RIOJA', 'MENDOZA', 
        'MISIONES', 'NEUQUÉN', 'RÍO NEGRO', 'SALTA', 'SAN JUAN', 'SAN LUIS', 
        'SANTA CRUZ', 'SANTA FE', 'SANTIAGO DEL ESTERO', 
        'TIERRA DEL FUEGO, ANTÁRTIDA E ISLAS DEL ATLÁNTICO SUR', 'TUCUMÁN', 
        'CIUDAD AUTÓNOMA DE BUENOS AIRES'
    ]
    df['provincia'] = df['provincia'].str.upper().str.strip()
    df = df[df['provincia'].isin(provincias_reales)]

    # 8. Conversión de Fechas y Cálculo de Edad
    df['fecha_nacimiento'] = pd.to_datetime(df['fecha_nacimiento'], errors='coerce')
    df = df.dropna(subset=['fecha_nacimiento'])
    
    hoy = pd.Timestamp.now()
    df['edad'] = df['fecha_nacimiento'].apply(lambda x: hoy.year - x.year)

    # 9. Rango Etario Razonable: Filtro para evitar errores de carga (18-100 años)
    df = df[(df['edad'] >= 18) & (df['edad'] <= 100)]

    # 10. Segmentación de Negocio
    df['segmento_persona'] = df['edad'].apply(
        lambda e: 'Joven' if e < 35 else ('Mayor' if e >= 60 else 'Adulto')
    )

    # 11. Normalización de Sexo: Mapeo a F/M/O (Otro)
    mapeo_sexo = {
        'M': 'M', 'MASCULINO': 'M', 'HOMBRE': 'M', 'VARON': 'M',
        'F': 'F', 'FEMENINO': 'F', 'MUJER': 'F'
    }
    df['sexo'] = df['sexo'].astype(str).str.upper().str.strip().map(mapeo_sexo).fillna('O')

    log.info(f"  ✔ Registros limpios y validados: {len(df)}")

    # 12. Volcado a Staging (Zona de Validación): Aislamiento preventivo
    log.info("📥 Volcando datos curados a Staging (val_clientes_validados)...")
    df.to_sql(name="val_clientes_validados", con=engine_staging, if_exists="replace", index=False)   

    return df