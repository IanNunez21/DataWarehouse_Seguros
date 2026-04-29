import os
import pandas as pd
from sqlalchemy import create_engine
import logging

#ESTO NO MODIFICA NADA AUN, UNICAMENTE GUARDA EN MEMORIA LA INFO YA CURADA (ADEMAS FALTA 
# CURARLA AUN MAS), NO LA MANDA A WORKBENCH

# Configuración de conexiones

USER = "root"
PASSWORD = "root"
HOST = "localhost"

engine_staging = create_engine(f"mysql+mysqlconnector://{USER}:{PASSWORD}@{HOST}/dw_staging")
engine_dw = create_engine(f"mysql+mysqlconnector://{USER}:{PASSWORD}@{HOST}/data_warehouse_seguros")

# Configuración de Logging para trazabilidad
logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

def limpiar_y_transformar_clientes():
    log.info("=== Procesando tabla: Clientes ===")
    
    # 1. Extracción desde Staging
    df = pd.read_sql("SELECT * FROM clientes", engine_staging)
    total_inicial = len(df)

    # 2. Eliminación de Duplicados
    # Eliminamos registros con el mismo ID, manteniendo el último cargado
    df = df.drop_duplicates(subset=['id_cliente'], keep='last')
    log.info(f"  ✔ Duplicados eliminados: {total_inicial - len(df)}")

    # 3. Limpieza de datos (Manejo de Nulos)
    # Descartamos si no tienen datos críticos como ID o Provincia
    df = df.dropna(subset=['id_cliente', 'apellido', 'provincia'])

    # 4. Conversión de tipos
    # Aseguramos que las fechas y montos tengan el formato correcto para MySQL
    df['fecha_nacimiento'] = pd.to_datetime(df['fecha_nacimiento'], errors='coerce')
    df['ingreso_anual'] = pd.to_numeric(df['ingreso_anual'], errors='coerce').fillna(0.0)
    # Eliminamos filas con fechas de nacimiento imposibles de parsear
    df = df.dropna(subset=['fecha_nacimiento'])

    # 5. Normalización de formatos
    # Estandarizamos textos para evitar que "Resistencia" y "resistencia" sean distintos
    df['nombre'] = df['nombre'].str.strip().str.title()
    df['apellido'] = df['apellido'].str.strip().str.title()
    df['localidad'] = df['localidad'].str.strip().str.title()
    
    # Lógica de Negocio (Preparación para SCD Tipo 2)
    # Calculamos el segmento para tu análisis de perfil de cliente
    hoy = pd.Timestamp.now()
    df['edad'] = df['fecha_nacimiento'].apply(lambda x: hoy.year - x.year)
    df['segmento_persona'] = df['edad'].apply(
        lambda e: 'Joven' if e < 35 else ('Mayor' if e >= 60 else 'Adulto')
    )

    log.info(f"  ✔ Registros finales listos: {len(df)}")
    return df

if __name__ == "__main__":
    df_limpio = limpiar_y_transformar_clientes()
    # Aquí seguiría la carga a la dimensión final, pero por ahora no lo hacemos