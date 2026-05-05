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
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    ruta_maestro = os.path.join(BASE_DIR, "scripts_adicionales", "SourcesAdicionales", "provincias_localidades.csv")

    # 2. Extracción desde Staging
    df = pd.read_sql("SELECT * FROM clientes", engine_staging)
    total_inicial = len(df)

# 3. Integridad de ID y Duplicados (Corregido)
    # Eliminamos el to_numeric porque los IDs son alfanuméricos (ej: CLI-00001)
    df = df.dropna(subset=['id_cliente'])
    df['id_cliente'] = df['id_cliente'].astype(str).str.strip()
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
# 6. Conversión de Fechas y Cálculo de Edad
    # Nos aseguramos de que sea datetime ANTES de calcular
    df['fecha_nacimiento'] = pd.to_datetime(df['fecha_nacimiento'], errors='coerce')
    df = df.dropna(subset=['fecha_nacimiento'])

    hoy = pd.Timestamp.now()
    # Usamos .dt.year que es la forma más rápida y limpia en Pandas
    df['edad'] = hoy.year - df['fecha_nacimiento'].dt.year

    # 7. Reglas de Negocio (Rango etario y Sexo)
    # Ahora 'edad' es un número entero y esta comparación no fallará
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

def limpiar_y_transformar_polizas():
    log.info("═══ Transformando Pólizas ═══")
    
    # 1. Extracción desde Staging (Datos Crudos)
    df = pd.read_sql("SELECT * FROM polizas", engine_staging)
    total_inicial = len(df)
    
    # 2. Integridad de ID y Duplicados
    # Al igual que con clientes, limpiamos los IDs y quitamos duplicados
    df = df.dropna(subset=['id_poliza', 'id_cliente'])
    df['id_poliza'] = df['id_poliza'].astype(str).str.strip()
    df['id_cliente'] = df['id_cliente'].astype(str).str.strip()
    df = df.drop_duplicates(subset=['id_poliza'], keep='last')
    
    # 3. Limpieza de Texto Agresiva (Normalización)
    # Reciclamos tu función normalizar_texto para los campos categóricos
    columnas_texto = ['tipo_seguro', 'cobertura', 'tipo_poliza', 'canal_venta', 'estado']
    for col in columnas_texto:
        df[col] = df[col].apply(normalizar_texto)
        
    # 4. VALIDACIÓN DE INTEGRIDAD REFERENCIAL (El equivalente al Maestro Geo)
    # Solo nos quedamos con las pólizas cuyos clientes hayan sobrevivido al filtro geográfico
    log.info("  🔍 Validando integridad contra clientes válidos...")
    try:
        # Extraemos solo los IDs de la tabla de clientes limpios
        df_clientes_validos = pd.read_sql("SELECT id_cliente FROM val_clientes_validados", engine_staging)
        df_clientes_validos['id_cliente'] = df_clientes_validos['id_cliente'].astype(str).str.strip()
        
        # Inner join: mueren las pólizas de clientes filtrados previamente
        df = df.merge(df_clientes_validos, on='id_cliente', how='inner')
    except Exception as e:
        log.warning("  ⚠ Tabla val_clientes_validados no encontrada. Se omite cruce relacional.")

    # 5. Conversión de Fechas
    columnas_fecha = ['fecha_alta', 'vigencia_desde', 'vigencia_hasta']
    for col in columnas_fecha:
        df[col] = pd.to_datetime(df[col], errors='coerce')
        
    # 6. Reglas de Negocio Numéricas (Limpieza de montos)
    columnas_numericas = ['prima_mensual', 'prima_total', 'customer_lifetime_value', 'numero_polizas_cliente', 'meses_desde_inicio']
    for col in columnas_numericas:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
        
    # 7. Reglas de Negocio Específicas (Actualización de Estado)
    # Si la fecha de vigencia_hasta ya pasó al día de hoy, forzamos el estado a 'VENCIDA'
    hoy = pd.Timestamp.now()
    df.loc[df['vigencia_hasta'] < hoy, 'estado'] = 'VENCIDA'
    
    log.info(f"  ✔ Pólizas finales tras validación cruzada: {len(df)}")

    # 8. Volcado a Staging (Zona de Validación)
    log.info("📥 Volcando datos curados a Staging (val_polizas_validadas)...")
    df.to_sql(name="val_polizas_validadas", con=engine_staging, if_exists="replace", index=False)
    
    return df

def limpiar_y_transformar_autoinsurance():
    log.info("═══ Transformando AutoInsurance ═══")
    
    # 1. Extracción desde Staging
    df_auto = pd.read_sql("SELECT * FROM autoinsurance", engine_staging)
    
    # 2. Limpieza de Texto (Demográficos y del Vehículo)
    cols_texto = [
        'response', 'coverage', 'education', 'employmentstatus', 
        'gender', 'location_code', 'marital_status', 'vehicle_class', 'vehicle_size'
    ]
    for col in cols_texto:
        df_auto[col] = df_auto[col].apply(normalizar_texto)
        
    # 3. Limpieza de Fechas
    df_auto['effective_to_date'] = pd.to_datetime(df_auto['effective_to_date'], errors='coerce')
    
    # 4. CRUCE Y VALIDACIÓN REFERENCIAL (El "truco" matemático)
    log.info("  🔍 Mapeando IDs externos contra el Data Warehouse interno...")
    try:
        # Traemos nuestras pólizas validadas para usarlas de puente
        df_polizas = pd.read_sql("SELECT id_poliza, id_cliente, customer_lifetime_value, prima_mensual FROM val_polizas_validadas", engine_staging)
        
        # Preparamos las llaves de cruce: redondeamos CLV para evitar errores de coma flotante
        df_auto['join_clv'] = df_auto['customer_lifetime_value'].round(2)
        df_polizas['join_clv'] = df_polizas['customer_lifetime_value'].round(2)
        
        # Ajustamos la escala de la prima (Claude la multiplicó por 100 en el CSV de pólizas)
        df_auto['join_premium'] = (df_auto['monthly_premium_auto'] * 100).astype(float)
        df_polizas['join_premium'] = df_polizas['prima_mensual'].astype(float)
        
        # Hacemos el Inner Join para heredar el id_cliente y el id_poliza reales
        df_cruce = df_auto.merge(df_polizas, on=['join_clv', 'join_premium'], how='inner')
        
        # Eliminamos duplicados por si acaso dos clientes tienen el mismo CLV y prima exacta
        df_cruce = df_cruce.drop_duplicates(subset=['customer'], keep='first')
        
        # Limpiamos la basura del cruce
        df_cruce = df_cruce.drop(columns=['join_clv', 'join_premium', 'customer_lifetime_value_y', 'prima_mensual'])
        df_cruce = df_cruce.rename(columns={'customer_lifetime_value_x': 'customer_lifetime_value'})
        
        # 5. Validación final contra Clientes (Supervivencia Geográfica)
        # Si el cliente fue borrado por localidad falsa, borramos su info de auto también
        df_clientes = pd.read_sql("SELECT id_cliente FROM val_clientes_validados", engine_staging)
        df = df_cruce.merge(df_clientes, on='id_cliente', how='inner')
        
    except Exception as e:
        log.warning(f"  ⚠ Error en el cruce relacional: {e}. Se guardará sin mapeo de IDs.")
        df = df_auto

    log.info(f"  ✔ Registros finales tras validación cruzada: {len(df)}")

    # 6. Volcado a Staging
    log.info("📥 Guardando val_autoinsurance_validadas...")
    df.to_sql(name="val_autoinsurance_validadas", con=engine_staging, if_exists="replace", index=False)
    # LAS VALIDADAS NO SE DAN POR ID, SINO POR PRIMA Y Customer Lifetime Value, SE AGREGA id_cliente A LA TABLA DE VALIDADOS
    #Ejemplo 1: El Cliente QZ44356
    # En tu tabla final, este cliente quedó asociado a CLI-00002 y a la póliza POL-000002. Si miramos los archivos crudos, vemos por qué:
    # En AutoInsurance: El Customer QZ44356 tenía un CLV de 6979.535 y una prima de 94.
    # En Pólizas: La póliza POL-000002 tenía un CLV de 6979.54 (redondeado) y una prima de 9400.0.
    # Resultado: El script detectó la coincidencia numérica, "absorbió" el ID de tu Data Warehouse y unió toda la información en una sola fila.
    return df

def limpiar_y_transformar_evaluaciones():
    log.info("═══ Transformando Evaluaciones de Peritos ═══")
    
    # 1. Extracción desde Staging (Datos Crudos)
    df = pd.read_sql("SELECT * FROM evaluaciones", engine_staging)
    
    # 2. Integridad de IDs y Duplicados
    # Limpiamos espacios y eliminamos nulos en las llaves primarias y foráneas
    columnas_id = ['id_evaluacion', 'id_parte', 'id_perito']
    df = df.dropna(subset=columnas_id)
    for col in columnas_id:
        df[col] = df[col].astype(str).str.strip()
        
    df = df.drop_duplicates(subset=['id_evaluacion'], keep='last')
    
    # 3. Conversión de Fechas
    df['fecha_visita'] = pd.to_datetime(df['fecha_visita'], errors='coerce')
    
    # 4. Limpieza de Montos
    df['monto_estimado_dano'] = pd.to_numeric(df['monto_estimado_dano'], errors='coerce').fillna(0.0)
    
    # 5. Normalización de Texto Libre
    # El dictamen suele venir con tildes y caracteres especiales, lo normalizamos
    df['dictamen'] = df['dictamen'].apply(normalizar_texto)
    
    # 6. Estandarización de Booleanos
    # Nos aseguramos de que 'requiere_reinspeccion' sea interpretado correctamente por MySQL
    df['requiere_reinspeccion'] = df['requiere_reinspeccion'].astype(bool)
    
    log.info(f"  ✔ Evaluaciones procesadas correctamente: {len(df)}")

    # 7. Volcado a Staging (Zona de Validación)
    log.info("📥 Guardando val_evaluaciones_validadas en Staging...")
    df.to_sql(name="val_evaluaciones_validadas", con=engine_staging, if_exists="replace", index=False)
    
    return df

def limpiar_y_transformar_peritos():
    log.info("═══ Transformando Peritos ═══")
    
    # 1. Extracción desde Staging
    df = pd.read_sql("SELECT * FROM peritos", engine_staging)
    total_inicial = len(df)

    # 2. Integridad de ID
    df = df.dropna(subset=['id_perito'])
    df['id_perito'] = df['id_perito'].astype(str).str.strip()

    # 3. Filtro: Solo peritos activos
    df = df[df['activo'].astype(str).str.strip().isin(['1', 'True', 'true', 'TRUE'])]

    log.info(f"  ✔ Peritos activos: {len(df)} de {total_inicial}")

    # 4. Combinar nombre y apellido y quedarse solo con eso
    df['nombre'] = df['nombre'].apply(normalizar_texto)
    df['apellido'] = df['apellido'].apply(normalizar_texto)
    df = pd.DataFrame({
    'id_perito': df['id_perito'],
    'nombre_completo': df['apellido'] + ', ' + df['nombre']
    })

    log.info(f"  ✔ Peritos procesados correctamente: {len(df)}")

    # 4. Volcado a Staging
    log.info("📥 Guardando val_peritos_validados en Staging...")
    df.to_sql(name="val_peritos_validados", con=engine_staging, if_exists="replace", index=False)

    return df

def limpiar_y_transformar_pagos():
    log.info("═══ Transformando Pagos ═══")
    
    # 1. Extracción desde Staging
    df = pd.read_sql("SELECT * FROM pagos", engine_staging)
    total_inicial = len(df)

    # 2. Integridad de IDs y Duplicados
    df = df.dropna(subset=['id_pago', 'id_parte', 'id_receptor'])
    df['id_pago'] = df['id_pago'].astype(str).str.strip()
    df['id_parte'] = df['id_parte'].astype(str).str.strip()
    df['id_receptor'] = df['id_receptor'].astype(str).str.strip()
    df = df.drop_duplicates(subset=['id_pago'], keep='last')

    # 3. Limpieza de monto
    df['monto_pagado'] = pd.to_numeric(df['monto_pagado'], errors='coerce').fillna(0.0)
    df['fecha_pago'] = pd.to_datetime(df['fecha_pago'], errors='coerce')

    # 4. Quedarse solo con las columnas necesarias
    df = pd.DataFrame({
        'id_pago': df['id_pago'],
        'id_parte': df['id_parte'],
        'id_receptor': df['id_receptor'],
        'monto_pagado': df['monto_pagado'],
        'fecha_pago': df['fecha_pago']
    })

    log.info(f"  ✔ Pagos procesados correctamente: {len(df)} de {total_inicial}")

    # 5. Volcado a Staging
    log.info("📥 Guardando val_pagos_validados en Staging...")
    df.to_sql(name="val_pagos_validados", con=engine_staging, if_exists="replace", index=False)

    return df

def limpiar_y_transformar_objetos():
    log.info("═══ Transformando Objetos Asegurados ═══")
    
    # 1. Extracción desde Staging
    df = pd.read_sql("SELECT * FROM objetos", engine_staging)
    total_inicial = len(df)

    # 2. Integridad de ID y Duplicados
    df = df.dropna(subset=['id_objeto'])
    df['id_objeto'] = df['id_objeto'].astype(str).str.strip()
    df = df.drop_duplicates(subset=['id_objeto'], keep='last')

    # 3. Normalización de Texto
    df['tipo_objeto'] = df['tipo_objeto'].apply(normalizar_texto)
    df['descripcion'] = df['descripcion'].apply(normalizar_texto)
    df['localidad'] = df['localidad'].apply(normalizar_texto)
    df['provincia'] = df['provincia'].apply(normalizar_texto)
    df['marca'] = df['marca'].apply(normalizar_texto)
    df['modelo'] = df['modelo'].apply(normalizar_texto)

    # 4. Limpieza de Numéricos
    df['valor_asegurado'] = pd.to_numeric(df['valor_asegurado'], errors='coerce').fillna(0.0)
    df['valor_inmueble'] = pd.to_numeric(df['valor_inmueble'], errors='coerce').fillna(0.0)
    df['superficie_m2'] = pd.to_numeric(df['superficie_m2'], errors='coerce').fillna(0.0)
    df['año_fabricacion'] = pd.to_numeric(df['año_fabricacion'], errors='coerce').fillna(0)
    df['año_construccion'] = pd.to_numeric(df['año_construccion'], errors='coerce').fillna(0)

    log.info(f"  ✔ Objetos procesados correctamente: {len(df)} de {total_inicial}")

    # 5. Volcado a Staging
    log.info("📥 Guardando val_objetos_validados en Staging...")
    df.to_sql(name="val_objetos_validados", con=engine_staging, if_exists="replace", index=False)

    return df