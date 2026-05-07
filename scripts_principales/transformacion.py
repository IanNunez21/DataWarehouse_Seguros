import pandas as pd
import logging
import os
from config import engine_staging
from utils import normalizar_texto, guardar_datos_curados, convertir_fechas, normalizar_columnas_texto, validar_geografia, limpiar_numericos, crear_nombre_completo, limpiar_ids

# Configuración de Logging
logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

def limpiar_y_transformar_clientes():
    log.info("═══ Transformando Clientes ═══")
    
    # 1. Extracción desde Staging
    df = pd.read_sql("SELECT * FROM clientes", engine_staging)
    total_inicial = len(df)

    # 2. Integridad de ID y Duplicados
    df = limpiar_ids(df, columnas_id='id_cliente', id_principal='id_cliente')
    
    # 3. Limpieza de Texto Agresiva (Normalización)
    # Aplicamos normalizar_texto para quitar tildes y caracteres raros en nombres y apellidos
    df = normalizar_columnas_texto(df, ['nombre', 'apellido'])
    df = crear_nombre_completo(df)

    # 4. VALIDACIÓN GEOGRÁFICA: Cruce contra el Maestro CSV
    df = validar_geografia(df)
# 6. Conversión de Fechas y Cálculo de Edad
    # Nos aseguramos de que sea datetime ANTES de calcular
    df = convertir_fechas(df, 'fecha_nacimiento')
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

    # 8. Segmentación
    df['segmento_persona'] = df['edad'].apply(
        lambda e: 'Joven' if e < 35 else ('Mayor' if e >= 60 else 'Adulto')
    )

    log.info(f"  ✔ Registros procesados correctamente: {len(df)} de {total_inicial}")

    # 9. Volcado a Staging y Exportación CSV
    return guardar_datos_curados(df, "val_clientes_validados")

def limpiar_y_transformar_polizas():
    log.info("═══ Transformando Pólizas ═══")
    
    # 1. Extracción desde Staging (Datos Crudos)
    df = pd.read_sql("SELECT * FROM polizas", engine_staging)
    total_inicial = len(df)
    
    # 2. Integridad de ID y Duplicados
    df = limpiar_ids(df, columnas_id=['id_poliza', 'id_cliente'], id_principal='id_poliza')
    
    # 3. Limpieza de Texto Agresiva (Normalización)
    # Reciclamos tu función normalizar_texto para los campos categóricos
    columnas_texto = ['tipo_seguro', 'cobertura', 'tipo_poliza', 'canal_venta', 'estado']
    df = normalizar_columnas_texto(df, columnas_texto)
        
    # 4. VALIDACIÓN DE INTEGRIDAD REFERENCIAL (El equivalente al Maestro Geo)
    # Solo nos quedamos con las pólizas cuyos clientes hayan sobrevivido al filtro geográfico
    try:
        # Extraemos solo los IDs de la tabla de clientes limpios
        df_clientes_validos = pd.read_sql("SELECT id_cliente FROM val_clientes_validados", engine_staging)
        df_clientes_validos['id_cliente'] = df_clientes_validos['id_cliente'].astype(str).str.strip()
        
        # Inner join: mueren las pólizas de clientes filtrados previamente
        df = df.merge(df_clientes_validos, on='id_cliente', how='inner')
    except Exception as e:
        log.warning("  ⚠ Tabla val_clientes_validados no encontrada. Se omite cruce relacional.")

    # 5. Conversión de Fechas
    df = convertir_fechas(df, ['fecha_alta', 'vigencia_desde', 'vigencia_hasta'])
        
    # 6. Reglas de Negocio Numéricas (Limpieza de montos)
    columnas_numericas = ['prima_mensual', 'prima_total', 'customer_lifetime_value', 'numero_polizas_cliente', 'meses_desde_inicio']
    df = limpiar_numericos(df, columnas_numericas)
        
    # 7. Reglas de Negocio Específicas (Actualización de Estado)
    # Si la fecha de vigencia_hasta ya pasó al día de hoy, forzamos el estado a 'VENCIDA'
    hoy = pd.Timestamp.now()
    df.loc[df['vigencia_hasta'] < hoy, 'estado'] = 'VENCIDA'
    
    log.info(f"  ✔ Registros procesados correctamente: {len(df)} de {total_inicial}")

    # 8. Volcado a Staging y Exportación CSV
    return guardar_datos_curados(df, "val_polizas_validadas")

def limpiar_y_transformar_autoinsurance():
    log.info("═══ Transformando AutoInsurance ═══")
    
    # 1. Extracción desde Staging
    df_auto = pd.read_sql("SELECT * FROM autoinsurance", engine_staging)
    total_inicial = len(df_auto)
    
    # 2. Limpieza de Texto (Demográficos y del Vehículo)
    cols_texto = [
        'response', 'coverage', 'education', 'employmentstatus', 
        'gender', 'location_code', 'marital_status', 'vehicle_class', 'vehicle_size'
    ]
    df_auto = normalizar_columnas_texto(df_auto, cols_texto)
        
    # 3. Limpieza de Fechas
    df_auto = convertir_fechas(df_auto, 'effective_to_date', formato='%m/%d/%y')
    
    # 4. CRUCE Y VALIDACIÓN REFERENCIAL (El "truco" matemático)
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

    log.info(f"  ✔ Registros procesados correctamente: {len(df)} de {total_inicial}")

    # 6. Volcado a Staging y Exportación CSV
    df = guardar_datos_curados(df, "val_autoinsurance_validadas")
    
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
    total_inicial = len(df)
    
    # 2. Integridad de IDs y Duplicados
    columnas_id = ['id_evaluacion', 'id_parte', 'id_perito']
    df = limpiar_ids(df, columnas_id=columnas_id, id_principal='id_evaluacion')
    
    # 3. Conversión de Fechas
    df = convertir_fechas(df, 'fecha_visita')
    
    # 4. Limpieza de Montos
    df = limpiar_numericos(df, 'monto_estimado_dano')
    
    # 5. Normalización de Texto Libre
    # El dictamen suele venir con tildes y caracteres especiales, lo normalizamos
    df = normalizar_columnas_texto(df, 'dictamen')
    
    # 6. Estandarización de Booleanos
    # Nos aseguramos de que 'requiere_reinspeccion' sea interpretado correctamente por MySQL
    df['requiere_reinspeccion'] = df['requiere_reinspeccion'].astype(bool)
    
    log.info(f"  ✔ Registros procesados correctamente: {len(df)} de {total_inicial}")

    # 7. Volcado a Staging y Exportación CSV
    return guardar_datos_curados(df, "val_evaluaciones_validadas")

def limpiar_y_transformar_peritos():
    log.info("═══ Transformando Peritos ═══")
    
    # 1. Extracción desde Staging
    df = pd.read_sql("SELECT * FROM peritos", engine_staging)
    total_inicial = len(df)

    # 2. Integridad de ID
    df = limpiar_ids(df, columnas_id='id_perito', id_principal='id_perito')

    # 3. Filtro: Solo peritos activos
    df = df[df['activo'].astype(str).str.strip().isin(['1', 'True', 'true', 'TRUE'])]

    # 4. Normalizar y crear nombre_completo (conservando nombre y apellido)
    df = normalizar_columnas_texto(df, ['nombre', 'apellido'])
    df = crear_nombre_completo(df)
    
    # Seleccionar columnas de interés
    df = df[['id_perito', 'nombre', 'apellido', 'nombre_completo']]

    log.info(f"  ✔ Registros procesados correctamente: {len(df)} de {total_inicial}")

    # 4. Volcado a Staging y Exportación CSV
    return guardar_datos_curados(df, "val_peritos_validados")

def limpiar_y_transformar_pagos():
    log.info("═══ Transformando Pagos ═══")
    
    # 1. Extracción desde Staging
    df = pd.read_sql("SELECT * FROM pagos", engine_staging)
    total_inicial = len(df)

    # 2. Integridad de IDs y Duplicados
    columnas_id = ['id_pago', 'id_parte', 'id_receptor']
    df = limpiar_ids(df, columnas_id=columnas_id, id_principal='id_pago')

    # 3. Limpieza de monto
    df = limpiar_numericos(df, 'monto_pagado')
    df = convertir_fechas(df, 'fecha_pago')

    # 4. Quedarse solo con las columnas necesarias
    df = df[
        [
            'id_pago',
            'id_parte',
            'id_receptor',
            'monto_pagado',
            'fecha_pago'
        ]
    ]

    log.info(f"  ✔ Registros procesados correctamente: {len(df)} de {total_inicial}")

    # 5. Volcado a Staging y Exportación CSV
    return guardar_datos_curados(df, "val_pagos_validados")

def limpiar_y_transformar_objetos():
    log.info("═══ Transformando Objetos Asegurados ═══")
    
    # 1. Extracción desde Staging
    df = pd.read_sql("SELECT * FROM objetos", engine_staging)
    total_inicial = len(df)

    # 2. Integridad de ID y Duplicados
    df = limpiar_ids(df, columnas_id='id_objeto', id_principal='id_objeto')

    # 3. Normalización de Texto
    df = normalizar_columnas_texto(df, ['tipo_objeto', 'descripcion', 'localidad', 'provincia', 'marca', 'modelo'])

    # 4. Limpieza de Numéricos
    df = limpiar_numericos(df, ['valor_asegurado', 'valor_inmueble', 'superficie_m2'])
    df = limpiar_numericos(df, ['año_fabricacion', 'año_construccion'], valor_defecto=0)

    log.info(f"  ✔ Registros procesados correctamente: {len(df)} de {total_inicial}")

    # 5. Volcado a Staging y Exportación CSV
    return guardar_datos_curados(df, "val_objetos_validados")

def limpiar_y_transformar_agentes():
    log.info("═══ Transformando Agentes ═══")

    # 1. Extracción desde Staging
    df = pd.read_sql("SELECT * FROM agentes", engine_staging)
    total_inicial = len(df)

    # 2. Integridad de ID y Duplicados
    df = limpiar_ids(df, columnas_id='id_agente', id_principal='id_agente')

    # 3. Normalización de textos
    columnas_texto = ["nombre", "apellido", "canal", "sucursal", "zona", "email"]
    df = normalizar_columnas_texto(df, columnas_texto)

    # 4. Filtrar solo agentes activos
    df["activo"] = df["activo"].astype(str).str.strip().str.upper()
    df = df[df["activo"].isin(["TRUE", "1", "SI", "SÍ"])]

    # 5. Conversión de fecha de ingreso
    df = convertir_fechas(df, "fecha_ingreso")
    df = df.dropna(subset=["fecha_ingreso"])

    # 6. Validación de canal
    canales_validos = ["SUCURSAL", "CALL CENTER", "AGENTE", "WEB"]
    df = df[df["canal"].isin(canales_validos)]

    # 7. Crear nombre completo para la dimensión
    df = crear_nombre_completo(df)

    log.info(f"  ✔ Registros procesados correctamente: {len(df)} de {total_inicial}")

    # 9. Guardar tabla validada en staging y Exportación CSV
    return guardar_datos_curados(df, "val_agentes_validados")

def limpiar_y_transformar_partes():
    log.info("═══ Transformando Partes de Accidente ═══")

    # 1. Extracción desde Staging
    df = pd.read_sql("SELECT * FROM partes", engine_staging)
    total_inicial = len(df)

    # 2. Integridad de IDs y Duplicados
    columnas_id = [
        "id_parte",
        "id_poliza",
        "id_objeto_asegurado",
        "id_perito",
        "id_denunciante",
        "id_receptor_pago"
    ]
    df = limpiar_ids(df, columnas_id=columnas_id, columnas_dropna=["id_parte", "id_poliza"], id_principal="id_parte")

    # 3. Conversión de fechas
    df = convertir_fechas(df, ["fecha_apertura", "fecha_cierre"])

    df = df.dropna(subset=["fecha_apertura"])

    # 4. Regla de consistencia temporal
    # Si hay fecha de cierre, no puede ser anterior a la fecha de apertura.
    df = df[
        df["fecha_cierre"].isna()
        | (df["fecha_cierre"] >= df["fecha_apertura"])
    ]

    # 5. Limpieza de monto reclamado
    df = limpiar_numericos(df, "monto_reclamado")
    df = df[df["monto_reclamado"] >= 0]

    # 6. Limpieza de días de resolución
    df = limpiar_numericos(df, "dias_resolucion", valor_defecto=0)
    df = df[df["dias_resolucion"] >= 0]

    # 7. Normalización de textos
    columnas_texto = ["tipo_siniestro", "estado", "tipo_seguro"]
    df = normalizar_columnas_texto(df, columnas_texto)

    log.info(f"  ✔ Registros procesados correctamente: {len(df)} de {total_inicial}")

    # 9. Guardar tabla validada en staging y Exportación CSV
    return guardar_datos_curados(df, "val_partes_validados")

def limpiar_y_transformar_garantias():
    log.info("═══ Transformando Garantías ═══")
    
    # 1. Extracción desde Staging
    df = pd.read_sql("SELECT * FROM garantias", engine_staging)
    total_inicial = len(df)

    # 2. Limpieza de datos (Filtros y Nulos)
    df = df[df['activa'].astype(str).str.strip().str.lower().isin(['true', '1'])].copy()
    df.drop(columns=['activa'], inplace=True)
    
    df = limpiar_ids(df, columnas_id='id_poliza', columnas_dropna=['id_poliza', 'tipo_garantia', 'suma_garantizada'])
    df = df.drop_duplicates(keep='last')

    # 3. Conversión de tipos y normalización
    df = limpiar_numericos(df, 'suma_garantizada')
    df['tipo_garantia'] = df['tipo_garantia'].str.strip().str.capitalize()

    # 4. Verificación de constraints (suma_garantizada > 0)
    invalidos = df[df['suma_garantizada'] <= 0]
    if len(invalidos) > 0:
        log.warning(f"  ⚠ Atención: Hay {len(invalidos)} garantías con suma <= 0. Serán omitidas.")
        df = df[df['suma_garantizada'] > 0]

    log.info(f"  ✔ Registros procesados correctamente: {len(df)} de {total_inicial}")
    
    # 5. Guardar tabla validada en Staging
    return guardar_datos_curados(df, "val_garantias_validadas")