import pandas as pd
import logging
import os
from config import engine_staging
from utils import normalizar_texto, guardar_datos_curados, convertir_fechas, normalizar_columnas_texto, validar_geografia, limpiar_numericos, crear_nombre_completo, limpiar_ids

# Configuración de Logging
logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

def limpiar_y_transformar_clientes():
    
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
    # 5. Conversión de Fechas y Cálculo de Edad
    # Nos aseguramos de que sea datetime ANTES de calcular
    df = convertir_fechas(df, 'fecha_nacimiento')

    hoy = pd.Timestamp.now()
    # Usamos .dt.year que es la forma más rápida y limpia en Pandas
    df['edad'] = hoy.year - df['fecha_nacimiento'].dt.year

    # 6. Reglas de Negocio (Rango etario y Sexo)
    # Ahora 'edad' es un número entero y esta comparación no fallará
    df = df[(df['edad'] >= 18) & (df['edad'] <= 100)]
    
    mapeo_sexo = {
        'M': 'M', 'MASCULINO': 'M', 'HOMBRE': 'M', 'VARON': 'M',
        'F': 'F', 'FEMENINO': 'F', 'MUJER': 'F'
    }
    df['sexo'] = df['sexo'].astype(str).apply(normalizar_texto).map(mapeo_sexo).fillna('O')

    def estandarizar_laboral(texto):
        if pd.isna(texto) or str(texto).strip() == '':
            return 'Other'
        t = normalizar_texto(texto)
        
        # 1. Desempleado (incluye Disabled y sin trabajo)
        if any(palabra in t for palabra in ['UNEMPLOY', 'DESEMPL', 'SIN TRAB', 'DISABL', 'INACTIV']):
            return 'Desempleado'
        # 2. Jubilado
        elif any(palabra in t for palabra in ['RETIRE', 'JUB']):
            return 'Jubilado'
        # 3. Licencia Médica
        elif any(palabra in t for palabra in ['MEDIC', 'LICENCIA', 'LEAVE']):
            return 'Licencia medica'
        # 4. Empleado (atrapa Employed, Self-Employed, empleado, emp., activo)
        elif any(palabra in t for palabra in ['EMP', 'ACTIV']):
            return 'Empleado'
        else:
            return 'Other'

    df['situacion_laboral'] = df['situacion_laboral'].apply(estandarizar_laboral)

    # 7. Segmentación
    df['segmento_persona'] = df['edad'].apply(
        lambda e: 'Joven' if e < 35 else ('Mayor' if e >= 60 else 'Adulto')
    )

    log.info(f"  Procesados: {len(df)} de {total_inicial}")

    # 8. Volcado a Staging y Exportación CSV
    return guardar_datos_curados(df, "val_clientes_validados")

def limpiar_y_transformar_polizas():
    
    # 1. Extracción desde Staging (Datos Crudos)
    df = pd.read_sql("SELECT * FROM polizas", engine_staging)
    total_inicial = len(df)
    
    # 2. Integridad de ID y Duplicados
    df = limpiar_ids(df, columnas_id=['id_poliza', 'id_cliente'], id_principal='id_poliza')
    
    # 3. Limpieza de Texto Agresiva (Normalización)
    # Reciclamos tu función normalizar_texto para los campos categóricos
    columnas_texto = ['tipo_seguro', 'cobertura', 'tipo_poliza', 'canal_venta', 'estado']
    df = normalizar_columnas_texto(df, columnas_texto)
        
    # 4. VALIDACIÓN DE INTEGRIDAD REFERENCIAL
    # Solo nos quedamos con las pólizas cuyos clientes y agentes sean válidos
    try:
        val_clientes = pd.read_sql("SELECT id_cliente FROM val_clientes_validados", engine_staging)
        val_agentes = pd.read_sql("SELECT id_agente FROM val_agentes_validados", engine_staging)
        
        # Filtrado todo en uno
        df = df[df['id_cliente'].isin(val_clientes['id_cliente']) & df['id_agente'].isin(val_agentes['id_agente'])]
    except Exception as e:
        log.debug(f"  Integridad referencial polizas omitida: {e}")

    # 5. Conversión de Fechas
    df = convertir_fechas(df, ['fecha_alta', 'vigencia_desde', 'vigencia_hasta'])
        
    # 6. Reglas de Negocio Numéricas (Limpieza de montos)
    columnas_numericas = ['prima_mensual', 'prima_total', 'customer_lifetime_value', 'numero_polizas_cliente', 'meses_desde_inicio']
    df = limpiar_numericos(df, columnas_numericas)
    
    log.info(f"  Procesados: {len(df)} de {total_inicial}")

    # 7. Volcado a Staging y Exportación CSV
    return guardar_datos_curados(df, "val_polizas_validadas")


def limpiar_y_transformar_evaluaciones():
    
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
    
    # 7. Integridad Referencial: Solo partes de accidente y peritos válidos
    try:
        val_partes = pd.read_sql("SELECT id_parte FROM val_partes_validados", engine_staging)
        val_peritos = pd.read_sql("SELECT id_perito FROM val_peritos_validados", engine_staging)
        
        df = df[df['id_parte'].isin(val_partes['id_parte']) & df['id_perito'].isin(val_peritos['id_perito'])]
    except Exception as e:
        log.warning(f"  ⚠ No se pudo validar la integridad referencial con Partes o Peritos: {e}")

    log.info(f"  Procesados: {len(df)} de {total_inicial}")

    # 8. Volcado a Staging y Exportación CSV
    return guardar_datos_curados(df, "val_evaluaciones_validadas")

def limpiar_y_transformar_peritos():
    
    # 1. Extracción desde Staging
    df = pd.read_sql("SELECT * FROM peritos", engine_staging)
    total_inicial = len(df)

    # 2. Integridad de ID
    df = limpiar_ids(df, columnas_id='id_perito', id_principal='id_perito')

    # 3. Normalizar y crear nombre_completo (conservando nombre y apellido)
    df = normalizar_columnas_texto(df, ['nombre', 'apellido'])
    df = crear_nombre_completo(df)
    
    # Seleccionar columnas de interés
    df = df[['id_perito', 'nombre', 'apellido', 'nombre_completo', 'zona_cobertura']]

    log.info(f"  Procesados: {len(df)} de {total_inicial}")

    # 4. Volcado a Staging y Exportación CSV
    return guardar_datos_curados(df, "val_peritos_validados")

def limpiar_y_transformar_pagos():
    
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

    # 5. Integridad Referencial: Pagos asociados a partes y clientes válidos
    try:
        val_partes = pd.read_sql("SELECT id_parte FROM val_partes_validados", engine_staging)
        val_clientes = pd.read_sql("SELECT id_cliente FROM val_clientes_validados", engine_staging)
        val_receptor = pd.read_sql("SELECT id_asegurado FROM val_polizas_validadas", engine_staging)
        
        df = df[df['id_parte'].isin(val_partes['id_parte']) & df['id_receptor'].isin(val_clientes['id_cliente']) & df['id_receptor'].isin(val_receptor['id_asegurado'])]
    except Exception as e:
        log.warning(f"  ⚠ No se pudo validar la integridad referencial con Partes o Clientes: {e}")

    log.info(f"  Procesados: {len(df)} de {total_inicial}")

    # 6. Volcado a Staging y Exportación CSV
    return guardar_datos_curados(df, "val_pagos_validados")

def limpiar_y_transformar_objetos():
    
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

    # 5. Integridad Referencial: Solo objetos asociados a pólizas validadas
    try:
        val_polizas = pd.read_sql("SELECT id_objeto_asegurado FROM val_polizas_validadas", engine_staging)
        df = df[df['id_objeto'].isin(val_polizas['id_objeto_asegurado'])]
    except Exception as e:
        log.warning(f"  ⚠ No se pudo cruzar con val_polizas_validadas para integridad referencial: {e}")

    log.info(f"  Procesados: {len(df)} de {total_inicial}")

    # 6. Volcado a Staging y Exportación CSV
    return guardar_datos_curados(df, "val_objetos_validados")

def limpiar_y_transformar_agentes():

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

    log.info(f"  Procesados: {len(df)} de {total_inicial}")

    # 9. Guardar tabla validada en staging y Exportación CSV
    return guardar_datos_curados(df, "val_agentes_validados")

def limpiar_y_transformar_partes():

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

    # 8. Integridad Referencial (Cruce con tablas curadas)
    try:
        df_polizas = pd.read_sql("SELECT id_poliza FROM val_polizas_validadas", engine_staging)
        df_clientes = pd.read_sql("SELECT id_cliente FROM val_clientes_validados", engine_staging)
        df_peritos = pd.read_sql("SELECT id_perito FROM val_peritos_validados", engine_staging)
        df_objetos = pd.read_sql("SELECT id_objeto FROM val_objetos_validados", engine_staging)

        # Manejo de nulos que fueron pasados a string en la limpieza de IDs
        nulos_str = ['nan', 'None', '', '<NA>', 'NaN']

        df = df[df['id_poliza'].isin(df_polizas['id_poliza']) | df['id_poliza'].isin(nulos_str)]
        df = df[df['id_denunciante'].isin(df_clientes['id_cliente']) | df['id_denunciante'].isin(nulos_str)]
        df = df[df['id_receptor_pago'].isin(df_clientes['id_cliente']) | df['id_receptor_pago'].isin(nulos_str)]
        df = df[df['id_perito'].isin(df_peritos['id_perito']) | df['id_perito'].isin(nulos_str)]
        df = df[df['id_objeto_asegurado'].isin(df_objetos['id_objeto']) | df['id_objeto_asegurado'].isin(nulos_str)]

    except Exception as e:
        log.warning(f"  ⚠ Advertencia en validación referencial de Partes: {e}")

    log.info(f"  Procesados: {len(df)} de {total_inicial}")

    # 9. Guardar tabla validada en staging y Exportación CSV
    return guardar_datos_curados(df, "val_partes_validados")

def limpiar_y_transformar_garantias():
    
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

    # 4. Integridad Referencial
    try:
        df_polizas = pd.read_sql("SELECT id_poliza FROM val_polizas_validadas", engine_staging)
        nulos_str = ['nan', 'None', '', '<NA>', 'NaN']
        df = df[df['id_poliza'].isin(df_polizas['id_poliza']) | df['id_poliza'].isin(nulos_str)]
    except Exception as e:
        log.warning(f"  ⚠ Advertencia en validación referencial de Garantías: {e}")

    # 4. Verificación de constraints (suma_garantizada > 0)
    invalidos = df[df['suma_garantizada'] <= 0]
    if len(invalidos) > 0:
        log.warning(f"  ⚠ Atención: Hay {len(invalidos)} garantías con suma <= 0. Serán omitidas.")
        df = df[df['suma_garantizada'] > 0]

    log.info(f"  Procesados: {len(df)} de {total_inicial}")
    
    # 5. Guardar tabla validada en Staging
    return guardar_datos_curados(df, "val_garantias_validadas")

def limpiar_y_transformar_indicadores_fraude():

    # 1. Extracción
    df = pd.read_sql("SELECT id_parte, confirmado_fraude FROM indicadores", engine_staging)
    total_inicial = len(df)

    # 2. Conversión necesaria para que 'any' funcione
    # Convertimos a booleano real: True si es '1' o 'true'
    df['confirmado_fraude'] = df['confirmado_fraude'].astype(str).str.strip().str.lower().isin(['true', '1'])

    # 3. Consolidación: Solo id_parte y el flag
    # Si al menos un registro de ese id_parte es True, fraude_flag será True
    df_consolidado = (
        df.groupby('id_parte', as_index=False)
          .agg(fraude_flag=('confirmado_fraude', 'any'))
    )

    # 4. Integridad Referencial: Solo partes de accidentes válidos
    try:
        val_partes = pd.read_sql("SELECT id_parte FROM val_partes_validados", engine_staging)
        df_consolidado = df_consolidado[df_consolidado['id_parte'].isin(val_partes['id_parte'])]
    except Exception as e:
        log.warning(f"  ⚠ No se pudo cruzar con val_partes_validados para integridad referencial: {e}")

    # 5. Log solicitado
    log.info(f"  ✔ Registros procesados correctamente: {len(df_consolidado)} de {total_inicial}")

    # 6. Volcado a MySQL
    return guardar_datos_curados(df_consolidado, "val_indicadores_fraude_validados")