import staging
import transformacion
import carga_dimensiones
import carga_hechos
import etl_logger
import logging
from datetime import datetime

# Formato profesional: Nivel de log alineado | Mensaje
logging.basicConfig(level=logging.INFO, format="%(levelname)-8s | %(message)s")
log = logging.getLogger(__name__)

def ejecutar_etl_inicial():
    log.info("\nPIPELINE ETL SEGUROS - INICIO\n")
    log.info("PASO 1: STAGING AREA\n")
    etl_logger.asegurar_tabla_log()
    inicio_total = datetime.now()

    t0 = datetime.now()
    try:
        staging.cargar_staging_area()
        etl_logger.registrar("Staging", t0, filas=0, estado="OK", mensaje="Carga cruda completada")
    except Exception as e:
        etl_logger.registrar("Staging", t0, estado="ERROR", mensaje=str(e))
        log.error(f"Error en Staging: {e}")

    log.info("\nPASO 2: TRANSFORMACION\n")
    tareas_transformacion = [
        ("Clientes",           transformacion.limpiar_y_transformar_clientes),
        ("Agentes",            transformacion.limpiar_y_transformar_agentes),
        ("Polizas",            transformacion.limpiar_y_transformar_polizas),
        ("Peritos",            transformacion.limpiar_y_transformar_peritos),
        ("Objetos",            transformacion.limpiar_y_transformar_objetos),
        ("Partes",             transformacion.limpiar_y_transformar_partes),
        ("Evaluaciones",       transformacion.limpiar_y_transformar_evaluaciones),
        ("Pagos",              transformacion.limpiar_y_transformar_pagos),
        ("Garantias",          transformacion.limpiar_y_transformar_garantias),
        ("Indicadores Fraude", transformacion.limpiar_y_transformar_indicadores_fraude),
    ]

    for nombre, funcion in tareas_transformacion:
        t0 = datetime.now()
        try:
            log.info(f"Procesando transformacion: {nombre}")
            funcion()
            etl_logger.registrar(f"Transformacion: {nombre}", t0, estado="OK")
        except Exception as e:
            etl_logger.registrar(f"Transformacion: {nombre}", t0, estado="ERROR", mensaje=str(e))
            log.error(f"Error en transformacion {nombre}: {e}")

    staging.crear_indices_staging()

    log.info("\nPASO 3: CARGA DE DIMENSIONES\n")
    tareas_dimensiones = [
        ("dim_agente",       carga_dimensiones.cargar_dim_agente),
        ("dim_tiempo",       carga_dimensiones.cargar_dim_tiempo),
        ("dim_perito",       carga_dimensiones.cargar_dim_perito),
        ("dim_objeto",       carga_dimensiones.cargar_dim_objeto),
        ("dim_tipo_seguro",  carga_dimensiones.cargar_dim_tipo_seguro),
        ("dim_tiposiniestro",carga_dimensiones.cargar_dim_tiposiniestro),
        ("dim_ubicacion",    carga_dimensiones.cargar_dim_ubicacion),
        ("dim_personas",     carga_dimensiones.cargar_dim_personas),
    ]
    
    log.info('CARGA DE TABLAS DE DIMENSIONES\n')
    for nombre, funcion in tareas_dimensiones:
        t0 = datetime.now()
        try:
            log.info(f"Cargando dimension: {nombre}")
            funcion()
            etl_logger.registrar(nombre, t0, estado="OK")
        except Exception as e:
            etl_logger.registrar(nombre, t0, estado="ERROR", mensaje=str(e))
            log.error(f"Error en dimension {nombre}: {e}")

    carga_dimensiones.asegurar_registros_desconocidos()

    log.info("\nPASO 4: CARGA DE TABLAS DE HECHOS\n")
    tareas_hechos = [
        ("fact_poliza",    carga_hechos.cargar_fact_poliza),
        ("fact_siniestro", carga_hechos.cargar_fact_siniestro)
    ]

    for nombre, funcion in tareas_hechos:
        t0 = datetime.now()
        try:
            log.info(f"Cargando hechos: {nombre}")
            funcion()
            etl_logger.registrar(nombre, t0, estado="OK")
        except Exception as e:
            etl_logger.registrar(nombre, t0, estado="ERROR", mensaje=str(e))
            log.error(f"Error en hechos {nombre}: {e}")

    etl_logger.registrar("PIPELINE COMPLETO", inicio_total, estado="OK", mensaje="Exito")
    log.info("\nPIPELINE ETL SEGUROS - FINALIZADO\n")

if __name__ == "__main__":
    ejecutar_etl_inicial()
