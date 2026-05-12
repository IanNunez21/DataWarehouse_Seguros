import staging
import transformacion
import carga_dimensiones
import carga_hechos
import etl_logger
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

def ejecutar_etl_inicial():
    log.info("INICIANDO PIPELINE ETL SEGUROS")

    # Crear tabla de metadatos si no existe
    etl_logger.asegurar_tabla_log()
    inicio_total = datetime.now()

    # ── PASO 1: Staging (carga cruda) ────────────────────────────────────────
    t0 = datetime.now()
    try:
        staging.cargar_staging_area()
        etl_logger.registrar("Staging", t0, filas=0, estado="OK",
                             mensaje="Carga cruda de CSVs a dw_staging completada")
    except Exception as e:
        etl_logger.registrar("Staging", t0, estado="ERROR", mensaje=str(e))
        log.error(f"❌ Error en Staging: {e}")

    # ── PASO 2: Transformación y validación ───────────────────────────────────
    tareas_transformacion = [
         ("Clientes",           transformacion.limpiar_y_transformar_clientes),
         ("Agentes",            transformacion.limpiar_y_transformar_agentes),
         ("Pólizas",            transformacion.limpiar_y_transformar_polizas),
         ("Peritos",            transformacion.limpiar_y_transformar_peritos),
         ("Objetos",            transformacion.limpiar_y_transformar_objetos),
         ("Partes",             transformacion.limpiar_y_transformar_partes),
         ("Evaluaciones",       transformacion.limpiar_y_transformar_evaluaciones),
         ("Pagos",              transformacion.limpiar_y_transformar_pagos),
         ("Garantías",          transformacion.limpiar_y_transformar_garantias),
         ("Indicadores Fraude", transformacion.limpiar_y_transformar_indicadores_fraude),
     ]

    for nombre, funcion in tareas_transformacion:
        t0 = datetime.now()
        try:
            log.info(f"--- Procesando: {nombre} ---")
            funcion()
            etl_logger.registrar(f"Transformación: {nombre}", t0, estado="OK")
        except Exception as e:
            etl_logger.registrar(f"Transformación: {nombre}", t0, estado="ERROR", mensaje=str(e))
            log.error(f"❌ Error en la transformación de {nombre}: {e}")

    # Crear índices en staging para acelerar los lookups de carga al DW
    staging.crear_indices_staging()

    # ── PASO 3: Carga al DW (dimensiones) ────────────────────────────────────
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
    tareas_hechos = [
        ("fact_poliza",    carga_hechos.cargar_fact_poliza),
        ("fact_siniestro", carga_hechos.cargar_fact_siniestro)
    ]

    for nombre, funcion in tareas_dimensiones:
        t0 = datetime.now()
        try:
            log.info(f"--- Cargando: {nombre} ---")
            funcion()
            etl_logger.registrar(nombre, t0, estado="OK")
        except Exception as e:
            etl_logger.registrar(nombre, t0, estado="ERROR", mensaje=str(e))
            log.error(f"❌ Error cargando {nombre}: {e}")

    carga_dimensiones.asegurar_registros_desconocidos()

    # ── PASO 4: Carga al DW (hechos) ─────────────────────────────────────────
    for nombre, funcion in tareas_hechos:
        t0 = datetime.now()
        try:
            log.info(f"--- Cargando: {nombre} ---")
            funcion()
            etl_logger.registrar(nombre, t0, estado="OK")
        except Exception as e:
            etl_logger.registrar(nombre, t0, estado="ERROR", mensaje=str(e))
            log.error(f"❌ Error cargando {nombre}: {e}")

    # ── Registro final del pipeline completo ─────────────────────────────────
    etl_logger.registrar("PIPELINE COMPLETO", inicio_total, estado="OK",
                         mensaje="Todos los pasos finalizaron correctamente")
    log.info("✅ PIPELINE FINALIZADO")

if __name__ == "__main__":
    ejecutar_etl_inicial()