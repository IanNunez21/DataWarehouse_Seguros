import staging
import transformacion
import carga_dimensiones
import logging
import carga_hechos

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

def ejecutar_etl_inicial():
    log.info("INICIANDO PIPELINE ETL SEGUROS")

    # ── PASO 1: Staging (carga cruda) ────────────────────────────────────────
    staging.cargar_staging_area()

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
          try:
              log.info(f"--- Procesando: {nombre} ---")
              funcion()
          except Exception as e:
              log.error(f"❌ Error en la transformación de {nombre}: {e}")

    # ── PASO 3: Carga al DW (dimensiones) ────────────────────────────────────
    tareas_dimensiones = [
        ("dim_agente", carga_dimensiones.cargar_dim_agente),
        ("dim_tiempo", carga_dimensiones.cargar_dim_tiempo),
        ("dim_perito", carga_dimensiones.cargar_dim_perito),
        ("dim_objeto", carga_dimensiones.cargar_dim_objeto),
        ("dim_tipo_seguro", carga_dimensiones.cargar_dim_tipo_seguro),
        ("dim_tiposiniestro", carga_dimensiones.cargar_dim_tiposiniestro),
        ("dim_ubicacion", carga_dimensiones.cargar_dim_ubicacion),
        ("dim_personas", carga_dimensiones.cargar_dim_personas),
    ]
    tareas_hechos = [
        ("fact_poliza", carga_hechos.cargar_fact_poliza),
        ("fact_siniestro", carga_hechos.cargar_fact_siniestro)
    ]


    for nombre, funcion in tareas_dimensiones:
         try:
             log.info(f"--- Cargando: {nombre} ---")
             funcion()
         except Exception as e:
             log.error(f"❌ Error cargando {nombre}: {e}")
    
    # ── PASO 4: Carga al DW (hechos) ────────────────────────────────────────
    for nombre, funcion in tareas_hechos:
        try:
            log.info(f"--- Cargando: {nombre} ---")
            funcion()
        except Exception as e:
            log.error(f"❌ Error cargando {nombre}: {e}")

    log.info("✅ PIPELINE FINALIZADO")

if __name__ == "__main__":
    ejecutar_etl_inicial()