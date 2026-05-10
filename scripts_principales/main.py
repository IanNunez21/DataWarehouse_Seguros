import staging
import transformacion
import logging

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

def ejecutar_etl_inicial():
    log.info("INICIANDO PIPELINE ETL SEGUROS")

    # Lista de tareas para poder iterar o controlar errores individualmente
    tareas = [
        ("Clientes", transformacion.limpiar_y_transformar_clientes),
        ("Pólizas", transformacion.limpiar_y_transformar_polizas),
        ("Auto Insurance", transformacion.limpiar_y_transformar_autoinsurance),
        ("Peritos", transformacion.limpiar_y_transformar_peritos),
        ("Pagos", transformacion.limpiar_y_transformar_pagos),
        ("Objetos", transformacion.limpiar_y_transformar_objetos),
        ("Agentes", transformacion.limpiar_y_transformar_agentes),
        ("Partes", transformacion.limpiar_y_transformar_partes),
        ("Evaluaciones", transformacion.limpiar_y_transformar_evaluaciones),
        ("Garantías", transformacion.limpiar_y_transformar_garantias),
        ("Indicadores Fraude", transformacion.limpiar_y_transformar_indicadores_fraude),
    ]

    for nombre, funcion in tareas:
        try:
            log.info(f"--- Procesando: {nombre} ---")
            funcion() # Ejecuta la transformación
        except Exception as e:
            log.error(f"❌ Error en la transformación de {nombre}: {e}")
            # Opcional: raise e (si quieres que el proceso se detenga ante cualquier error)

    log.info("✅ PIPELINE FINALIZADO")
    
    # Tarea 3: Aquí iría la carga de tabla de hechos
    #log.info("\n--- Próximo paso: Carga de Dimensiones con Surrogate Keys ---")

if __name__ == "__main__":
    ejecutar_etl_inicial() 