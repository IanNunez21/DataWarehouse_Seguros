import staging
import transformacion
import logging

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

def ejecutar_etl_inicial():
    log.info("INICIANDO PIPELINE ETL SEGUROS")
    # Tarea 1: Carga cruda
    staging.cargar_staging_area()
    
    # Tarea 2: Limpieza y Transformación
    # Guardamos el resultado en memoria para el siguiente paso (Carga de Dimensiones)
    df_clientes_limpios = transformacion.limpiar_y_transformar_clientes()
    
    df_polizas_limpias = transformacion.limpiar_y_transformar_polizas()

    df_auto = transformacion.limpiar_y_transformar_autoinsurance()

    df_evaluaciones = transformacion.limpiar_y_transformar_evaluaciones()

    # Tarea 3: Aquí iría la carga de tabla de hechos
    #log.info("\n--- Próximo paso: Carga de Dimensiones con Surrogate Keys ---")

if __name__ == "__main__":
    # WARNING: COSAS COMENTADAS XD
    # ejecutar_etl_inicial() <-- Acá solo tendría que ejecutarse esto de seguido, no las lineas de abajo
    # transformacion.limpiar_y_transformar_clientes()
    # transformacion.limpiar_y_transformar_polizas()
    # # transformacion.limpiar_y_transformar_autoinsurance()
    transformacion.limpiar_y_transformar_evaluaciones()