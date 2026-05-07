import staging
import transformacion
import logging

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

def ejecutar_etl_inicial():
    log.info("INICIANDO PIPELINE ETL SEGUROS")
    # Tarea 1: Carga cruda
    #staging.cargar_staging_area() ESTO ESTA COMENTADO PARA NO CARGAR STAGING DE NUEVO 
    # CADA RATO NOMAS, DESPUES SACARLO COMO COMENTARIO
    
    # Tarea 2: Limpieza y Transformación
    df_clientes_limpios = transformacion.limpiar_y_transformar_clientes()
    df_polizas_limpias = transformacion.limpiar_y_transformar_polizas()
    df_auto = transformacion.limpiar_y_transformar_autoinsurance()
    df_evaluaciones = transformacion.limpiar_y_transformar_evaluaciones()
    df_perito = transformacion.limpiar_y_transformar_peritos()
    df_pagos = transformacion.limpiar_y_transformar_pagos()
    df_objetos = transformacion.limpiar_y_transformar_objetos()
    df_agentes = transformacion.limpiar_y_transformar_agentes()
    df_partes = transformacion.limpiar_y_transformar_partes()
    df_garantias = transformacion.limpiar_y_transformar_garantias()
    
    # Tarea 3: Aquí iría la carga de tabla de hechos
    #log.info("\n--- Próximo paso: Carga de Dimensiones con Surrogate Keys ---")

if __name__ == "__main__":
    ejecutar_etl_inicial() 