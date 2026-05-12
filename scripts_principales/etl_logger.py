"""
etl_logger.py
-------------
Módulo de metadatos del ETL.
Registra en la tabla `etl_log` de dw_staging:
  - fecha y hora de inicio/fin de cada paso
  - volumen de filas procesadas
  - estado final (OK / ERROR)
  - mensaje de error si corresponde
"""

import logging
from datetime import datetime
from sqlalchemy import text
from config import engine_staging

log = logging.getLogger(__name__)

# ── DDL: crear la tabla si no existe ─────────────────────────────────────────
_DDL = """
CREATE TABLE IF NOT EXISTS etl_log (
    id             INT AUTO_INCREMENT PRIMARY KEY,
    fecha_inicio   DATETIME     NOT NULL,
    fecha_fin      DATETIME,
    paso           VARCHAR(100) NOT NULL,
    filas_cargadas INT          DEFAULT 0,
    estado         VARCHAR(10)  NOT NULL DEFAULT 'OK',
    mensaje        TEXT
);
"""

def asegurar_tabla_log():
    """Crea la tabla etl_log en dw_staging si todavía no existe."""
    with engine_staging.connect() as conn:
        conn.execute(text(_DDL))
        conn.commit()


def registrar(paso: str, fecha_inicio: datetime,
              fecha_fin: datetime = None,
              filas: int = 0,
              estado: str = "OK",
              mensaje: str = None):
    """
    Inserta un registro de ejecución en etl_log.

    Parámetros
    ----------
    paso         : nombre del paso (ej: 'dim_agente', 'fact_poliza', 'Staging')
    fecha_inicio : datetime en que comenzó el paso
    fecha_fin    : datetime en que terminó (por defecto: ahora)
    filas        : cantidad de filas procesadas/cargadas
    estado       : 'OK' o 'ERROR'
    mensaje      : texto libre (descripción o traza del error)
    """
    fecha_fin = fecha_fin or datetime.now()
    sql = text("""
        INSERT INTO etl_log (fecha_inicio, fecha_fin, paso, filas_cargadas, estado, mensaje)
        VALUES (:fi, :ff, :paso, :filas, :estado, :msg)
    """)
    try:
        with engine_staging.connect() as conn:
            conn.execute(sql, {
                "fi":    fecha_inicio,
                "ff":    fecha_fin,
                "paso":  paso,
                "filas": filas,
                "estado": estado,
                "msg":   mensaje,
            })
            conn.commit()
    except Exception as e:
        # El logger no debe romper el ETL si hay un problema de conexión
        log.warning(f"  ⚠ No se pudo escribir en etl_log: {e}")
