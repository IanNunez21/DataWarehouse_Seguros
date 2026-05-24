-- ==============================================================================
-- MIGRACIÓN: Fact_Poliza — fechas de vigencia
-- Ejecutar UNA SOLA VEZ contra data_warehouse_seguros (base ya existente).
-- Renombra id_fecha_venta_sk → id_vigencia_desde_sk
-- Agrega id_vigencia_hasta_sk (nullable)
-- ==============================================================================

USE data_warehouse_seguros;

-- 1. Eliminar el índice viejo (si existe)
DROP INDEX IF EXISTS idx_poliza_fecha_venta ON Fact_Poliza;

-- 2. Eliminar la FK vieja
ALTER TABLE Fact_Poliza
    DROP FOREIGN KEY fk_fecha_venta;

-- 3. Renombrar la columna (MySQL 8+: RENAME COLUMN)
ALTER TABLE Fact_Poliza
    RENAME COLUMN id_fecha_venta_sk TO id_vigencia_desde_sk;

-- 4. Agregar la nueva columna vigencia_hasta (nullable: NULL = póliza activa)
ALTER TABLE Fact_Poliza
    ADD COLUMN id_vigencia_hasta_sk INT NULL
        AFTER id_vigencia_desde_sk;

-- 5. Volver a crear las FKs con los nombres nuevos
ALTER TABLE Fact_Poliza
    ADD CONSTRAINT fk_vigencia_desde
        FOREIGN KEY (id_vigencia_desde_sk) REFERENCES Dim_Tiempo(id_tiempo_sk),
    ADD CONSTRAINT fk_vigencia_hasta
        FOREIGN KEY (id_vigencia_hasta_sk) REFERENCES Dim_Tiempo(id_tiempo_sk);

-- 6. Crear los índices para las nuevas columnas de fecha
CREATE INDEX idx_poliza_vig_desde ON Fact_Poliza(id_vigencia_desde_sk);
CREATE INDEX idx_poliza_vig_hasta ON Fact_Poliza(id_vigencia_hasta_sk);

-- Verificación final
DESCRIBE Fact_Poliza;
