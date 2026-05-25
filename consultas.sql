-- ==============================================================================
-- Sub-objetivo 1 — Analizar rentabilidad por tipo de seguro
-- ==============================================================================

-- Q1: Volumen total de ventas (primas) desglosado por tipo de seguro y zona geografica.
SELECT 
    t.categoria_plan, 
    u.provincia, 
    SUM(p.monto_prima) AS volumen_ventas
FROM Fact_Poliza p
JOIN Dim_Tipo_Seguro t ON p.id_tipo_seguro_sk = t.id_tipo_seguro_sk
JOIN Dim_Ubicacion u ON p.id_ubicacion_sk = u.id_ubicacion_sk
JOIN Dim_Tiempo tiempo ON p.id_vigencia_desde_sk = tiempo.id_tiempo_sk
-- Opcional: WHERE tiempo.Anio = 2023 
GROUP BY t.categoria_plan, u.provincia
ORDER BY volumen_ventas DESC;


-- DIAGNOSTICO: rango real de fechas en Fact_Poliza
-- Ejecutar para saber que anios tiene el dataset cargado.
SELECT
    MIN(dt.Anio)           AS anio_min_vigencia_desde,
    MAX(dt.Anio)           AS anio_max_vigencia_desde,
    MIN(dt2.Anio)          AS anio_min_vigencia_hasta,
    MAX(dt2.Anio)          AS anio_max_vigencia_hasta,
    COUNT(fp.id_poliza_sk) AS total_polizas
FROM Fact_Poliza fp
JOIN      Dim_Tiempo dt  ON fp.id_vigencia_desde_sk = dt.id_tiempo_sk
LEFT JOIN Dim_Tiempo dt2 ON fp.id_vigencia_hasta_sk = dt2.id_tiempo_sk;

-- Distribucion de polizas por anio de inicio
SELECT
    dt.Anio                AS anio_vigencia_desde,
    COUNT(fp.id_poliza_sk) AS cantidad_polizas,
    MIN(dt2.Anio)          AS vigencia_hasta_min,
    MAX(dt2.Anio)          AS vigencia_hasta_max
FROM Fact_Poliza fp
JOIN      Dim_Tiempo dt  ON fp.id_vigencia_desde_sk = dt.id_tiempo_sk
LEFT JOIN Dim_Tiempo dt2 ON fp.id_vigencia_hasta_sk = dt2.id_tiempo_sk
GROUP BY dt.Anio
ORDER BY dt.Anio;


-- Q2 y Q3: Rentabilidad neta por tipo de seguro — comparativo 2023 / 2024 / 2025
-- Resultado: una fila por (categoria_plan, anio).
-- Ingresos  = meses reales de vigencia dentro de cada anio x cuota mensual.
-- Egresos   = suma de monto_pagado de siniestros abiertos ese anio.
-- Para agregar/quitar anios: modificar los dos CROSS JOIN de la tabla de periodos.
SELECT
    t.categoria_plan,
    y.anio,
    COALESCE(p.total_primas,     0) AS ingresos_primas,
    COALESCE(s.total_siniestros, 0) AS egresos_siniestros,
    (COALESCE(p.total_primas, 0) - COALESCE(s.total_siniestros, 0)) AS beneficio_neto
FROM Dim_Tipo_Seguro t

-- Tabla de periodos a analizar
CROSS JOIN (
    SELECT 2023 AS anio, '2023-01-01' AS periodo_inicio, '2024-01-01' AS periodo_fin
    UNION ALL SELECT 2024, '2024-01-01', '2025-01-01'
    UNION ALL SELECT 2025, '2025-01-01', '2026-01-01'
) y

-- INGRESOS: meses reales de solapamiento (vigencia de poliza) x (periodo del anio)
LEFT JOIN (
    SELECT
        fp.id_tipo_seguro_sk,
        y2.anio,
        SUM(
            fp.monto_prima *
            GREATEST(
                TIMESTAMPDIFF(
                    MONTH,
                    GREATEST(
                        STR_TO_DATE(CAST(dt_desde.id_tiempo AS CHAR), '%Y%m%d'),
                        y2.periodo_inicio
                    ),
                    LEAST(
                        IFNULL(
                            STR_TO_DATE(CAST(dt_hasta.id_tiempo AS CHAR), '%Y%m%d'),
                            '9999-12-31'
                        ),
                        y2.periodo_fin
                    )
                ),
                0
            )
        ) AS total_primas
    FROM Fact_Poliza fp
    JOIN      Dim_Tiempo dt_desde ON fp.id_vigencia_desde_sk = dt_desde.id_tiempo_sk
    LEFT JOIN Dim_Tiempo dt_hasta ON fp.id_vigencia_hasta_sk = dt_hasta.id_tiempo_sk
    CROSS JOIN (
        SELECT 2023 AS anio, '2023-01-01' AS periodo_inicio, '2024-01-01' AS periodo_fin
        UNION ALL SELECT 2024, '2024-01-01', '2025-01-01'
        UNION ALL SELECT 2025, '2025-01-01', '2026-01-01'
    ) y2
    WHERE
        STR_TO_DATE(CAST(dt_desde.id_tiempo AS CHAR), '%Y%m%d') < y2.periodo_fin
        AND IFNULL(
                STR_TO_DATE(CAST(dt_hasta.id_tiempo AS CHAR), '%Y%m%d'),
                '9999-12-31'
            ) > y2.periodo_inicio
    GROUP BY fp.id_tipo_seguro_sk, y2.anio
) p ON t.id_tipo_seguro_sk = p.id_tipo_seguro_sk
     AND y.anio = p.anio

-- EGRESOS: siniestros abiertos en cada uno de los anios
LEFT JOIN (
    SELECT
        fp.id_tipo_seguro_sk,
        dt.Anio,
        SUM(fs.monto_pagado) AS total_siniestros
    FROM Fact_Siniestro fs
    JOIN Fact_Poliza fp ON fs.id_poliza_sk    = fp.id_poliza_sk
    JOIN Dim_Tiempo  dt ON fs.FechaAperturaKey = dt.id_tiempo_sk
    WHERE dt.Anio IN (2023, 2024, 2025)
    GROUP BY fp.id_tipo_seguro_sk, dt.Anio
) s ON t.id_tipo_seguro_sk = s.id_tipo_seguro_sk
     AND y.anio = s.Anio

ORDER BY y.anio ASC, beneficio_neto DESC;


-- Q4: Categorias de plan con mayor tasa de siniestralidad (Cantidad siniestros / Cantidad polizas).
SELECT 
    t.categoria_plan,
    COALESCE(s.cant_siniestros, 0) AS cantidad_siniestros,
    COALESCE(p.cant_polizas, 0) AS cantidad_polizas,
    (COALESCE(s.cant_siniestros, 0) * 1.0 / NULLIF(COALESCE(p.cant_polizas, 0), 0)) AS tasa_siniestralidad
FROM Dim_Tipo_Seguro t
LEFT JOIN (
    SELECT id_tipo_seguro_sk, COUNT(id_poliza_sk) AS cant_polizas
    FROM Fact_Poliza
    GROUP BY id_tipo_seguro_sk
) p ON t.id_tipo_seguro_sk = p.id_tipo_seguro_sk
LEFT JOIN (
    SELECT p_inner.id_tipo_seguro_sk, COUNT(s_inner.SiniestroKey) AS cant_siniestros
    FROM Fact_Siniestro s_inner
    JOIN Fact_Poliza p_inner ON s_inner.id_poliza_sk = p_inner.id_poliza_sk
    GROUP BY p_inner.id_tipo_seguro_sk
) s ON t.id_tipo_seguro_sk = s.id_tipo_seguro_sk
ORDER BY tasa_siniestralidad DESC;


-- ==============================================================================
-- Sub-objetivo 2 — Evaluar riesgo por perfil de cliente
-- ==============================================================================

-- Q1: Segmento demografico con mayor cantidad de seguros Premium.
SELECT 
    pe.ocupacion,
    pe.segmento_persona,
    u.provincia,
    COUNT(p.id_poliza_sk) AS cantidad_seguros_premium
FROM Fact_Poliza p
JOIN Dim_Personas pe ON p.id_persona_tomador_sk = pe.id_persona_sk
JOIN Dim_Ubicacion u ON pe.id_ubicacion_fk = u.id_ubicacion_sk
JOIN Dim_Tipo_Seguro t ON p.id_tipo_seguro_sk = t.id_tipo_seguro_sk
WHERE t.categoria_plan = 'Premium'
GROUP BY pe.ocupacion, pe.segmento_persona, u.provincia
ORDER BY cantidad_seguros_premium DESC
LIMIT 10;


-- Q2: Segmentos de clientes que concentran la mayor tasa de siniestralidad.
SELECT 
    ps.ocupacion,
    ps.segmento_persona,
    ps.provincia,
    COALESCE(ss.cant_siniestros, 0) AS siniestros,
    ps.cant_polizas AS polizas,
    (COALESCE(ss.cant_siniestros, 0) * 1.0 / ps.cant_polizas) AS tasa_siniestralidad
FROM (
    SELECT pe.ocupacion, pe.segmento_persona, u.provincia, COUNT(p.id_poliza_sk) AS cant_polizas
    FROM Fact_Poliza p
    JOIN Dim_Personas pe ON p.id_persona_tomador_sk = pe.id_persona_sk
    JOIN Dim_Ubicacion u ON pe.id_ubicacion_fk = u.id_ubicacion_sk
    GROUP BY pe.ocupacion, pe.segmento_persona, u.provincia
) ps
LEFT JOIN (
    SELECT pe.ocupacion, pe.segmento_persona, u.provincia, COUNT(s.SiniestroKey) AS cant_siniestros
    FROM Fact_Siniestro s
    JOIN Fact_Poliza p_inner ON s.id_poliza_sk = p_inner.id_poliza_sk
    JOIN Dim_Personas pe ON p_inner.id_persona_tomador_sk = pe.id_persona_sk
    JOIN Dim_Ubicacion u ON pe.id_ubicacion_fk = u.id_ubicacion_sk
    GROUP BY pe.ocupacion, pe.segmento_persona, u.provincia
) ss 
  ON ps.ocupacion = ss.ocupacion 
  AND ps.segmento_persona = ss.segmento_persona 
  AND ps.provincia = ss.provincia
ORDER BY tasa_siniestralidad DESC
LIMIT 10;


-- ==============================================================================
-- Sub-objetivo 3 — Analizar frecuencia y costo de siniestros por region
-- ==============================================================================

-- Q1: Los 5 tipos de siniestros mas frecuentes por mes y en que zonas geograficas se concentran.
SELECT 
    t.Anio,
    t.Mes,
    ts.Nombre_Siniestro,
    u.provincia,
    COUNT(s.SiniestroKey) AS frecuencia
FROM Fact_Siniestro s
JOIN Dim_Tiempo t ON s.FechaAperturaKey = t.id_tiempo_sk
JOIN Dim_TipoSiniestro ts ON s.TipoSiniestroKey = ts.id_tipo_siniestro_sk
JOIN Dim_Ubicacion u ON s.UbicacionKey = u.id_ubicacion_sk
GROUP BY t.Anio, t.Mes, ts.Nombre_Siniestro, u.provincia
ORDER BY t.Anio DESC, t.Mes DESC, frecuencia DESC;


-- Q2: Zonas geograficas con mayor promedio de siniestros por tipo de accidente.
SELECT 
    u.provincia,
    ts.Nombre_Siniestro,
    COUNT(s.SiniestroKey) AS cantidad_siniestros_provincia,
    nacional.promedio_nacional
FROM Fact_Siniestro s
JOIN Dim_TipoSiniestro ts ON s.TipoSiniestroKey = ts.id_tipo_siniestro_sk
JOIN Dim_Ubicacion u ON s.UbicacionKey = u.id_ubicacion_sk
JOIN (
    SELECT 
        TipoSiniestroKey,
        (COUNT(SiniestroKey) * 1.0 / (SELECT COUNT(DISTINCT provincia) FROM Dim_Ubicacion)) AS promedio_nacional
    FROM Fact_Siniestro
    GROUP BY TipoSiniestroKey
) nacional ON s.TipoSiniestroKey = nacional.TipoSiniestroKey
GROUP BY u.provincia, ts.Nombre_Siniestro, nacional.promedio_nacional
ORDER BY ts.Nombre_Siniestro, cantidad_siniestros_provincia DESC;


-- ==============================================================================
-- Sub-objetivo 4 — Desempeno de agentes de venta y peritos
-- ==============================================================================

-- Q1: Volumen de polizas emitidas por agente y prima promedio por poliza.
SELECT 
    a.nombre_agente,
    COUNT(p.id_poliza_sk) AS volumen_polizas,
    AVG(p.monto_prima) AS prima_promedio
FROM Fact_Poliza p
JOIN Dim_Agente a ON p.id_agente_sk = a.id_agente_sk
JOIN Dim_Tiempo t ON p.id_vigencia_desde_sk = t.id_tiempo_sk
-- Opcional: WHERE t.Anio = 2023
GROUP BY a.nombre_agente
ORDER BY volumen_polizas DESC
LIMIT 10;


-- Q2: Peritos con mayor cantidad de siniestros con fraude confirmado y su porcentaje sobre el total.
SELECT 
    dp.Nombre_Perito,
    SUM(s.Fraude_flag) AS fraudes_confirmados,
    COUNT(s.SiniestroKey) AS total_evaluaciones,
    (SUM(s.Fraude_flag) * 100.0 / COUNT(s.SiniestroKey)) AS porcentaje_fraude
FROM Fact_Siniestro s
JOIN Dim_Perito dp ON s.PeritoKey = dp.id_perito_sk
GROUP BY dp.Nombre_Perito
ORDER BY fraudes_confirmados DESC, porcentaje_fraude DESC;


-- Q3: Provincias con mayor tasa de fraudes y monto reclamado promedio en esos fraudes.
SELECT 
    u.provincia,
    SUM(s.Fraude_flag) AS cantidad_fraudes,
    COUNT(s.SiniestroKey) AS total_siniestros,
    (SUM(s.Fraude_flag) * 100.0 / COUNT(s.SiniestroKey)) AS tasa_fraude,
    AVG(CASE WHEN s.Fraude_flag = 1 THEN s.monto_declarado ELSE NULL END) AS monto_reclamado_promedio_fraude
FROM Fact_Siniestro s
JOIN Dim_Ubicacion u ON s.UbicacionKey = u.id_ubicacion_sk
GROUP BY u.provincia
ORDER BY tasa_fraude DESC;


-- Q4: Porcentaje del monto reclamado ajustado a la baja por peritos segun el tipo de siniestro.
SELECT 
    ts.Nombre_Siniestro,
    SUM(s.monto_declarado) AS total_reclamado,
    SUM(s.monto_evaluado) AS total_evaluado,
    SUM(s.monto_declarado - s.monto_evaluado) AS total_ajustado,
    (SUM(s.monto_declarado - s.monto_evaluado) * 100.0 / NULLIF(SUM(s.monto_declarado), 0)) AS porcentaje_ajuste
FROM Fact_Siniestro s
JOIN Dim_TipoSiniestro ts ON s.TipoSiniestroKey = ts.id_tipo_siniestro_sk
GROUP BY ts.Nombre_Siniestro
ORDER BY porcentaje_ajuste DESC;
