-- ==============================================================================
-- Sub-objetivo 1 — Analizar rentabilidad por tipo de seguro
-- ==============================================================================

-- Q1: Volumen total de ventas (primas) desglosado por tipo de seguro y zona geográfica en el último año.
SELECT 
    t.categoria_plan, 
    u.provincia, 
    SUM(p.monto_prima) AS volumen_ventas
FROM Fact_Poliza p
JOIN Dim_Tipo_Seguro t ON p.id_tipo_seguro_sk = t.id_tipo_seguro_sk
JOIN Dim_Ubicacion u ON p.id_ubicacion_sk = u.id_ubicacion_sk
JOIN Dim_Tiempo tiempo ON p.id_fecha_venta_sk = tiempo.id_tiempo_sk
-- Opcional: WHERE tiempo.Anio = 2023 
GROUP BY t.categoria_plan, u.provincia
ORDER BY volumen_ventas DESC;


-- Q2 y Q3: Tipos de seguros que aportan mayores beneficios netos y los que generan pérdidas netas.
SELECT 
    t.categoria_plan,
    COALESCE(p.total_primas, 0) AS ingresos_primas,
    COALESCE(s.total_siniestros, 0) AS egresos_siniestros,
    (COALESCE(p.total_primas, 0) - COALESCE(s.total_siniestros, 0)) AS beneficio_neto
FROM Dim_Tipo_Seguro t
LEFT JOIN (
    SELECT id_tipo_seguro_sk, SUM(monto_prima) AS total_primas
    FROM Fact_Poliza
    GROUP BY id_tipo_seguro_sk
) p ON t.id_tipo_seguro_sk = p.id_tipo_seguro_sk
LEFT JOIN (
    SELECT p_inner.id_tipo_seguro_sk, SUM(s_inner.monto_pagado) AS total_siniestros
    FROM Fact_Siniestro s_inner
    JOIN Fact_Poliza p_inner ON s_inner.id_poliza_sk = p_inner.id_poliza_sk
    GROUP BY p_inner.id_tipo_seguro_sk
) s ON t.id_tipo_seguro_sk = s.id_tipo_seguro_sk
ORDER BY beneficio_neto DESC;


-- Q4: Categorías de plan con menor tasa de siniestralidad (Cantidad siniestros / Cantidad pólizas).
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
ORDER BY tasa_siniestralidad ASC;


-- ==============================================================================
-- Sub-objetivo 2 — Evaluar riesgo por perfil de cliente
-- ==============================================================================

-- Q1: Segmento demográfico con mayor cantidad de seguros Premium.
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
-- Sub-objetivo 3 — Analizar frecuencia y costo de siniestros por región
-- ==============================================================================

-- Q1: Los 5 tipos de siniestros más frecuentes por mes y en qué zonas geográficas se concentran.
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


-- Q2: Zonas geográficas con mayor promedio de siniestros por tipo de accidente.
-- (Compara la cantidad de siniestros de cada provincia contra el promedio nacional para ese accidente)
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
-- Sub-objetivo 4 — Desempeño de agentes de venta y peritos
-- ==============================================================================

-- Q1: Volumen de pólizas emitidas por agente y prima promedio por póliza.
SELECT 
    a.nombre_agente,
    COUNT(p.id_poliza_sk) AS volumen_polizas,
    AVG(p.monto_prima) AS prima_promedio
FROM Fact_Poliza p
JOIN Dim_Agente a ON p.id_agente_sk = a.id_agente_sk
JOIN Dim_Tiempo t ON p.id_fecha_venta_sk = t.id_tiempo_sk
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


-- Q4: Porcentaje del monto reclamado ajustado a la baja por peritos según el tipo de siniestro.
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
