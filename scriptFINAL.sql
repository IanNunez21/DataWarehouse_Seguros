CREATE DATABASE IF NOT EXISTS data_warehouse_seguros;
USE data_warehouse_seguros;


CREATE TABLE Dim_Ubicacion (
    id_ubicacion_sk INT AUTO_INCREMENT PRIMARY KEY,
    pais VARCHAR(100),
    provincia VARCHAR(100),
    ciudad VARCHAR(100),
    barrio VARCHAR(100)
);


CREATE TABLE Dim_Personas (
    id_persona_sk INT AUTO_INCREMENT PRIMARY KEY,
    id_ubicacion_fk INT,
    ocupacion VARCHAR (40),
    segmento_persona ENUM('Joven', 'Adulto', 'Mayor') NOT NULL,
    fecha_desde DATE NOT NULL, -- Cuándo empieza a ser válida esta versión
    fecha_hasta DATE DEFAULT NULL, -- Cuándo deja de ser válida (NULL = actual)
    es_actual BOOLEAN DEFAULT 1, -- Indicador rápido (1 para la versión vigente, 0 para histórico)
    es_tercero BOOLEAN DEFAULT 0, -- Indicador rápido (0 si es cliente, 1 si es tercero)
    CONSTRAINT fk_persona_ubicacion
        FOREIGN KEY (id_ubicacion_fk) REFERENCES Dim_Ubicacion(id_ubicacion_sk)
);


CREATE TABLE Dim_Objeto (
    id_objeto_sk INT AUTO_INCREMENT PRIMARY KEY,
    tipo_objeto VARCHAR(50),
    valor_objeto DECIMAL(12,2)
);


CREATE TABLE Dim_Agente (
    id_agente_sk INT AUTO_INCREMENT PRIMARY KEY,
    nombre_agente VARCHAR(100)
);


CREATE TABLE Dim_Tipo_Seguro (
    id_tipo_seguro_sk INT AUTO_INCREMENT PRIMARY KEY,
    categoria_plan ENUM('Basico', 'Estandar', 'Premium') NOT NULL
);


CREATE TABLE Dim_Tiempo (
    id_tiempo_sk INT PRIMARY KEY,
    Dia INT,
    Mes INT,
    Anio INT,
   CONSTRAINT chk_tiempo_rango CHECK ( Dia BETWEEN 1 AND 31 AND Mes BETWEEN 1 AND 12 AND  Anio BETWEEN 2000 AND 2100 )
);


CREATE TABLE Dim_Perito (
    id_perito_sk INT AUTO_INCREMENT PRIMARY KEY, -- Clave subrogada
    Nombre_Perito VARCHAR(100)
);


CREATE TABLE Dim_TipoSiniestro (
    id_tipo_siniestro_sk INT AUTO_INCREMENT PRIMARY KEY,
    Nombre_Siniestro VARCHAR(100)
);


CREATE TABLE Fact_Poliza (
    id_poliza_sk INT AUTO_INCREMENT PRIMARY KEY,
    id_persona_tomador_sk INT NOT NULL,
    id_persona_receptor_sk INT NOT NULL,
    id_fecha_venta_sk INT NOT NULL,
   
    id_objeto_sk INT NOT NULL,
    id_agente_sk INT NOT NULL,
    id_tipo_seguro_sk INT NOT NULL,
    id_ubicacion_sk INT NOT NULL,
   
    monto_prima DECIMAL(12,2) NOT NULL,
    suma_garantizada DECIMAL(12,2) NOT NULL,


    CONSTRAINT fk_persona_tomador FOREIGN KEY (id_persona_tomador_sk) REFERENCES Dim_Personas(id_persona_sk),
    CONSTRAINT fk_persona_receptor FOREIGN KEY (id_persona_receptor_sk) REFERENCES Dim_Personas(id_persona_sk),
    CONSTRAINT fk_fecha_venta FOREIGN KEY (id_fecha_venta_sk) REFERENCES Dim_Tiempo(id_tiempo_sk),
    CONSTRAINT fk_objeto FOREIGN KEY (id_objeto_sk) REFERENCES Dim_Objeto(id_objeto_sk),
    CONSTRAINT fk_agente FOREIGN KEY (id_agente_sk) REFERENCES Dim_Agente(id_agente_sk),
    CONSTRAINT fk_tipo_seguro FOREIGN KEY (id_tipo_seguro_sk) REFERENCES Dim_Tipo_Seguro(id_tipo_seguro_sk),
    CONSTRAINT fk_ubicacion FOREIGN KEY (id_ubicacion_sk) REFERENCES Dim_Ubicacion(id_ubicacion_sk),
    CONSTRAINT chk_suma_garantizada_positiva CHECK (suma_garantizada > 0),
    CONSTRAINT chk_monto_prima_positiva CHECK (monto_prima > 0)
);


CREATE TABLE Fact_Siniestro (
    -- Clave Subrogada (PK)
    SiniestroKey INT AUTO_INCREMENT PRIMARY KEY,
    id_poliza_sk INT NOT NULL,
    FechaAperturaKey INT NOT NULL,
    FechaCierreKey INT NOT NULL,
    CobradorKey INT NOT NULL, -- Apunta a Dim_Personas (rol cobrador)
    PeritoKey INT NOT NULL,   -- Apunta a Dim_Personas o Dim_Perito
    TipoSiniestroKey INT,   -- Apunta a Dim_TipoSiniestro (descripción del hecho)
    UbicacionKey INT NOT NULL, -- Apunta a Dim_Ubicacion
   
    monto_declarado DECIMAL(12,2) NOT NULL,
    monto_evaluado DECIMAL(12,2) NOT NULL,
    monto_pagado DECIMAL(12,2) NOT NULL,
       
    CONSTRAINT fk_siniestro_poliza FOREIGN KEY (id_poliza_sk)
        REFERENCES Fact_Poliza(id_poliza_sk),
 
    CONSTRAINT fk_siniestro_fecha_apertura FOREIGN KEY (FechaAperturaKey)
        REFERENCES Dim_Tiempo(id_tiempo_sk),


    CONSTRAINT fk_siniestro_fecha_cierre FOREIGN KEY (FechaCierreKey)
        REFERENCES Dim_Tiempo(id_tiempo_sk),
       
    CONSTRAINT fk_siniestro_cobrador FOREIGN KEY (CobradorKey)
        REFERENCES Dim_Personas(id_persona_sk),
       
    CONSTRAINT fk_siniestro_perito FOREIGN KEY (PeritoKey)
        REFERENCES Dim_Perito(id_perito_sk),
       
    CONSTRAINT fk_siniestro_suceso FOREIGN KEY (TipoSiniestroKey)
        REFERENCES Dim_TipoSiniestro(id_tipo_siniestro_sk),
       
    CONSTRAINT fk_siniestro_ubicacion FOREIGN KEY (UbicacionKey)
        REFERENCES Dim_Ubicacion(id_ubicacion_sk),


    CONSTRAINT chk_jerarquia_montos CHECK (monto_declarado >= monto_evaluado AND monto_evaluado >= monto_pagado AND monto_pagado >= 0 ),
    CONSTRAINT chk_fechas_orden CHECK (FechaAperturaKey <= FechaCierreKey )
);


CREATE INDEX idx_siniestro_poliza ON Fact_Siniestro(id_poliza_sk);
CREATE INDEX idx_siniestro_apertura ON Fact_Siniestro(FechaAperturaKey);
CREATE INDEX idx_siniestro_perito ON Fact_Siniestro(PeritoKey);
CREATE INDEX idx_poliza_ubicacion ON Fact_Poliza(id_ubicacion_sk);
CREATE INDEX idx_poliza_agente ON Fact_Poliza(id_agente_sk);
