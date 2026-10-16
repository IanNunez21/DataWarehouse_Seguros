# Data Warehouse de Seguros

Proyecto de integración y análisis de datos de seguros. Implementa un pipeline ETL en Python que carga archivos CSV en una base de staging, limpia y transforma los datos y los organiza en un modelo dimensional MySQL para consultar pólizas y siniestros. Las consultas SQL pueden usarse para análisis exploratorio y dashboards en Metabase.

## Contenido

- `SourcesSeguros/`: archivos CSV de origen.
- `scripts_principales/`: pipeline ETL, configuración de conexiones, transformaciones y cargas de dimensiones y hechos.
- `scripts_adicionales/`: scripts auxiliares para generar datos maestros.
- `scriptFINAL.sql`: creación de bases y tablas del warehouse.
- `consultas.sql`: consultas analíticas y consultas preparadas para Metabase.
- `docker-compose.yml`: servicio de Metabase.
- `requirements.txt`: dependencias de Python.

## Arquitectura

El flujo principal está dividido en cuatro etapas:

1. **Staging:** carga los CSV de `SourcesSeguros/` en la base `dw_staging`.
2. **Transformación:** estandariza y valida clientes, agentes, pólizas, peritos, objetos asegurados, partes, evaluaciones, pagos, garantías e indicadores de fraude.
3. **Dimensiones:** carga dimensiones de personas, ubicación, tiempo, agentes, peritos, objetos, tipos de seguro y tipos de siniestro.
4. **Hechos:** carga las tablas `Fact_Poliza` y `Fact_Siniestro` en `data_warehouse_seguros`.

El modelo conserva historial de cambios de personas mediante versiones de la dimensión. Las consultas permiten analizar primas y siniestros por tipo de seguro, perfil de cliente, período, región, agente y perito.

## Requisitos

- Python 3
- MySQL en ejecución y accesible desde la máquina donde se ejecuta el ETL
- Docker y Docker Compose para levantar Metabase (opcional)

## Puesta en marcha

1. Cloná el repositorio y entrá en su carpeta:

   ```bash
   git clone https://github.com/IanNunez21/DataWarehouse_Seguros.git
   cd DataWarehouse_Seguros
   ```

2. Instalá las dependencias:

   ```bash
   pip install -r requirements.txt
   ```

3. Configurá usuario, contraseña y host de MySQL en `scripts_principales/config.py`. El archivo contiene valores de ejemplo; reemplazalos por las credenciales de tu entorno.

4. Creá las bases y tablas del warehouse ejecutando `scriptFINAL.sql` en MySQL. **Atención:** el script elimina y vuelve a crear las tablas de dimensiones y hechos; si ya tenés datos que quieras conservar, hacé una copia antes de ejecutarlo.

5. Desde la raíz del repositorio, ejecutá el pipeline:

   **Linux/macOS**
   ```bash
   PYTHONPATH=scripts_principales python scripts_principales/main.py
   ```

   **PowerShell**
   ```powershell
   $env:PYTHONPATH = "scripts_principales"
   python scripts_principales/main.py
   ```

El proceso espera encontrar los CSV en `SourcesSeguros/` y las bases `dw_staging` y `data_warehouse_seguros` disponibles en MySQL.

## Metabase

Para iniciar Metabase:

```bash
docker compose up -d
```

Luego abrí [http://localhost:3000](http://localhost:3000), completá la configuración inicial y conectá Metabase a la base MySQL `data_warehouse_seguros`. El archivo `docker-compose.yml` levanta Metabase; MySQL debe estar instalado o ejecutándose por separado.

Las consultas del archivo `consultas.sql` sirven como punto de partida para preguntas y dashboards. Algunas usan variables opcionales de Metabase, como `{{anio}}` y `{{mes}}`.

## Fuentes de datos

El pipeline contempla archivos de clientes, agentes, pólizas, objetos asegurados, partes de accidentes, peritos, evaluaciones, pagos, garantías e indicadores de fraude, además de fuentes de seguros de auto y salud. Los archivos se leen desde `SourcesSeguros/`.
