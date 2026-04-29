from sqlalchemy import create_engine

# Credenciales centralizadas
USER = "root"
PASSWORD = "root"
HOST = "localhost"

# Motores de conexión que exportamos a los demás scripts
engine_staging = create_engine(f"mysql+mysqlconnector://{USER}:{PASSWORD}@{HOST}/dw_staging")
engine_dw = create_engine(f"mysql+mysqlconnector://{USER}:{PASSWORD}@{HOST}/data_warehouse_seguros")