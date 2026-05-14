import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
import urllib.parse
from pathlib import Path

# Cargar variables de entorno desde .env
env_path = Path(__file__).parent.parent / "scripts" / ".env"
load_dotenv(dotenv_path=env_path)

# Leer credenciales desde .env
db_user = os.getenv("DB_USER", "root")
db_password = os.getenv("DB_PASSWORD", "")
db_host = os.getenv("DB_HOST", "localhost")
db_port = os.getenv("DB_PORT", "3306")
db_name = os.getenv("DB_NAME", "seguros_staging")

# Limpieza preventiva
db_host = db_host.replace("@", "").strip()
db_user = db_user.strip()

# Codificar contraseña para caracteres especiales
password_encoded = urllib.parse.quote_plus(db_password)

# Construir URI de conexión
if db_password:
    DATABASE_URI = f"mysql+mysqlconnector://{db_user}:{password_encoded}@{db_host}:{db_port}/{db_name}"
else:
    DATABASE_URI = f"mysql+mysqlconnector://{db_user}@{db_host}:{db_port}/{db_name}"

# Crear motores de conexión
engine_staging = create_engine(DATABASE_URI, echo=False)
engine_dw = create_engine(f"mysql+mysqlconnector://{db_user}:{password_encoded}@{db_host}:{db_port}/data_warehouse_seguros", echo=False)

print(f"✅ Configuración cargada desde: {env_path}")
print(f"📊 Conectando a: {db_host} | Base: {db_name} | Usuario: {db_user}")