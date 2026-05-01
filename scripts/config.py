import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
import urllib.parse  # Para manejar caracteres especiales en la clave

load_dotenv()

# 1. Leemos las variables
db_user = os.getenv("DB_USER", "root")
db_password = os.getenv("DB_PASSWORD", "")
db_host = os.getenv("DB_HOST", "localhost")
db_port = os.getenv("DB_PORT", "3306")
db_name = os.getenv("DB_NAME", "seguros_staging")

# 2. Limpieza preventiva: eliminamos posibles arrobas o espacios accidentales
db_host = db_host.replace("@", "").strip()
db_user = db_user.strip()

# 3. Codificamos la contraseña (por si tiene caracteres como @, #, $, etc.)
password_encoded = urllib.parse.quote_plus(db_password)

# 4. Construimos la URL de forma inteligente
# Si no hay password, la URL es diferente
if db_password:
    DATABASE_URI = f"mysql+mysqlconnector://{db_user}:{password_encoded}@{db_host}:{db_port}/{db_name}"
else:
    DATABASE_URI = f"mysql+mysqlconnector://{db_user}@{db_host}:{db_port}/{db_name}"

engine_staging = create_engine(DATABASE_URI)

# Debug opcional (quitalo después de que funcione)
print(f"✅ Intentando conectar a: {db_host} como {db_user}...")