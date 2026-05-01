import os
from dotenv import load_dotenv

load_dotenv()

print("--- Validando Variables de Entorno ---")
print(f"Usuario: {os.getenv('DB_USER')}")
print(f"Base de Datos: {os.getenv('DB_NAME')}")
# No imprimas la contraseña entera por seguridad, solo si existe
print(f"¿Tiene contraseña?: {'Sí' if os.getenv('DB_PASSWORD') else 'No'}")