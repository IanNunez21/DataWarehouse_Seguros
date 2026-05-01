import pandas as pd
import os
import unicodedata

def normalizar_texto(texto):
    """
    Elimina tildes, acentos y caracteres especiales.
    Convierte a mayúsculas y limpia espacios.
    """
    if not isinstance(texto, str):
        return str(texto)
    
    # Normalización NFD: separa el carácter de su tilde (ej: 'á' -> 'a' + '´')
    texto_nfd = unicodedata.normalize('NFD', texto)
    # Filtramos para quedarnos solo con el carácter base (sin la tilde)
    texto_limpio = "".join(c for c in texto_nfd if unicodedata.category(c) != 'Mn')
    
    return texto_limpio.upper().strip()

print("Iniciando creación del Maestro de Localidades (Normalizado)...")

# 1. Configuración de rutas[cite: 4]
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
folder = "SourcesAdicionales"
ruta_prov = os.path.join(BASE_DIR, folder, "provincias.csv")
ruta_loc = os.path.join(BASE_DIR, folder, "localidades.csv")
ruta_salida = os.path.join(BASE_DIR, folder, "provincias_localidades.csv")

# 2. Lectura con manejo de Encoding
try:
    # Intentamos leer en UTF-8, si falla por caracteres raros, usamos Latin-1
    try:
        df_provincias = pd.read_csv(ruta_prov, encoding='utf-8')
        df_localidades = pd.read_csv(ruta_loc, encoding='utf-8')
    except UnicodeDecodeError:
        df_provincias = pd.read_csv(ruta_prov, encoding='latin-1')
        df_localidades = pd.read_csv(ruta_loc, encoding='latin-1')
        
except FileNotFoundError as e:
    print(f"❌ Error: No se encontró el archivo en {folder}. Verificá los nombres.")
    exit()

# 3. Combinar (Merge)[cite: 4]
df_combinado = pd.merge(
    df_localidades, 
    df_provincias, 
    left_on='provincia_id', 
    right_on='id', 
    suffixes=('_loc', '_prov')
)

# 4. Selección y Limpieza Progresiva[cite: 3, 4]
df_maestro = df_combinado[['nombre_prov', 'nombre_loc']].copy()

# 5. Aplicar Normalización (Quita tildes y caracteres raros)
df_maestro['provincia'] = df_maestro['nombre_prov'].apply(normalizar_texto)
df_maestro['localidad'] = df_maestro['nombre_loc'].apply(normalizar_texto)

# 6. Eliminar duplicados y seleccionar columnas finales[cite: 4]
df_maestro = df_maestro[['provincia', 'localidad']].drop_duplicates()

# 7. Guardar resultado
# Usamos utf-8-sig para que Excel y Windows reconozcan bien el formato[cite: 3]
df_maestro.to_csv(ruta_salida, index=False, encoding='utf-8-sig')

print(f"✔ Maestro creado exitosamente en: {ruta_salida}")
print(f"✔ Total de localidades únicas: {len(df_maestro)}")
print("\nPrimeras filas del resultado:")
print(df_maestro.head())