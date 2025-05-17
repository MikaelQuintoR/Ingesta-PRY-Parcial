import os
import sys
import pandas as pd
from sqlalchemy import create_engine

# Configuración de la conexión
DB_USER     = os.getenv('DB_USER', 'root')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'utec')
DB_HOST     = os.getenv('DB_HOST', '172.31.24.145')
DB_PORT     = os.getenv('DB_PORT', '8005')
DB_NAME     = os.getenv('DB_NAME', 'citasdb')
TABLES      = os.getenv('TABLES', 'citas,recetas').split(',')

SQLALCHEMY_DATABASE_URI = (
    f"mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

# Crear conexión
try:
    engine = create_engine(SQLALCHEMY_DATABASE_URI)
except Exception as e:
    print(f"Error creando engine SQLAlchemy: {e}")
    sys.exit(1)

# Función para insertar cada tabla desde su CSV en 'data/'
def insert_table_from_csv(table_name: str):
    csv_file = os.path.join('data', f"{table_name}.csv")
    if not os.path.exists(csv_file):
        print(f"Archivo {csv_file} no encontrado. Se omite.")
        return

    try:
        df = pd.read_csv(csv_file)
    except Exception as e:
        print(f"Error leyendo CSV {csv_file}: {e}")
        return

    try:
        df.to_sql(table_name, con=engine, if_exists='replace', index=False)
        print(f"Tabla '{table_name}' insertada correctamente en la base de datos.")
    except Exception as e:
        print(f"Error insertando tabla {table_name}: {e}")

# Ejecutar para todas las tablas
if __name__ == "__main__":
    for tbl in TABLES:
        insert_table_from_csv(tbl)
