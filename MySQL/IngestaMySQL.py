import os
import sys
import pandas as pd
import boto3
from sqlalchemy import create_engine

# ————— Configuración de conexión MySQL —————
DB_USER     = os.getenv('DB_USER', 'root')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'utec')
DB_HOST     = os.getenv('DB_HOST', '172.31.24.145')
DB_PORT     = os.getenv('DB_PORT', '8005')
DB_NAME     = os.getenv('DB_NAME', 'citasdb')
BUCKET_NAME = os.getenv('S3_BUCKET', 'bucket-ingesta-mysql')
TABLES      = os.getenv('TABLES', 'citas,recetas').split(',')

# Credenciales AWS
AWS_KEY    = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_TOKEN  = os.getenv('AWS_SESSION_TOKEN')  # opcional

# URI SQLAlchemy
SQLALCHEMY_DATABASE_URI = (
    f"mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

# Crear engine
try:
    engine = create_engine(SQLALCHEMY_DATABASE_URI)
except Exception as e:
    print(f"Error creando engine SQLAlchemy: {e}")
    sys.exit(1)

# Función para exportar
def export_table_to_s3(table_name: str):
    try:
        df = pd.read_sql(f"SELECT * FROM `{table_name}`", con=engine)
    except Exception as e:
        print(f"Error leyendo tabla {table_name}: {e}")
        return

    csv_file = f"{table_name}.csv"
    try:
        df.to_csv(csv_file, index=False, encoding="utf-8")
    except Exception as e:
        print(f"Error escribiendo CSV {csv_file}: {e}")
        return

    # Configurar cliente S3 con credenciales
    try:
        s3 = boto3.client(
            's3',
            aws_access_key_id=AWS_KEY,
            aws_secret_access_key=AWS_SECRET,
            aws_session_token=AWS_TOKEN
        )
    except Exception as e:
        print(f"Error configurando cliente S3: {e}")
        return

    # Subir archivo
    try:
        s3.upload_file(csv_file, BUCKET_NAME, csv_file)
        print(f"{csv_file} subido a s3://{BUCKET_NAME}/{csv_file}")
    except Exception as e:
        print(f"Error subiendo {csv_file}: {e}")


if __name__ == "__main__":
    for tbl in TABLES:
        export_table_to_s3(tbl)