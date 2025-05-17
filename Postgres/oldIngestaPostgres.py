
import os
import sys
import boto3
from botocore.exceptions import ClientError

# ————— Configuración de AWS S3 —————
AWS_S3_BUCKET = "ingesta-pryparcial"
# Si quieres subir a un subdirectorio, p. ej. "data/":
# S3_PREFIX = "data/"
S3_PREFIX = "Postgres/"  

# ————— Archivos locales a subir —————
LOCAL_DATA_DIR = "data"
CSV_FILES = [
    "persona.csv",
    "medico.csv",
    "paciente.csv",
]

def upload_file(s3_client, local_path, bucket, key):
    try:
        s3_client.upload_file(local_path, bucket, key)
        print(f" {local_path} → s3://{bucket}/{key}")
    except ClientError as e:
        print(f" Error subiendo {local_path} a {key}: {e}")
        sys.exit(1)

def main():
    # Comprueba que existe el directorio
    if not os.path.isdir(LOCAL_DATA_DIR):
        print(f"Directorio local '{LOCAL_DATA_DIR}' no existe.")
        sys.exit(1)

    # Crea cliente S3 (usa variables de entorno AWS_* para credenciales)
    s3 = boto3.client('s3')

    # Recorre cada CSV y súbelo
    for filename in CSV_FILES:
        local_path = os.path.join(LOCAL_DATA_DIR, filename)
        if not os.path.isfile(local_path):
            print(f"Archivo local no encontrado: {local_path}")
            sys.exit(1)

        # Si quieres usar subcarpeta en S3, asegúrate de que termine en '/'
        key = f"{S3_PREFIX}{filename}"
        upload_file(s3, local_path, AWS_S3_BUCKET, key)

    print("Todos los archivos fueron subidos correctamente.")

if __name__ == "__main__":
    main()