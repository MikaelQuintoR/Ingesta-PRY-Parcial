# ingestpostgres.py
import os
import csv
import boto3
import psycopg2
from botocore.exceptions import ClientError

# ————— Configuración de AWS S3 —————
AWS_S3_BUCKET = "ingesta-pryparcial"
S3_PREFIX     = "data/"

# ————— Configuración de conexión PostgreSQL —————
DB_HOST     = "172.31.24.145"
DB_PORT     = 8006
DB_NAME     = "personasdb"
DB_USER     = "postgres"
DB_PASSWORD = "utec"

BATCH_SIZE = 1000
LOCAL_DATA_DIR = "data"
CSV_FILES = {
    "persona":   "persona.csv",
    "medico":    "medico.csv",
    "paciente":  "paciente.csv",
}


def ensure_local_dir(path):
    if not os.path.isdir(path):
        os.makedirs(path, exist_ok=True)


def download_csv_from_s3(s3_client, bucket, key, local_path):
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
    except ClientError as e:
        raise FileNotFoundError(f"Archivo {key} no encontrado en el bucket {bucket}")
    s3_client.download_file(bucket, key, local_path)


def copy_csv_to_table(cursor, csv_path, table_name, columns):
    placeholder = ", ".join(["%s"] * len(columns))
    cols = ", ".join(columns)
    sql = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholder});"

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader)  # saltar encabezado
        batch = []
        for row in reader:
            batch.append(row)
            if len(batch) >= BATCH_SIZE:
                cursor.executemany(sql, batch)
                batch.clear()
        if batch:
            cursor.executemany(sql, batch)


if __name__ == "__main__":
    # Preparar descarga de CSVs desde S3
    ensure_local_dir(LOCAL_DATA_DIR)
    s3 = boto3.client('s3')

    # Descargar y verificar archivos CSV
    for table, filename in CSV_FILES.items():
        key = f"{S3_PREFIX}{filename}"
        local_path = os.path.join(LOCAL_DATA_DIR, filename)
        print(f"Descargando {key} a {local_path}...")
        try:
            download_csv_from_s3(s3, AWS_S3_BUCKET, key, local_path)
        except FileNotFoundError as fnf:
            print(fnf)
            exit(1)

    # Conectar a PostgreSQL e ingresar datos
    try:
        conn = psycopg2.connect(
            host     = DB_HOST,
            port     = DB_PORT,
            dbname   = DB_NAME,
            user     = DB_USER,
            password = DB_PASSWORD
        )
        cur = conn.cursor()

        # Ingesta de CSVs
        copy_csv_to_table(
            cur,
            os.path.join(LOCAL_DATA_DIR, CSV_FILES['persona']),
            "persona",
            [
                "dni","password","nombres","apellidos","fecha_nacimiento",
                "sexo","direccion","telefono","email","type"
            ]
        )
        copy_csv_to_table(
            cur,
            os.path.join(LOCAL_DATA_DIR, CSV_FILES['medico']),
            "medico",
            ["id","especialidad","colegiatura","horario"]
        )
        copy_csv_to_table(
            cur,
            os.path.join(LOCAL_DATA_DIR, CSV_FILES['paciente']),
            "paciente",
            ["id","seguro_salud","estado_civil","tipo_sangre"]
        )

        conn.commit()
        print("Carga completada exitosamente.")

    except Exception as e:
        print(f"Error durante la ingestión: {e}")
        exit(1)
    finally:
        if 'cur' in locals(): cur.close()
        if 'conn' in locals(): conn.close()
