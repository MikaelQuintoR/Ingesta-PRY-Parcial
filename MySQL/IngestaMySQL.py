import boto3
import mysql.connector
import pandas as pd

# ————— Configuración de conexión MySQL —————
DB_CONFIG = {
    "host":     "172.31.24.145",
    "port":     8005,
    "user":     "root",
    "password": "utec",
    "database": "citasdb"
}
BUCKET_NAME = "bucket-ingesta-mysql"


def export_table_to_s3(table_name: str):
    try:
        # Conexión a la base de datos
        cnx = mysql.connector.connect(**DB_CONFIG)
        df  = pd.read_sql(f"SELECT * FROM {table_name}", con=cnx)
    except mysql.connector.Error as err:
        print(f"Error de conexión o consulta en tabla {table_name}: {err}")
        return
    finally:
        # Cerrar conexión si existe
        if 'cnx' in locals() and cnx.is_connected():
            cnx.close()

    # Exportar a CSV
    csv_file = f"{table_name}.csv"
    try:
        df.to_csv(csv_file, index=False, encoding="utf-8")
    except Exception as err:
        print(f"Error al generar CSV {csv_file}: {err}")
        return

    # Subir a S3
    try:
        # boto3 lee credenciales de entorno o IAM role
        s3 = boto3.client("s3")
        s3.upload_file(csv_file, BUCKET_NAME, csv_file)
        print(f"Tabla {table_name} subida correctamente a s3://{BUCKET_NAME}/{csv_file}")
    except Exception as err:
        print(f"Error al subir {csv_file} a S3: {err}")


if __name__ == "__main__":
    for tbl in ("citas", "recetas"):
        export_table_to_s3(tbl)