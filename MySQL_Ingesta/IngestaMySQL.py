import mysql.connector
import pandas as pd
import boto3

# ——— AJUSTA ESTO ———
DB_CONFIG = {
    "host":     "3.230.28.178",
    "port":     8002,
    "user":     "root",
    "password": "utec",
    "database": "citasdb"
}
BUCKET_NAME = "bucket-ingesta-mysql"
# ——————————————————

def export_table_to_s3(table_name: str):
    # Conectar y leer la tabla entera en un DataFrame
    cnx = mysql.connector.connect(**DB_CONFIG)
    df  = pd.read_sql(f"SELECT * FROM {table_name}", con=cnx)
    cnx.close()

    # Volcar a CSV local
    csv_file = f"{table_name}.csv"
    df.to_csv(csv_file, index=False, encoding="utf-8")
    print(f"✔ Tabla `{table_name}` exportada a `{csv_file}`")

    # Subir a S3
    s3 = boto3.client("s3")
    s3.upload_file(csv_file, BUCKET_NAME, csv_file)
    print(f"✔ `{csv_file}` subido a s3://{BUCKET_NAME}/{csv_file}")

if __name__ == "__main__":
    for tbl in ("citas", "recetas"):
        export_table_to_s3(tbl)