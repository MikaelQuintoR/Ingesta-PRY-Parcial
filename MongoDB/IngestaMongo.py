import os
import sys
import json
import boto3
from pymongo import MongoClient
from bson import json_util

# ————— Configuración de MongoDB —————
MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb://admin:utec@172.31.24.145:8007/historialdb?authSource=admin"
)
MONGO_DB        = os.getenv("MONGO_DB", "historialdb")
MONGO_COLL      = os.getenv("MONGO_COLL", "examenes")
BUCKET_NAME     = os.getenv("S3_BUCKET", "ingesta-pryparcial")
OUTPUT_FILENAME = os.getenv("OUTPUT_FILE", "data.json")

# Conexión a MongoDB
try:
    client = MongoClient(MONGO_URI)
    db     = client[MONGO_DB]
    coll   = db[MONGO_COLL]
except Exception as e:
    print(f"Error conectando a MongoDB: {e}")
    sys.exit(1)

# Obtener datos
try:
    docs = list(coll.find())
    if not docs:
        print(f"Colección '{MONGO_COLL}' vacía o no existe.")
except Exception as e:
    print(f"Error leyendo la colección '{MONGO_COLL}': {e}")
    sys.exit(1)

# Guardar a JSON con serialización de BSON
try:
    with open(OUTPUT_FILENAME, 'w', encoding='utf-8') as f:
        json.dump(docs, f, default=json_util.default, indent=4)
    print(f"Archivo '{OUTPUT_FILENAME}' generado exitosamente.")
except Exception as e:
    print(f"Error creando JSON '{OUTPUT_FILENAME}': {e}")
    sys.exit(1)

# Subir a S3
try:
    s3 = boto3.client('s3')
    s3.upload_file(OUTPUT_FILENAME, BUCKET_NAME, OUTPUT_FILENAME)
    print(f"Subido a s3://{BUCKET_NAME}/{OUTPUT_FILENAME}")
except Exception as e:
    print(f"Error subiendo '{OUTPUT_FILENAME}' a S3: {e}")
    sys.exit(1)