import boto3
import json

from bson import ObjectId
from pymongo import MongoClient

# Configuración
nombreColeccion = "examenes"
nombreBucket = "bucket-ingesta-parcial"
ficheroUpload = "data.json"

# Conexión con MongoDB
client = MongoClient("mongodb://localhost:27017")  # Cambiar la URI según tu configuración
db = client['citasdb']
collection = db[nombreColeccion]

# Obtener todos los documentos
def obtener_datos_mongodb():
    data = list(collection.find())
    return data

# Convertir ObjectId a string y otros tipos MongoDB a JSON serializables
def mongodb_to_json(obj):
    if isinstance(obj, ObjectId):
        return str(obj)
    return obj

# Obtener datos y preparar archivo JSON
data = obtener_datos_mongodb()

# Guardar datos en archivo JSON
with open(ficheroUpload, 'w') as file:
    json.dump(data, file, indent=4, default=mongodb_to_json)

# Subir archivo a S3
s3 = boto3.client('s3')
try:
    s3.upload_file(ficheroUpload, nombreBucket, ficheroUpload)
except Exception as e:
    pass