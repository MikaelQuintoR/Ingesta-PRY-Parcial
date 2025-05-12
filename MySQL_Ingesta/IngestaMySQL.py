import uuid
import random
from datetime import datetime, timedelta
import psycopg2

# ——— AJUSTA ESTOS PARÁMETROS ———
DB_HOST     = "3.230.28.178"
DB_PORT     = 8001
DB_USER     = "postgres"
DB_PASSWORD = "utec"
DB_NAME     = "citasdb"

TOTAL_CITAS   = 20000   # cuántas citas crear

# Función para fecha/hora aleatoria
def random_datetime(start_year=2024, end_year=2025):
    start = datetime(start_year, 1, 1)
    end   = datetime(end_year, 12, 31, 23, 59, 59)
    delta = end - start
    return start + timedelta(seconds=random.randint(0, int(delta.total_seconds())))

# Conexión
conn = psycopg2.connect(
    host=DB_HOST, port=DB_PORT,
    user=DB_USER, password=DB_PASSWORD,
    dbname=DB_NAME
)
cur = conn.cursor()

# Preparar consultas
Q_INSERT_CITA = """
    INSERT INTO citas
      (idcita, idpaciente, iddoctor, especialidad, fecha_hora, tipo)
    VALUES (%s, %s, %s, %s, %s, %s)
"""
Q_INSERT_RECETA = """
    INSERT INTO recetas
      (idreceta, idcita, fecha_emision, medicamentos,
       idpaciente, iddoctor, diagnostico, duracion,
       observaciones, requiere_examen_medico)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

especialidades = ["Cardiología","Pediatría","Dermatología","Neurología","Oftalmología"]
tipos         = ["Consulta","Control","Urgencia","Revisión"]
diagnosticos  = ["Gripe","Hipertensión","Alergia","Infección","Migraña"]
meds          = ["Paracetamol","Ibuprofeno","Amoxicilina","Loratadina","Metformina"]
observs       = ["", "Tomar con comida", "Evitar alcohol", "Revisión en 1 semana"]

# Batch insert para rendimiento
batch_citas   = []
batch_recetas = []

print(f"Iniciando ingesta de {TOTAL_CITAS} citas y recetas asociadas...")
for _ in range(TOTAL_CITAS):
    # Generar datos de cita
    idc   = str(uuid.uuid4())
    idp   = str(uuid.uuid4())
    idd   = str(uuid.uuid4())
    esp   = random.choice(especialidades)
    fh    = random_datetime()
    tp    = random.choice(tipos)
    batch_citas.append((idc, idp, idd, esp, fh, tp))

    # Generar datos de receta
    idr   = str(uuid.uuid4())
    fe    = random_datetime().date()
    med   = f"{random.choice(meds)} {random.randint(100,500)}mg"
    diag  = random.choice(diagnosticos)
    dur   = f"{random.randint(3,14)} días"
    obs   = random.choice(observs)
    req   = random.choice([True, False])
    batch_recetas.append((idr, idc, fe, med, idp, idd, diag, dur, obs, req))

    # Cada 1 000 registros, volcar al DB
    if len(batch_citas) >= 1000:
        cur.executemany(Q_INSERT_CITA, batch_citas)
        cur.executemany(Q_INSERT_RECETA, batch_recetas)
        conn.commit()
        batch_citas.clear()
        batch_recetas.clear()

# Volcar cualquier resto
if batch_citas:
    cur.executemany(Q_INSERT_CITA, batch_citas)
    cur.executemany(Q_INSERT_RECETA, batch_recetas)
    conn.commit()

print("✔ Ingesta completada.")

cur.close()
conn.close()