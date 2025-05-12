import csv
import psycopg2

# ————— Configuración de conexión —————
DB_HOST     = "3.230.28.178"
DB_PORT     = 8001
DB_NAME     = "personadb"
DB_USER     = "postgres"
DB_PASSWORD = "utec"

BATCH_SIZE = 1000

def copy_csv_to_table(cursor, csv_path, table_name, columns):
    placeholder = ", ".join(["%s"] * len(columns))
    cols        = ", ".join(columns)
    sql         = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholder});"

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader)
        batch = []
        for row in reader:
            batch.append(row)
            if len(batch) >= BATCH_SIZE:
                cursor.executemany(sql, batch)
                batch.clear()
        if batch:
            cursor.executemany(sql, batch)

# ————— Ingesta principal —————
def main():
    conn = psycopg2.connect(
        host     = DB_HOST,
        port     = DB_PORT,
        dbname   = DB_NAME,
        user     = DB_USER,
        password = DB_PASSWORD
    )
    cur = conn.cursor()

    # 1) persona.csv
    copy_csv_to_table(
        cur,
        "data/persona.csv",
        "persona",
        ["dni","password","nombres","apellidos","fecha_nacimiento",
         "sexo","direccion","telefono","email","type"]
    )

    # 2) medico.csv
    copy_csv_to_table(
        cur,
        "data/medico.csv",
        "medico",
        ["id","especialidad","colegiatura","horario"]
    )

    # 3) paciente.csv
    copy_csv_to_table(
        cur,
        "data/paciente.csv",
        "paciente",
        ["id","seguro_salud","estado_civil","tipo_sangre"]
    )

    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
