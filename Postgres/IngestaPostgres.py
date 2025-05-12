# ingestpostgres.py
import csv
import psycopg2

# ————— Configuración de conexión —————
DB_HOST     = "172.31.24.145"
DB_PORT     = 8006
DB_NAME     = "personasdb"
DB_USER     = "postgres"
DB_PASSWORD = "utec"

BATCH_SIZE = 1000


def copy_csv_to_table(cursor, csv_path, table_name, columns):
    placeholder = ", ".join(["%s"] * len(columns))
    cols        = ", ".join(columns)
    sql         = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholder});"

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
            "data/persona.csv",
            "persona",
            [
                "dni","password","nombres","apellidos","fecha_nacimiento",
                "sexo","direccion","telefono","email","type"
            ]
        )
        copy_csv_to_table(
            cur,
            "data/medico.csv",
            "medico",
            ["id","especialidad","colegiatura","horario"]
        )
        copy_csv_to_table(
            cur,
            "data/paciente.csv",
            "paciente",
            ["id","seguro_salud","estado_civil","tipo_sangre"]
        )

        conn.commit()
        print("Carga completada exitosamente.")

    except Exception as e:
        print(f"Error durante la ingestión: {e}")
    finally:
        if 'cur' in locals(): cur.close()
        if 'conn' in locals(): conn.close()