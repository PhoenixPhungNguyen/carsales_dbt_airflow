import csv
import io

import psycopg2
from psycopg2.extras import execute_values


def _upload_csv_to_postgres(csv_bytes: bytes, table_name: str, connection_info: dict):
    print(f"Uploading CSV to Postgres table: {table_name}")
    conn = psycopg2.connect(**connection_info)
    cursor = conn.cursor()

    # Decode CSV and prepare reader
    csvfile = io.StringIO(csv_bytes.decode("utf-8"))
    reader = csv.reader(csvfile)
    headers = next(reader)  # Extract headers

    # Optional: Create table if not exists
    cursor.execute(
        f"DROP TABLE IF EXISTS landing.lnd_{table_name}"
    )  # Drop table if exists for fresh load, not recommended for production
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS landing.lnd_{table_name} (
            {", ".join(f"{col} TEXT" for col in headers)}
        )
        """
    )

    # Insert rows
    insert_sql = (
        f"INSERT INTO landing.lnd_{table_name} ({', '.join(headers)}) VALUES %s"
    )

    cleaned_rows = [[value.strip() for value in row] for row in reader]
    execute_values(cursor, insert_sql, cleaned_rows)

    conn.commit()
    cursor.close()
    conn.close()
