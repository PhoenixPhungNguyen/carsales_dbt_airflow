import os
from datetime import datetime

from scripts.extractors import (
    _get_google_drive_file_content,
    _get_google_drive_file_list,
)
from scripts.loaders import _upload_csv_to_postgres

from airflow.decorators import dag, task

GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
GOOGLE_DRIVE_FOLDER_ID = os.getenv("GOOGLE_DRIVE_FOLDER_ID")

POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRESS_CONNECTION_INFO = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "host": POSTGRES_HOST,
    "port": POSTGRES_PORT,
    "dbname": POSTGRES_DB,
}

DBT_USER = os.getenv("DBT_USER")
DBT_ROLE = os.getenv("DBT_ROLE")
DBT_PASSWORD = os.getenv("DBT_PASSWORD")


@dag(
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["carsales"],
)
def carsales_dag():
    @task
    def extract_file_list():
        file_list = _get_google_drive_file_list(
            folder_id=GOOGLE_DRIVE_FOLDER_ID, api_key=GOOGLE_API_KEY
        )
        return file_list

    @task
    def load_to_postgres(file):
        file_content = _get_google_drive_file_content(
            file_id=file["id"], api_key=GOOGLE_API_KEY
        )
        table_name = file["name"].replace(".csv", "").lower()
        _upload_csv_to_postgres(
            csv_bytes=file_content,
            table_name=table_name,
            connection_info=POSTGRESS_CONNECTION_INFO,
        )

    file_list = extract_file_list()
    load_to_postgres.expand(file=file_list)


carsales_dag()
