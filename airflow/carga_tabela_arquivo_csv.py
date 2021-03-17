import csv
import psycopg2
from airflow import DAG
from datetime import datetime
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'kennedy_lima',
    'depends_on_past': False,
    'start_date': datetime(2020, 9, 16),
    'retries': 3,
}

def carga():
    conn = psycopg2.connect("host=localhost dbname=airflow user=postgres")
    cur = conn.cursor()
    with open('/home/kennedymartins/Documentos/carga.csv', 'r') as f:
        reader = csv.reader(f)
        next(reader) # Skip the header row.
        for row in reader:
            cur.execute(
                "INSERT INTO users VALUES (%s, %s)",
                row
                )
            conn.commit()


with DAG('carga_tabela', schedule_interval='@once', catchup=False, default_args=default_args ) as dag:

    carga_tabela_airflow_teste = PythonOperator(task_id = 'carga_tabela_airflow_teste', python_callable = carga)

    carga_tabela_airflow_teste