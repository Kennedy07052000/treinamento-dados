import csv
import psycopg2
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'kennedy_lima',
    'depends_on_past': False,
    'start_date': datetime(2020, 9, 16),
    'retries': 0,
}

def agendamento_carfa_airflow():
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

            
with DAG('agendamento_de_carga', schedule_interval = timedelta(minutes=20), catchup = False, default_args = default_args ) as dag:

     agendamento = PythonOperator(task_id = 'agendamento', python_callable = agendamento_carfa_airflow)

     agendamento