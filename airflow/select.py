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

def select():
    conn = psycopg2.connect("host=localhost dbname=airflow user=postgres")
    cur = conn.cursor()
    cur.execute("SELECT * FROM users" )
    row = cur.fetchone()
    while row is not None:
        print(row)
        row = cur.fetchone()

with DAG('select', schedule_interval='@once', catchup=False, default_args=default_args ) as dag:

     select_airflow = PythonOperator(task_id = 'select_airflow', python_callable = select)

     select_airflow 