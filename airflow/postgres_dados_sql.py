import psycopg2
import pyodbc
from airflow import DAG
from datetime import datetime
from airflow.operators.python_operator import PythonOperator

default_args = {
'owner': 'kennedy_lima',
'depends_on_past': False,
'start_date': datetime(2020, 9, 16),
'retries': 3,
}

def postgres_sql():
	server = 'localhost' 
	database = 'airflow' 
	username = 'sa' 
	password = 'E$Kgp@mo6cent0s' 
	conexao_postgres = psycopg2.connect("host=localhost dbname=airflow user=postgres")
	conexao_sql_server = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)

	cur_postgres = conexao_postgres.cursor()
	cur_sql_server= conexao_sql_server.cursor()

	cur_postgres.execute("SELECT * FROM users")
	row = cur_postgres.fetchone()
	while row is not None:
		print(row)
		cur_sql_server.execute(
			"INSERT INTO users VALUES (?, ?)",
			row
			)
		row = cur_postgres.fetchone()

		cur_sql_server.commit()

with DAG('transferência_dados', schedule_interval='@once', catchup=False, default_args=default_args ) as dag:

     potgres_dados_sql= PythonOperator(task_id = 'potgres_dados_sql', python_callable = postgres_sql)

     potgres_dados_sql 





