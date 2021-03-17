# Importando as bibliotecas que vamos usar nesse exemplo
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
# Definindo alguns argumentos básicos
default_args = {
   'owner': 'marcia_regina',
   'depends_on_past': False,
   'start_date': datetime(2020, 9, 16),
   'retries': 0,
   }
# Nomeando a DAG e definindo quando ela vai ser executada (você pode usar argumentos em Crontab também caso queira que a DAG execute por exemplo todos os dias ás 8 da manhã)
dag = DAG(
   'minha-primeira-dag',
   schedule_interval=timedelta(minutes=1),
   catchup=False,
   default_args=default_args
   )
# Definindo as tarefas que a DAG vai executar, neste caso a execução de dois programas Python, chamando sua execução por comandos bash
t1 = BashOperator(
   task_id='primeira_task',
   bash_command='echo : "Olá mundo!!!"',
   dag=dag)
t2 = BashOperator(
   task_id='segunda_task',
   bash_command='echo : "Testando task 2"',
   dag=dag)
t1 >> t2