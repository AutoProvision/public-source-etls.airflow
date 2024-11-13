from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os

def ola_mundo():
    print("Olá, mundo!")
    print("Mais um log do DAG de teste")
    print(os.listdir('./'))

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 11, 1),
}

with DAG(
    'meu_primeiro_dag',
    default_args=default_args,
    description='Um simples DAG de exemplo',
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:

    tarefa_ola_mundo = PythonOperator(
        task_id='tarefa_ola_mundo',
        python_callable=ola_mundo,
    )
