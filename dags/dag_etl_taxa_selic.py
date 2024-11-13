from datetime import datetime, timedelta
import requests
from airflow.decorators import dag, task
import pandas as pd
import os

DATA_PATH = '/opt/airflow/data/taxas-selic'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=10),
    'start_date': datetime(2023, 11, 1),
}

@dag(
    'dag_etl_taxa_selic',
    default_args=default_args,
    description='DAG para ETL da taxa SELIC',
    schedule_interval=timedelta(days=1),
    catchup=False,
)
def dag_etl_taxa_selic():

    @task
    def init_local_directories():
        os.makedirs(DATA_PATH, exist_ok=True)
        os.makedirs(f'{DATA_PATH}/raw', exist_ok=True)
        os.makedirs(f'{DATA_PATH}/trusted', exist_ok=True)
        os.makedirs(f'{DATA_PATH}/refined', exist_ok=True)

    @task
    def extrair_dados():
        print(os.listdir('./'))
        response = requests.get('https://api.bcb.gov.br/dados/serie/bcdata.sgs.11/dados?formato=csv')
        print(response)
        with open(f'{DATA_PATH}/raw/selic.csv', 'wb') as file:
            file.write(response.content)

    @task
    def transformar_dados_selic():
        df = pd.read_csv(f'{DATA_PATH}/raw/selic.csv', sep=';')
        df['valor'] = df['valor'].str.replace(',', '.').astype(float)
        df['data'] = pd.to_datetime(df['data'], format='%d/%m/%Y')
        df.to_parquet(f'{DATA_PATH}/trusted/df.parquet', index=False, engine='fastparquet')
        df.to_parquet(f'{DATA_PATH}/refined/df.parquet', index=False, engine='fastparquet')

    init_local_directories = init_local_directories()
    extrair_dados = extrair_dados()
    transformar_dados_selic = transformar_dados_selic()

    init_local_directories >> extrair_dados >> transformar_dados_selic

dag_instance = dag_etl_taxa_selic()
