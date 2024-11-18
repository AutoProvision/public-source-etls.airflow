from datetime import datetime, timedelta
import boto3
from airflow.decorators import dag, task

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=10),
    'start_date': datetime(2023, 11, 1),
}

@dag(
    'dag_testadora_de_iam',
    default_args=default_args,
    description='DAG para Testar o IAM do bucket',
    schedule_interval=timedelta(days=1),
    catchup=False,
)
def dag_etl_taxa_selic():

    @task
    def teste1():
        s3 = boto3.client('s3')

        response = s3.list_buckets()
        print("Buckets disponíveis:", response)

        bucket_name = "manoel-almeida"
        file_name = "teste.txt"
        content = "Este é um teste de upload de arquivo para o bucket S3."

        s3.put_object(Bucket=bucket_name, Key=file_name, Body=content)
        print(f"Arquivo '{file_name}' enviado com sucesso para o bucket '{bucket_name}'.")

    teste1 = teste1()

    teste1

dag_instance = dag_etl_taxa_selic()
