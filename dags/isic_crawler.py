from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
# Corrigindo a importação - usando o HttpOperator em vez de SimpleHttpOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable

import json 
import requests
import io
import logging
import pandas as pd


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def get_image_metadata(**context):
    """Obtém metadados das imagens do ISIC Archive."""
    # URL da API do ISIC
    api_url = "https://api.isic-archive.com/api/v2/images"
    
    # Parâmetros da consulta (limitando a 100 imagens para o exemplo)
    params = {
        "limit": 100,
        "offset": 0,
        "sort": "name",
    }
    
    # Fazer a requisição
    response = requests.get(api_url, params=params)
    response.raise_for_status()
    
    # Processar a resposta
    data = response.json()
    
    # Salvar em um contexto para ser usado por outras tarefas
    image_list = []
    for image in data['results']:
        image_list.append({
            'id': image['isic_id'],
            'url': image.get("files").get("full").get("url"),
            'metadata' : image.get("metadata")
        })
    
    # Passar para a próxima tarefa
    context['ti'].xcom_push(key='image_metadata', value=image_list)
    
    # Logging
    logging.info(f"Retrieved metadata for {len(image_list)} images")
    
    return image_list


def download_images(**context):
    """Baixa as imagens e salva no MinIO."""
    # Obter os metadados
    image_list = context['ti'].xcom_pull(key='image_metadata', task_ids='get_image_metadata')
    
    # Inicializar o hook S3 para conectar ao MinIO
    s3_hook = S3Hook(aws_conn_id='minio_conn')
    
    # Nome do bucket
    bucket_name = 'isic-images'
    
    # Verificar se o bucket existe, senão criar
    if not s3_hook.check_for_bucket(bucket_name):
        s3_hook.create_bucket(bucket_name=bucket_name)
    
    # Baixar e salvar as imagens
    downloaded_count = 0
    for image in image_list:
        try:
            # Baixar a imagem
            response = requests.get(image['url'])
            response.raise_for_status()
            
            # Salvar no MinIO
            s3_hook.load_bytes(
                bytes_data=response.content,
                key=f"{image['id']}.jpg",
                bucket_name=bucket_name,
                replace=True
            )
            
            downloaded_count += 1
            
            # Limitar a 10 para o exemplo (remova isso para baixar todas)
            if downloaded_count >= 10:
                break
                
        except Exception as e:
            logging.error(f"Error downloading image {image['id']}: {str(e)}")
    
    logging.info(f"Successfully downloaded {downloaded_count} images to MinIO")
    
    return downloaded_count


def save_metadata_to_csv(**context):
    """Salva os metadados em um arquivo CSV no MinIO."""
    # Obter os metadados
    image_list = context['ti'].xcom_pull(key='image_metadata', task_ids='get_image_metadata')
    
    # Converter para DataFrame
    df = pd.DataFrame(image_list)
    
    # Salvar como CSV em memória
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    
    # Inicializar o hook S3
    s3_hook = S3Hook(aws_conn_id='minio_conn')
    
    # Nome do bucket
    bucket_name = 'isic-metadata'
    
    # Verificar se o bucket existe, senão criar
    if not s3_hook.check_for_bucket(bucket_name):
        s3_hook.create_bucket(bucket_name=bucket_name)
    
    # Salvar no MinIO
    s3_hook.load_string(
        string_data=csv_buffer.getvalue(),
        key=f"isic_metadata_{datetime.now().strftime('%Y%m%d')}.csv",
        bucket_name=bucket_name,
        replace=True
    )
    
    logging.info(f"Saved metadata for {len(image_list)} images to CSV in MinIO")


# Criar a DAG
with DAG(
    'isic_archive_crawler',
    default_args=default_args,
    description='DAG para fazer crawling no ISIC Archive',
    schedule_interval=timedelta(days=7),  # Executar semanalmente
    start_date=datetime(2025, 4, 21),
    catchup=False,
    tags=['crawler', 'isic', 'dermatology'],
) as dag:

    # Tarefa para obter metadados
    get_metadata_task = PythonOperator(
        task_id='get_image_metadata',
        python_callable=get_image_metadata,
        provide_context=True,
    )
    
    # Tarefa para baixar imagens
    download_images_task = PythonOperator(
        task_id='download_images',
        python_callable=download_images,
        provide_context=True,
    )
    
    # Tarefa para salvar metadados em CSV
    save_metadata_task = PythonOperator(
        task_id='save_metadata_to_csv',
        python_callable=save_metadata_to_csv,
        provide_context=True,
    )
    
    # Definir a ordem das tarefas
    get_metadata_task >> [download_images_task, save_metadata_task]