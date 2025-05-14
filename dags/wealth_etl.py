from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.transfers.s3_to_s3 import S3FileTransfer
from airflow.utils.dates import days_ago

import pandas as pd
import requests
import json
from datetime import datetime

def extract_weather_data():
    api_key = "your-openweather-api-key"  # Replace with your actual key
    city = "London"
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}"
    response = requests.get(url)
    return response.json()

def transform_weather_data(**context):
    weather_data = context['task_instance'].xcom_pull(task_ids='extract_weather')
    
    main = weather_data.get("main", {})
    coord = weather_data.get("coord", {})
    sys = weather_data.get("sys", {})
    weather = weather_data.get("weather", [{}])[0]

    flat_data = {
        "main_temp": main.get("temp"),
        "main_feels_like": main.get("feels_like"),
        "main_temp_min": main.get("temp_min"),
        "main_temp_max": main.get("temp_max"),
        "main_pressure": main.get("pressure"),
        "main_humidity": main.get("humidity"),
        "name": weather_data.get("name"),
        "coord_lon": coord.get("lon"),
        "coord_lat": coord.get("lat"),
        "weather_id": weather.get("id"),
        "weather_main": weather.get("main"),
        "weather_description": weather.get("description"),
        "weather_icon": weather.get("icon"),
        "sys_type": sys.get("type"),
        "sys_id": sys.get("id"),
        "sys_country": sys.get("country"),
        "sys_sunrise": sys.get("sunrise"),
        "sys_sunset": sys.get("sunset"),
        "retrieved_at": datetime.utcnow()
    }

    df = pd.DataFrame([flat_data])
    json_data = df.to_json(orient='records')
    
    # Save to a file for S3 upload
    with open('/tmp/weather_data.json', 'w') as f:
        f.write(json_data)

    return json_data

def load_to_postgres(**context):
    df_json = context['task_instance'].xcom_pull(task_ids='transform_weather')
    df = pd.read_json(df_json)
    
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    pg_hook.insert_rows(
        table='weather_data',
        rows=df.values.tolist(),
        target_fields=df.columns.tolist()
    )

def load_to_s3(**context):
    s3_hook = S3FileTransfer()
    s3_hook.load_file(
        filename='/tmp/weather_data.json',
        key=f'weather/{datetime.now().strftime("%Y-%m-%d")}/data.json',
        bucket_name='my-etl-bucket',
        replace=True
    )

with DAG(
    dag_id='weather_etl',
    schedule_interval='@hourly',
    start_date=days_ago(1),
    catchup=False,
    tags=['weather', 'etl']
) as dag:

    extract_task = PythonOperator(
        task_id='extract_weather',
        python_callable=extract_weather_data
    )

    transform_task = PythonOperator(
        task_id='transform_weather',
        python_callable=transform_weather_data,
        provide_context=True
    )

    load_postgres_task = PythonOperator(
        task_id='load_postgres',
        python_callable=load_to_postgres,
        provide_context=True
    )

    load_s3_task = PythonOperator(
        task_id='load_s3',
        python_callable=load_to_s3,
        provide_context=True
    )

    extract_task >> transform_task >> [load_postgres_task, load_s3_task]
