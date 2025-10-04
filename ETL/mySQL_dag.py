# Подключаем библиотеки для работы с Airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Подключаем вспомогательные библиотеки
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import clickhouse_connect
import pandas as pd
import os

CONFIG_CLIKHOUSE = {
    "secret_host": os.getenv("CLICKHOUSE_HOST"),
    "secret_port": os.getenv("CLICKHOUSE_PORT"),
    "secret_user": os.getenv("CLICKHOUSE_USER"),
    "secret_password" os.getenv("CLICKHOUSE_PASSWORD"):
}

mysql_secret_url = os.getenv("MYSQL_URL")

default_args = {
    'owner': 'me',
    'start_date': datetime(2025, 9, 27),
    'end_date': datetime(2025, 11, 1),
    'max_active_runs': 1, # За раз отработает только один DAG
}

dag = DAG(
    'user_payments',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
)

# Допишите недостающие функции здесь
def download_object_from_mysql(**context):
    engine = create_engine(mysql_secret_url)
    data = pd.read_sql(f"SELECT * FROM ya_sample_store.user_payments WHERE date = '{context['ds']}'", engine)
    data.to_csv(f'/tmp/{context["ds"]}-user_payments_downloaded_file.csv', index=False)

# Адрес MySQL yoga shop

def load_object_from_mysql_to_clickhouse(**context):
    df = pd.read_csv(f'/tmp/{context["ds"]}-user_payments_downloaded_file.csv')

    client  = clickhouse_connect.get_client(
        host=secret_host, 
        port=secret_port, 
        user=secret_user, 
        password=secret_password,
        verify=False
        )

    client.insert_df('tmp.user_payments', df)

def etl_inside_clickhouse():

    client  = clickhouse_connect.get_client(
        host=secret_host, 
        port=secret_port, 
        user=secret_user, 
        password=secret_password,
        verify=False
        )

    client.command("""
        INSERT INTO raw.user_payments
        SELECT 
        toDateTime(date) as date,
        toDateTime(timestamp),
        user_client_id,
        item,
        price,
        quantity,
        amount,
        discount,
        order_id,
        status, 
        now() as insert_time,
        cityHash64(*) as hash
        FROM tmp.user_payments
        """)
        
    client.command("TRUNCATE TABLE tmp.user_payments ") # Очистка временной таблицы

def remove_tmp_file(**context):
  os.remove(f'/tmp/{context["ds"]}-user_payments_downloaded_file.csv')

download_object_from_mysql = PythonOperator(
    task_id="download_object_from_mysql",
    python_callable=download_object_from_mysql,
    dag=dag,
)

load_object_from_mysql_to_clickhouse= PythonOperator(
    task_id="load_object_from_mysql_to_clickhouse",
    python_callable=load_object_from_mysql_to_clickhouse,
    dag=dag,
)

etl_inside_clickhouse= PythonOperator(
    task_id="etl_inside_clickhouse",
    python_callable=etl_inside_clickhouse,
    dag=dag,
)

remove_tmp_file= PythonOperator(
    task_id="remove_tmp_file",
    python_callable=remove_tmp_file,
    dag=dag,
)

download_object_from_mysql >> load_object_from_mysql_to_clickhouse >> etl_inside_clickhouse >> remove_tmp_file