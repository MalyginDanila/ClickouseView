pip install pymysql==1.0.2
pip install SQLAlchemy==1.4.46
pip install boto3

import pymysql as MySQLdb
from sqlalchemy import create_engine
import pandas as pd

CONFIG = {
    "key_id": os.getenv("ACCESS_KEY_ID"),
    "secret": os.getenv("SECRET_ACCESS_KEY"),
    "endpoint": os.getenv("ENDPOINT"),
    "container": os.getenv("BUCKET"),
}

mysql_secret_url = os.getenv("MYSQL_URL")

engine = create_engine(mysql_secret_url)

with engine.connect() as conn:
    data = pd.read_sql('SELECT * FROM ya_sample_store.user_payments LIMIT 10', conn)

import boto3

# Подключение к хранилищу
s3 = boto3.client(
    's3',
    endpoint_url=endpoint,
    aws_access_key_id=key_id,
    aws_secret_access_key=secret,
    region_name='ru-central1'
)

# Выгрузка данных, лежащих в хранилище (первый аргумент), в локальный файл (второй аргумент)
bucket_name = 'yc-metrics-theme'
object_key  = '2022-06-30-site-visits.csv'
local_path  = '2022-06-30-site-visits.csv'

s3.download_file(bucket_name, object_key, local_path)

import pandas as pd
data = pd.read_csv('./2022-06-30-site-visits.csv')
data.head()
data.info()