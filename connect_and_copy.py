import logging
import shutil
import time
from datetime import datetime
from pprint import pprint

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python_operator import PythonOperator

import boto3

log = logging.getLogger(__name__)


dag = DAG(
    dag_id='hw-33',
    schedule_interval='* */5 * * *',
    start_date=datetime.now(),
    tags=['hw'],

)

def connect_and_copy():
    src_bucket = 'hw-fraud-data'
    dst_bucket = 'hw-3-bucket'
    session = boto3.session.Session()
    s3 = session.client(service_name='s3',
                    endpoint_url='https://storage.yandexcloud.net')

    hw_fraud_data_objs = []
    hw_3_bucket_objs = []

    contents_src = s3.list_objects(Bucket=src_bucket)
    if 'Contents' in contents_src.keys():
        for key in contents_src['Contents']:
            hw_fraud_data_objs.append(key['Key'])

    contents_dst = s3.list_objects(Bucket=dst_bucket)
    if 'Contents' in contents_dst.keys():
        for key in contents_dst['Contents']:
            hw_3_bucket_objs.append(key['Key'])
    
    not_copied = [obj for obj in hw_fraud_data_objs if obj not in hw_3_bucket_objs]
    if len(not_copied):
        copy_source = {'Bucket': src_bucket,
                        'Key': not_copied[0]}
        s3.copy(copy_source, dst_bucket, not_copied[0])

conn_and_copy = PythonOperator(task_id='conn_and_copy', python_callable=connect_and_copy, dag=dag)

print('Copy objects...')
conn_and_copy
