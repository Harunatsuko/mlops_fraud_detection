import airflow
from datetime import timedelta, datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import *
from pyspark.sql.functions import col,avg,sum,min,max,row_number, lag
from pyspark.sql.window import Window

import boto3

filepath = 'temp.csv'

dag_spark = DAG(
        dag_id = "hw4",
        schedule_interval='* */5 * * *',
        start_date = datetime.now()
)

def preprocess():

    columns = ['transaction_id',
           'tx_datetime',
           'customer_id',
           'terminal_id',
           'tx_amount',
           'tx_time_seconds',
           'tx_time_days',
           'tx_fraud',
           'tx_fraud_scenario']

    ss = SparkSession.builder.appName("test").getOrCreate()
    schema = StructType([StructField(columns[0], IntegerType()),
                        StructField(columns[1], DateType()),
                        StructField(columns[2], IntegerType()),
                        StructField(columns[3], IntegerType()),
                        StructField(columns[4], FloatType()),
                        StructField(columns[5], IntegerType()),
                        StructField(columns[6], IntegerType()),
                        StructField(columns[7], IntegerType()),
                        StructField(columns[8], IntegerType())])

    data=ss.read.option('mode','DROPMALFORMED').csv('/user/temp.csv', schema)

    windowSpec  = Window.partitionBy("customer_id").orderBy("transaction_id")
    df_lag = data[['customer_id', 'transaction_id', 'tx_amount']] \
                    .withColumn("lag_2",lag("tx_amount",2).over(windowSpec)) \
                    .withColumn("lag_3",lag("tx_amount",3).over(windowSpec))
    df_lag = df_lag.drop('tx_amount')
    data = data.join(df_lag, on=['customer_id', 'transaction_id'])
    colsToDrop = ['tx_time_seconds',
                   'tx_time_days']
    data = data.drop(*colsToDrop)
    data.write.mode("overwrite").parquet('/user/temp.parquet')

def upload_from_s3():
    src_bucket = 'hw-fraud-data'
    dst_bucket = 'hw-3-bucket'
    session = boto3.session.Session()
    s3 = session.client(service_name='s3',
                    endpoint_url='https://storage.yandexcloud.net')

    hw_fraud_data_objs = []

    contents_src = s3.list_objects(Bucket=src_bucket)
    if 'Contents' in contents_src.keys():
        for key in contents_src['Contents']:
            hw_fraud_data_objs.append(key['Key'])

    if len(hw_fraud_data_objs):
        s3.download_file(src_bucket, hw_fraud_data_objs[0], filepath)

print("upload data from s3 to local")
upload_from_s3 = PythonOperator(
    task_id="upload_from_s3",
    python_callable = upload_from_s3,
    dag=dag_spark)

print("copying from local to hdfs directory")
copy_from_local = BashOperator(
    task_id="copy_from_local",
    bash_command='hdfs dfs -copyFromLocal -f /home/ubuntu/airflow/dags/temp.csv /user/temp.csv',
    dag=dag_spark
            )
print('spark submit task to hdfs')
spark_submit_local = PythonOperator(
    task_id='spark_task',
    python_callable = preprocess,
    dag=dag_spark
    )

upload_from_s3 >> copy_from_local >> spark_submit_local

if __name__ == '__main__ ':
    dag_spark.cli()
