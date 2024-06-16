# -*- coding: utf-8 -*-
"""
Created on Tue May  7 15:38:10 2024

@author: NB30006
"""

import os

from airflow import DAG
from airflow.contrib.operators.gcs_to_gcs import GoogleCloudStorageToGoogleCloudStorageOperator
from airflow.contrib.sensors.gcs_sensor import GoogleCloudStorageObjectSensor
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.models import Variable
from datetime import datetime

args = {
    'owner': 'packt-developer',
}

# Environment Variables
gcp_project_id = os.environ.get('GCP_PROJECT')

# Airflow Variables
settings = Variable.get("level_3_dag_settings", deserialize_json=True)
gcs_source_data_bucket = settings['gcs_source_data_bucket']
bq_dwh_dataset = settings['bq_dwh_dataset']

# DAG Variables
bq_datamart_dataset = 'dm_bikesharing'
parent_dag = 'level_5_dag_sensor'

bq_fact_trips_daily_table_name = "bike_trips_daily"
bq_fact_trips_daily_table_id = f"{gcp_project_id}.{bq_datamart_dataset}.{bq_fact_trips_daily_table_name}"
sum_total_trips_table_id = f'{gcp_project_id}.{bq_datamart_dataset}.sum_total_trips_daily'

# Macros
extracted_date_nodash = '{{ ds_nodash }}'
execution_date = '{{ ds }}'

#Empty object in a specific bucket to read the signal from
empty_bucket_source_object = "from-git/chapter-4/data/staging/level_5_dag_sensor/"
second_empty_bucket_source_object = "from-git/chapter-4/data/staging/level_5_downstream_dag/"

with DAG(
    dag_id='level_5_downstream_dag',
    default_args=args,
    schedule_interval='0 5 * * *',
    start_date=datetime(2018, 1, 1),
    end_date=datetime(2018, 1, 3)
) as dag:
    
    ###This sensor will keep checking if the _SUCCESS file exists in that directory###
    ###and it will keep doing so every 60 seconds ###
    sensor_task = GoogleCloudStorageObjectSensor(
        task_id='sensor_task',
        bucket=gcs_source_data_bucket,
        #object=f"from-git/chapter-4/data/signal/staging/{parent_dag}/{execution_date_nodash}/_SUCCESS",
        object=f"{empty_bucket_source_object}{extracted_date_nodash}/_SUCCESS",
        mode='poke',
        poke_interval=60,
        timeout=60 * 60 * 24 * 7
    )

    data_mart_sum_total_trips  = BigQueryOperator(
        task_id = "data_mart_sum_total_trips",
        sql = f"""SELECT trip_date,
        number_of_trips_daily
        FROM `{bq_fact_trips_daily_table_id}`
        WHERE trip_date = DATE('{execution_date}')""",
        destination_dataset_table = sum_total_trips_table_id,
        write_disposition = 'WRITE_TRUNCATE',
        time_partitioning= {'time_partitioning_type':'DAY','field': 'trip_date'},
        create_disposition = 'CREATE_IF_NEEDED',
        use_legacy_sql = False,
        priority = 'BATCH'
    )
    ###even if this is the last DAG, it is a common practice to add this signal at the end ###
    ###of every DAG :) ###
    send_dag_success_signal = GoogleCloudStorageToGoogleCloudStorageOperator(
        task_id='send_dag_success_signal',
        source_bucket=gcs_source_data_bucket,
        source_object=f'from-git/chapter-4/data/signal/_SUCCESS',
        destination_bucket=gcs_source_data_bucket,
        destination_object=f"{second_empty_bucket_source_object}{extracted_date_nodash}/_SUCCESS"
    )
    ### Load Data Mart - Task dependecies ###
    sensor_task >> data_mart_sum_total_trips >> send_dag_success_signal   #The reference here has to be the task_id

if __name__ == "__main__":
    dag.cli()