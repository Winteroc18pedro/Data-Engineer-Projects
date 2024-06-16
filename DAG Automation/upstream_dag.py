# -*- coding: utf-8 -*-
"""
Created on Tue May  7 12:24:49 2024

@author: NB30006
"""

import json
import os

from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator

from airflow import DAG
from airflow.contrib.operators.gcp_sql_operator import CloudSqlInstanceExportOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.gcs_to_gcs import GoogleCloudStorageToGoogleCloudStorageOperator
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.models import Variable
from datetime import datetime, timedelta

from airflow.operators.python_operator import PythonOperator
from google.cloud import storage

from airflow.operators.dummy_operator import DummyOperator

args = {
    'owner': 'packt-developer',
}

def read_json_schema(gcs_file_path):
    with open(gcs_file_path, "r") as file:
        schema_json = json.load(file)

    return schema_json
#The code above reads files that are in a JSON format.

# Environment Variables
gcp_project_id = os.environ.get('GCP_PROJECT')
instance_name  = os.environ.get('MYSQL_INSTANCE_NAME')

# Airflow Variables
settings = Variable.get("level_3_dag_settings", deserialize_json=True)

# DAG Variables
gcs_source_data_bucket = settings['gcs_source_data_bucket']
bq_raw_dataset = settings['bq_raw_dataset']
bq_dwh_dataset = settings['bq_dwh_dataset']

# Macros
extracted_date = '{{ ds }}'
extracted_date_nodash  = '{{ ds_nodash }}'

# Stations   Data extracted from a database in CloudSQL
station_source_object = f"chapter-4/stations/stations.csv"
sql_query = "SELECT * FROM apps_db.stations"  
#There is a database in CloudSQL called apps_db with the stations dataset.

export_body = {
    "exportContext": {
        "fileType": "csv",
        "uri": f"""gs://{gcs_source_data_bucket}/{station_source_object}""",
        "csvExportOptions":{
            "selectQuery": sql_query
        }
    }
} #It will export 

bq_stations_table_name = "stations"
bq_stations_table_id = f"{gcp_project_id}.{bq_raw_dataset}.{bq_stations_table_name}"
bq_stations_table_schema = read_json_schema("/home/airflow/gcs/data/schema/stations_schema.json")

# Regions  -   Data extracted from GCS Bucket
gcs_regions_source_object = "from-git/chapter-3/dataset/regions/regions.csv"
gcs_regions_target_object = f"chapter-4/regions/{extracted_date_nodash}/regions.csv"
bq_regions_table_name = "regions"
bq_regions_table_id = f"{gcp_project_id}.{bq_raw_dataset}.{bq_regions_table_name}"
bq_regions_table_schema = read_json_schema("/home/airflow/gcs/data/schema/regions_schema.json")

# Trips  Data extracted from BigQuery
bq_temporary_extract_dataset_name = "temporary_staging"
bq_temporary_extract_table_name = "trips"
bq_temporary_table_id = f"{gcp_project_id}.{bq_temporary_extract_dataset_name}.{bq_temporary_extract_table_name}"#_{extracted_date_nodash}"

gcs_trips_source_object = f"chapter-4/trips/{extracted_date_nodash}/*.csv"
gcs_trips_source_uri=f"gs://{gcs_source_data_bucket}/{gcs_trips_source_object}"

bq_trips_table_name = "trips"
bq_trips_table_id = f"{gcp_project_id}.{bq_raw_dataset}.{bq_trips_table_name}"
bq_trips_table_schema = read_json_schema("/home/airflow/gcs/data/schema/trips_schema.json")

# DWH
bq_fact_trips_daily_table_name = "facts_trips_daily"
bq_fact_trips_daily_table_id = f"{gcp_project_id}.{bq_dwh_dataset}.{bq_fact_trips_daily_table_name}"#${extracted_date_nodash}"

bq_dim_stations_table_name = "dim_stations"
bq_dim_stations_table_id = f"{gcp_project_id}.{bq_dwh_dataset}.{bq_dim_stations_table_name}"

#buckettttttttt
empty_bucket_source_object = "from-git/chapter-4/data/staging/level_5_dag_sensor/"

def create_empty_folders(date): 
    empty_destination = f"{empty_bucket_source_object}{date}/"
    client = storage.Client()     #Here I am initializing the Google Cloud Storage Client to be able to create folders in the storage
    bucket = client.bucket(gcs_source_data_bucket)  #Here I am getting the bucket in which I want to create empty folders
    folder_name = empty_destination
    blob = bucket.blob(folder_name)
    blob.upload_from_string('') #upload an empty string to the blob to create the folder
#This function creates empty folders which will be used to signal the end of a DAG run

with DAG(
    dag_id='level_5_dag_sensor',
    default_args=args,
    schedule_interval='0 5 * * *',
    start_date=datetime(2018, 1, 1),
    end_date=datetime(2018, 1, 3)
) as dag:

    ### Extract Station Table from a database in CloudSQL ###
    export_mysql_station = CloudSqlInstanceExportOperator(
        task_id='export_mysql_station',
        project_id=gcp_project_id,
        body=export_body,
        instance=instance_name
    )
    ### Load Station Table from the bucket to BigQuery ###
    gcs_to_bq_station = GoogleCloudStorageToBigQueryOperator(
        task_id = "gcs_to_bq_station",
        bucket = gcs_source_data_bucket,
        source_objects = [station_source_object],
        destination_project_dataset_table = bq_stations_table_id,
        schema_fields = bq_stations_table_schema,
        write_disposition = 'WRITE_TRUNCATE'
    )

    ### Extract Region Table from bucket in Google Cloud Storage to the same ###
    ### bucket (but it could be a different one) but in a different folder ###
    ###As it can be seen from the code below, the source bucket and destination ###
    ###bucket are the same, however the source and destination object are not ###
    gcs_to_gcs_region = GoogleCloudStorageToGoogleCloudStorageOperator(
        task_id = 'gcs_to_gcs_region',
        source_bucket = gcs_source_data_bucket,
        source_object = gcs_regions_source_object,
        destination_bucket = gcs_source_data_bucket,
        destination_object = gcs_regions_target_object
    )
    #$Load Region Table from that new location inside the Google Cloud Storage to BigQuery###
    gcs_to_bq_region = GoogleCloudStorageToBigQueryOperator(
    task_id = "gcs_to_bq_region",
    bucket = gcs_source_data_bucket,
    source_objects = [gcs_regions_target_object],
    destination_project_dataset_table = bq_regions_table_id,
    schema_fields = bq_regions_table_schema,
    write_disposition ='WRITE_TRUNCATE'
    )

    ### Creation of a temporary Trips Table in BigQuery ###
    ###This table will be seen as an external database ###
    bq_to_bq_temporary_trips = BigQueryOperator(
    task_id='bq_to_bq_temporary_trips',
    sql=f"""
        SELECT * FROM `bigquery-public-data-418317.raw_bikesharing.trips`
        WHERE DATE(start_date) = DATE('{extracted_date}')
        """,
    use_legacy_sql=False,
    destination_dataset_table=bq_temporary_table_id,
    write_disposition='WRITE_APPEND',
    create_disposition='CREATE_IF_NEEDED')
    
    ### Extract the temporary table from BigQuery to Google Cloud Storage ###
    bq_to_gcs_extract_trips = BigQueryToCloudStorageOperator(
    task_id='bq_to_gcs_extract_trips',
    source_project_dataset_table=bq_temporary_table_id,
    destination_cloud_storage_uris=[gcs_trips_source_uri],
    print_header=False,
    export_format='CSV',
    force_rerun=True)
    
    ### Load the Trips Table from Google Cloud Storage to BigQuery
    gcs_to_bq_trips = GoogleCloudStorageToBigQueryOperator(
    task_id = "gcs_to_bq_trips",
    bucket = gcs_source_data_bucket,
    source_objects = [gcs_trips_source_object],
    destination_project_dataset_table = bq_trips_table_id,## + f"${extracted_date_nodash}",
    schema_fields = bq_trips_table_schema,
    #time_partitioning = {'time_partitioning_type':'DAY','field': 'start_date'},
    write_disposition ='WRITE_APPEND'
    )
    
    ###With this code, I have all the tables in the same place both in BigQuery and ###
    ###in the Google Cloud Storage ###
###############Creating Partitioned tables####################
    partitioned_table_name = "partitioned_table"
    partitioned_table_id = f"{gcp_project_id}.{bq_temporary_extract_dataset_name}.{partitioned_table_name}"

    create_partitioned_table_task = BigQueryExecuteQueryOperator(
    task_id=f'create_partitioned_table', #####_{partition_suffix}
    sql=f"""
            CREATE OR REPLACE TABLE `{partitioned_table_id}`(start_date TIMESTAMP, start_station_name STRING)
            PARTITION BY DATE(start_date)
            AS (
            SELECT start_date, start_station_name
            FROM `{bq_temporary_table_id}`)
            """,
    use_legacy_sql=False,
    )

    transfer_data_task = BigQueryExecuteQueryOperator(
    task_id=f'transfer_data', ##_{partition_suffix}',
    sql=f"""INSERT INTO `{partitioned_table_id}`
            SELECT start_date, start_station_name
            FROM `{bq_temporary_table_id}`
            """,
    use_legacy_sql=False,
    )
####################################################################################################

    ### Load table with numerical data (fact table) ###
    dwh_fact_trips_daily  = BigQueryOperator(
        task_id = "dwh_fact_trips_daily",
        sql = f"""SELECT DATE(start_date) as trip_date,
        start_station_id,
        COUNT(trip_id) as total_trips,
        SUM(duration_sec) as sum_duration_sec,
        AVG(duration_sec) as avg_duration_sec
        FROM `{bq_trips_table_id}`
        WHERE DATE(start_date) = DATE('{extracted_date}')
        GROUP BY trip_date, start_station_id""",
        destination_dataset_table = bq_fact_trips_daily_table_id,
        write_disposition = 'WRITE_TRUNCATE',
        #time_partitioning = {'time_partitioning_type':'DAY','field': 'trip_date'},
        create_disposition = 'CREATE_IF_NEEDED',
        use_legacy_sql = False,
        priority = 'BATCH'
    )
    ### Load data with background information and context (dimension table)###
    dwh_dim_stations  = BigQueryOperator(
        task_id = "dwh_dim_stations",
        sql = f"""SELECT station_id,
        stations.name AS station_name,
        regions.name AS region_name,
        capacity
        FROM `{bq_stations_table_id}` AS stations
        JOIN `{bq_regions_table_id}` AS regions
        ON stations.region_id = CAST(regions.region_id AS STRING)
        ;""",
        destination_dataset_table = bq_dim_stations_table_id,
        write_disposition = 'WRITE_TRUNCATE',
        create_disposition = 'CREATE_IF_NEEDED',
        use_legacy_sql = False,
        priority = 'BATCH'
    )

    ### BigQuery Row Count Checker - it checks if the tables above were indeed created. ###
    bq_row_count_check_dwh_fact_trips_daily = BigQueryCheckOperator(
    task_id='bq_row_count_check_dwh_fact_trips_daily',
    sql=f"""
    SELECT count(*) FROM `{gcp_project_id}.{bq_dwh_dataset}.{bq_fact_trips_daily_table_name}`
    WHERE trip_date = DATE('{extracted_date}')
    """,
    use_legacy_sql=False)

    bq_row_count_check_dwh_dim_stations = BigQueryCheckOperator(
    task_id='bq_row_count_check_dwh_dim_stations',
    sql=f"""
    SELECT count(*) FROM `{bq_dim_stations_table_id}`
    """,
    use_legacy_sql=False)
    
    ###A task that implements the functions that was created above ###
    create_empty_folders_task = PythonOperator(
        task_id="create_empty_folders_task",
        python_callable=create_empty_folders,
        op_kwargs={"date":extracted_date_nodash}
    )
    ###When the DAG runs completely and successfully, it sends a signal to that ###
    ###directory which will be sensored by upstream DAGs.
    send_dag_success_signal = GoogleCloudStorageToGoogleCloudStorageOperator(
        task_id='send_dag_success_signal',
        source_bucket=gcs_source_data_bucket,
        source_object=f"from-git/chapter-4/data/signal/_SUCCESS",
        destination_bucket = gcs_source_data_bucket,
        #destination_object=f'chapter-4/data/signal/staging/level_5_dag_sensor/{extracted_date_nodash}/_SUCCESSS'
        destination_object = f"{empty_bucket_source_object}{extracted_date_nodash}/_SUCCESS"
    )



    ### Load Data Mart - Task dependencies using the  bitshift operator ###
    ###It is common practice to have the task dependecies at the end of the DAG ###
    
    export_mysql_station >> gcs_to_bq_station
    gcs_to_gcs_region >> gcs_to_bq_region
    bq_to_bq_temporary_trips >> bq_to_gcs_extract_trips >> gcs_to_bq_trips >> create_partitioned_table_task >> transfer_data_task  >> create_empty_folders_task

    [gcs_to_bq_station,gcs_to_bq_region,gcs_to_bq_trips] >> dwh_fact_trips_daily >> bq_row_count_check_dwh_fact_trips_daily
    [gcs_to_bq_station,gcs_to_bq_region,gcs_to_bq_trips] >> dwh_dim_stations >> bq_row_count_check_dwh_dim_stations

    [bq_row_count_check_dwh_fact_trips_daily,bq_row_count_check_dwh_dim_stations]  >> send_dag_success_signal


###Initialize the DAG ###
if __name__ == "__main__":    
    dag.cli()
