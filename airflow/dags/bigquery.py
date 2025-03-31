import os
import json
from pathlib import Path
from datetime import datetime, timedelta
from google_play_scraper import reviews, Sort
from google.cloud import storage

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 3, 30),
    'end_date': datetime(2025, 4, 5),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="ELT_google_playstore_review",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
    tags=['google_playstore', 'ELT']
) as dag:
    
    def get_reviews():
        result, _ = reviews(
            'com.whatsapp',
            sort=Sort.NEWEST,
            lang='id',
            country='id',
            count=50,
            filter_score_with=None
        )
        
        for review in result:
            review["at"] = review['at'].isoformat()
        
        folder = "/opt/airflow/dags"
        path = os.path.join(folder, 'review.json')
        
        with open(path, "w", encoding="utf-8") as file:
            for review in result:
                file.write(json.dumps(review) + "\n")
        
        return path
    
    get_review = PythonOperator(
        task_id='get_reviews',
        python_callable=get_reviews)
    
    def local_to_gcs():

        PATH = "/opt/airflow/dags/core-sprite-407216-e97256ea7324.json"
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = PATH

        storage_client = storage.Client()
        bucket = storage_client.bucket('bucketproject-99')

        file = "/opt/airflow/dags/review.json"  
        file_name = os.path.basename(file)

        blob = bucket.blob(f'google_review/{file_name}')
        blob.upload_from_filename(file)

    local_to_gcs = PythonOperator(
        task_id='local_to_gcs',
        python_callable= local_to_gcs)
    
    create_external_table = BigQueryCreateExternalTableOperator(
        task_id="create_external_table",
        table_resource={
            "tableReference": {
                "projectId": "core-sprite-407216",
                "datasetId": "review_google_playstore",
                "tableId": "google_review_result"
            },
            "schema": {
                "fields": [
                    {'name': 'reviewId', 'type': 'STRING', 'mode': 'NULLABLE'},
                    {'name': 'userName', 'type': 'STRING', 'mode': 'NULLABLE'},
                    {'name': 'userImage', 'type': 'STRING', 'mode': 'NULLABLE'},
                    {'name': 'content', 'type': 'STRING', 'mode': 'NULLABLE'},
                    {'name': 'score', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                    {'name': 'thumbsUpCount', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                    {'name': 'reviewCreatedVersion', 'type': 'STRING', 'mode': 'NULLABLE'},
                    {'name': 'at', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
                    {'name': 'replyContent', 'type': 'STRING', 'mode': 'NULLABLE'},
                    {'name': 'repliedAt', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'}
                ]
            },
            "externalDataConfiguration": {
                "sourceUris": ["gs://bucketproject-99/google_review/review.json"],
                "sourceFormat": "NEWLINE_DELIMITED_JSON",
                "autodetect": True
            }
        },
    )

    #create_external_table = BigQueryCreateExternalTableOperator(
    #    task_id="create_external_table",
    #    destination_project_dataset_table="core-sprite-407216.review_google_playstore.google_review_result",
    #    bucket="bucketproject-99",
    #    source_objects=["google_review/review.json"],
    #    table_resource={
    #    "tableReference": {
    #        "projectId": "core-sprite-407216",
    #        "datasetId": "review_google_playstore",
    #        "tableId": "google_review_result"
    #    },
    #    "schema":{
    #        "fields":[
    #        {'name': 'reviewId', 'type': 'STRING', 'mode': 'NULLABLE'},
    #        {'name': 'userName', 'type': 'STRING', 'mode': 'NULLABLE'},
    #        {'name': 'userImage', 'type': 'STRING', 'mode': 'NULLABLE'},
    #        {'name': 'content', 'type': 'STRING', 'mode': 'NULLABLE'},
    #        {'name': 'score', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    #        {'name': 'thumbsUpCount', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    #        {'name': 'reviewCreatedVersion', 'type': 'STRING', 'mode': 'NULLABLE'},
    #        {'name': 'at', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
    #        {'name': 'replyContent', 'type': 'STRING', 'mode': 'NULLABLE'},
    #        {'name': 'repliedAt', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'}],
    #    }},
    #    source_format="NEWLINE_DELIMITED_JSON",
    #    autodetect=True,
    #    )
#
    dbt_datamart = BashOperator(
        task_id="datamarts",
        bash_command= 'dbt run --select datamart --profiles-dir /opt/airflow/dags --project-dir /opt/airflow/dags')
    
    dbt_table_transform = BashOperator(
        task_id="transform_table",
        bash_command= 'dbt run --select core_transform --profiles-dir /opt/airflow/dags --project-dir /opt/airflow/dags')
    
    dbt_newdata = BashOperator(
        task_id="view_newdata",
        bash_command= 'dbt run --select view_newdata --profiles-dir /opt/airflow/dags --project-dir /opt/airflow/dags')
        
    dbt_score = BashOperator(
        task_id="view_score",
        bash_command= 'dbt run --select stg_review --profiles-dir /opt/airflow/dags --project-dir /opt/airflow/dags')
    
    get_review >> local_to_gcs >> create_external_table >> dbt_datamart >> [dbt_table_transform,  dbt_newdata, dbt_score]
    