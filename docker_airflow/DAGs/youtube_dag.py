from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd
import time
from googleapiclient.discovery import build
from datetime import datetime
from datetime import timedelta

from youtube_etl import get_video_list
from youtube_etl import get_video_details
from cred import api_key


search_term = "Bitcoin"
api_key = api_key
youtube = build("youtube", "v3", developerKey=api_key)
number_of_videos = 50


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1, ## how many retries if it fails
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2021, 11, 27)
}

dag = DAG(
    dag_id="youtube_dag",
    default_args=default_args,
    description="First DAG with ETL process",
    schedule_interval = "@daily"
)



#with dag:
get_video_list = PythonOperator(
    task_id = "get_video_list",
    python_callable=get_video_list,
    provide_context=True,
    dag = dag
)

get_video_details = PythonOperator(
    task_id="get_video_details",
    python_callable=get_video_details,
    provide_context=True,
    dag = dag
)

get_video_list >> get_video_details