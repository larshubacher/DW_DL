from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd
import time
from googleapiclient.discovery import build
from datetime import datetime
from datetime import timedelta

from youtube_etl import get_video_list
from youtube_etl import get_video_details

search_term = "Bitcoin"
api_key = "AIzaSyCrbo3YbznPw7nFYpx6Ru3Y7k__ZzdgaGo"
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
    schedule_interval = timedelta(days=1)
)


# def get_video_list(ti):
#     video_list = [1, 2, 3, 4]
#     print(video_list)
#     ti.xcom_push(key="video_list", value=video_list)
#
# def get_video_details():
#     video_list = ti.xcom_pull(key="video_list", task_ids="get_video_list")
#     json_string = json.dumps(stats_list)
#     print(json_string)


with dag:
    load_video_id = PythonOperator(
        task_id = "get_video_list",
        python_callable=get_video_list,
        provide_context=True,
        op_kwargs={'youtube': youtube}
    )

    get_video_details = PythonOperator(
        task_id="get_video_details",
        python_callable=get_video_details,
        provide_context=True,
        op_kwargs={'youtube': youtube}
    )

    load_video_id >> get_video_details