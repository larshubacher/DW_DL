from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd
import time
from googleapiclient.discovery import build
from datetime import datetime
from datetime import timedelta

#from youtube_etl import get_video_list
#from youtube_etl import get_video_details

default_args = {
    "owner" : "airflow",
    "depends_on_past": False,
    "retries": 1, ## how many retries if it fails
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2021, 11, 27),
}

dag = DAG(
    dag_id="youtube_dag",
    default_args=default_args,
    description="First DAG with ETL process",
    schedule_interval = timedelta(days=1)
)


def get_video_list():
    video_list = [1, 2, 3, 4]
    print(video_list)

def get_video_details():
    df = pd.DataFrame(video_list)


with dag:
    task_a = PythonOperator(
        task_id = "get_video_list",
        python_callable=get_video_list,
        provide_context=True,
    )

    task_b = PythonOperator(
        task_id="get_video_details",
        python_callable=get_video_details,
        provide_context=True,
    )

task_a >> task_b