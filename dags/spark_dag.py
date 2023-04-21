import datetime
from airflow import models

from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

with models.DAG(dag_id="taxi_batch",
                start_date=datetime.datetime(2023, 4, 19),
                schedule_interval="0 1 * * *",
                default_args={
                        "owner": "BI",
                        "email_on_failure": False,
                },
                catchup=False, ) as dag:

    submit_job = SparkSubmitOperator(
        task_id='submit_job', 
        conn_id='spark',
        application="./dags/taxi_batch.py",
        application_args=["{{ds}}"]
    )
 