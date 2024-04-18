from airflow.decorators import dag, task
from airflow.providers.dbt.cloud.hooks.dbt import DbtCloudHook, DbtCloudJobRunStatus
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from pendulum import datetime

DBT_CLOUD_CONN_ID = "dbt-conn"
JOB_ID = "70403103919624"

@dag(
    start_date=datetime(2024, 4, 18),
    schedule="@daily",
    catchup=False,
)
def running_dbt_cloud():

    trigger_job = DbtCloudRunJobOperator(
        task_id="trigger_dbt_cloud_job",
        dbt_cloud_conn_id=DBT_CLOUD_CONN_ID,
        job_id=JOB_ID,
        check_interval=60,
        timeout=360,
    )

    trigger_job


running_dbt_cloud()