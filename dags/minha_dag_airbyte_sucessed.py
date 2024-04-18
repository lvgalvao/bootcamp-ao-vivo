from airflow.decorators import dag, task
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.models import Variable
import json
from datetime import datetime
from time import sleep

AIRBYTE_CONNECTION_ID = Variable.get("AIRBYTE_GOOGLE_POSTGRES_CONNECTION_ID")
API_KEY = f'Bearer {Variable.get("AIRBYTE_API_TOKEN")}'

@dag(start_date=datetime(2024, 4, 18), schedule_interval="@daily", catchup=False)
def running_airbyte_top():

    @task
    def esperar():
        sleep(160)

    start_airbyte_sync = SimpleHttpOperator(
        task_id='start_airbyte_sync',
        http_conn_id='airbyte',
        endpoint='/v1/jobs',  # api/v1/connections/sync Endpoint correto para disparar a sincronização
        method='POST',
        headers={
            "Content-Type": "application/json",
            "User-Agent": "fake-useragent",
            "Accept": "application/json",
            "Authorization": API_KEY
        },
        data=json.dumps({"connectionId": AIRBYTE_CONNECTION_ID, "jobType": "sync"}),  # Assegure que o connectionId está correto
        response_check=lambda response: response.json()['status'] == 'running'
    )

    t1 = esperar()
    # Define task dependencies
    start_airbyte_sync >> t1 >> check_if_done

dag_instance = running_airbyte_top()
