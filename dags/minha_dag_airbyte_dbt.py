from airflow.decorators import dag
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.models import Variable
import json
from datetime import datetime

AIRBYTE_CONNETCION_ID = Variable.get("AIRBYTE_GOOGLE_POSTGRES_CONNECTION_ID")
API_KEY = f'Bearer {Variable.get("AIRBYTE_API_TOKEN")}'


@dag(start_date=datetime(2024, 4, 18), schedule_interval="@daily", catchup=False)
def running_airbyte():

    start_airbyte_sync = SimpleHttpOperator(
        task_id='start_airbyte_sync',
        http_conn_id='airbyte',
        endpoint=f'/v1/jobs',  # api/v1/connections/sync Endpoint correto para disparar a sincronização
        method='POST',
        headers={"Content-Type": "application/json", 
                 "User-Agent":"fake-useragent", 
                 "Accept":"application/json",
                 "Authorization": API_KEY},
        data=json.dumps({"connectionId": AIRBYTE_CONNETCION_ID, "jobType":"sync"}),  # Assegure que o connectionId está correto
        response_check=lambda response: response.json()['status'] == 'running'
    )

    start_airbyte_sync


running_airbyte()