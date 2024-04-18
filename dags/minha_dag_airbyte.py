from airflow.decorators import dag
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.models import Variable
import json
import datetime

AIRBYTE_CONNETCION_ID = Variable.get("AIRBYTE_GOOGLE_POSTGRES_CONNECTION_ID")
API_KEY = f'Bearer {Variable.get("AIRBYTE_API_TOKEN")}'


@dag(
    start_date=datetime(2024, 4, 18),
    schedule="@daily",
    catchup=False,
)
def running_airbyte():

    start_airbyte_sync = SimpleHttpOperator(
    task_id='start_airbyte_sync',
    http_conn_id='airbyte',
    endpoint='api/v1/connections/sync',  # Use the correct endpoint for triggering sync
    method='POST',
    headers={"Content-Type": "application/json", "Authorization": "Bearer {{ var.value.AIRBYTE_API_TOKEN }}"},
    data=json.dumps({"connectionId": "{{ var.value.AIRBYTE_GOOGLE_POSTGRES_CONNECTION_ID }}"}),  # Ensure the connection ID is correct
    response_check=lambda response: response.json()['job']['status'] == 'running',
    dag=dag
)
    start_airbyte_sync


running_airbyte()