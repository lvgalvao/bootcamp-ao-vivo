from datetime import datetime
from airflow.decorators import task, dag

@dag(start_date=datetime(2024, 4, 1), catchup=False)
def dbt_pipeline():
    
    @task.bash
    def running() -> str:
        return 'cd include/bootcamp && dbt run'
    
    @task.bash
    def desculpe_marc() -> str:
        return 'echo "Nos desculpe Marc"'
    
    t1 = running()
    t2 = desculpe_marc()
    t1 >> t2

dbt_pipeline()