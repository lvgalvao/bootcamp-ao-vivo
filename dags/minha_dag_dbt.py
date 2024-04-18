from datetime import datetime
from airflow.decorators import task, dag

@dag(start_date=datetime(2023, 1, 1))
def dbt_pipeline():
    
    @task.bash
    def running():
        return "cd include/bootcamp && dbt run"
    
    @task.bash
    def desculpe_marc():
        return "echo 'Nos desculpe Marc'"
    
    t1 = running()
    t2 = running()
    t1 > t2
    
dbt_pipeline()