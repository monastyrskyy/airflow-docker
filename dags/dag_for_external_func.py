from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from random import randint
from scripts.hello_world import hello_world_func



with DAG("dag_for_external_func", 
         start_date = datetime(2024, 8, 13), 
         schedule_interval = "* * * * *", catchup = False) as dag:
    
    print_hello = PythonOperator(
        task_id = "printing_hello",
        python_callable = hello_world_func
    )