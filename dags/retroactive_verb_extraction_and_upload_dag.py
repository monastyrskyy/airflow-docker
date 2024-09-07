# ===================================================================================
#                      _____          _  _____                 
#                     |  __ \        | |/ ____|                
#                     | |__) |__   __| | (___  _   _ _ __ ___  
#                     |  ___/ _ \ / _` |\___ \| | | | '_ ` _ \ 
#                     | |  | (_) | (_| |____) | |_| | | | | | |
#                     |_|   \___/ \__,_|_____/ \__,_|_| |_| |_|
#                                                              
# ===================================================================================


# Libraries
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

bash_command = """
python /home/maksym/Documents/airflow-docker/external_scripts/retroactive_verb_extraction_and_upload.py
"""

with DAG("retroactive_verb_extraction_and_upload_dag", 
         start_date = datetime(2024, 9, 6), 
         schedule_interval = "@continuous", 
         catchup = False, 
         max_active_runs=1 # doesn't run, if the most recent run is not finished
         ) as dag:
    
    run_local_script = BashOperator(
        task_id = "retroactive_verb_extraction_and_upload_task",
        bash_command = bash_command,
        execution_timeout=timedelta(seconds=500)
    )