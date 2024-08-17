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
from datetime import datetime


bash_command = """
python /home/maksym/Documents/airflow-docker/external_scripts/whisper_transcription.py
"""

with DAG("whisper_transcription_dag", 
         start_date = datetime(2024, 8, 13), 
         schedule_interval = "@continuous", 
         catchup = False, 
         max_active_runs=1 # doesn't run, if the most recent run is not finished
         ) as dag:
    
    run_local_script = BashOperator(
        task_id = "whisper_transcription_task",
        bash_command = bash_command
    )