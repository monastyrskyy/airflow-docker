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

# Command to activate the environment and run the script
# source /home/maksym/Documents/whisper/whisper-env/bin/activate && \
# past the above back into the bash command
bash_command = """
python /home/maksym/Documents/airflow-docker/external_scripts/whisper_draft_1.py
"""

with DAG("whisper_dag", 
         start_date = datetime(2024, 8, 13), 
         schedule_interval = "* * * * *", 
         catchup = False, 
         max_active_runs=1 # doesn't run, if the most recent run is not finished
         ) as dag:
    
    run_local_script = BashOperator(
        task_id = "run_local_script_with_env",
        bash_command = bash_command
    )