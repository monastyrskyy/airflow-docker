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
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import subprocess
from dotenv import load_dotenv
import os


# Function to check internet connection
def check_internet_connection():
    response = os.system("ping -c 1 google.com")
    return response == 0

# Function to commit and push changes to Git repository
def commit_and_push():
    repo_dir = "/home/maksym/Documents/whisper/files/azure/nouns_and_frequencies"
    os.chdir(repo_dir)

    # Load environment variables from .env file
    load_dotenv("/home/maksym/Documents/airflow-docker/.env")
    github_username = os.environ["GITEmail"]
    github_token = os.environ["GITToken"]

    if not github_username or not github_token:
        print("GitHub credentials are not set in environment variables.")
        return

    # Set Git user configuration (fix for missing user identity)
    subprocess.run(["git", "config", "--global", "user.email", github_username], check=True)
    subprocess.run(["git", "config", "--global", "user.name", "Bruh Automatic"], check=True)


    # Check if nouns.csv exists
    if os.path.isfile("/home/maksym/Documents/whisper/files/azure/nouns_and_frequencies/nouns.csv"):
        try:
            # Add nouns.csv to staging
            add_output = subprocess.check_output(["git", "add", "nouns.csv"], stderr=subprocess.STDOUT)
            print(add_output.decode())
            add_output = subprocess.check_output(["git", "add", "episodes_table.csv"], stderr=subprocess.STDOUT)
            print(add_output.decode())
            add_output = subprocess.check_output(["git", "add", "phrases.csv"], stderr=subprocess.STDOUT)
            print(add_output.decode())
            add_output = subprocess.check_output(["git", "add", "verbs.csv"], stderr=subprocess.STDOUT)
            print(add_output.decode())
            add_output = subprocess.check_output(["git", "add", "weights_and_calcs.csv"], stderr=subprocess.STDOUT)
            print(add_output.decode())

            # Commit changes
            commit_output = subprocess.check_output(["git", "commit", "-m", "Daily backup"], stderr=subprocess.STDOUT)
            print(commit_output.decode())

            # Push changes to the GitHub repository using the username and token
            push_output = subprocess.check_output(
                ["git", "push", f"https://{github_token}@github.com/monastyrskyy/nouns_and_frequencies.git"],
                stderr=subprocess.STDOUT
            )
            print(push_output.decode())

        except subprocess.CalledProcessError as e:
            print(f"Command '{e.cmd}' failed with return code {e.returncode}")
            print(f"Output: {e.output.decode()}")
    else:
        print("nouns.csv does not exist.")


# Bash command to run the local Python script
bash_command = """
python /home/maksym/Documents/airflow-docker/external_scripts/vocab_local_backup.py
"""

with DAG("vocab_local_backup_dag", 
    start_date = datetime(2024, 8, 26),
    schedule_interval = "0 13 * * *",
    catchup = False,
    max_active_runs=1 # doesn't run, if the most recent run is not finished
) as dag:
    
    # Task 1: Check Internet Connection
    check_internet = PythonOperator(
        task_id='check_internet_connection',
        python_callable=check_internet_connection,
        retries=50,
        retry_delay=timedelta(hours=5),
    )

    # Task 2: Run Local Script
    run_local_script = BashOperator(
        task_id = "vocab_local_backup_task",
        bash_command = bash_command
    )

    # Task 3: Commit and Push Changes to Git
    commit_push = PythonOperator(
        task_id='commit_and_push',
        python_callable=commit_and_push,
    )

    # Task dependencies: Internet check -> Run script -> Commit and push
    check_internet >> run_local_script >> commit_push