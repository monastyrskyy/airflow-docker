#!/bin/bash

# Specify the path to your Python environment and script
SCRIPT="/home/maksym/Documents/airflow-docker/external_scripts/whisper_transcription.py"
ENV_PATH="/home/maksym/Documents/whisper/whisper-env/bin/activate"
LOGDIR="/home/maksym/Documents/airflow-docker/crontab_logs"
LOGFILE="$LOGDIR/transcription_logfile_$(date +\%Y-\%m-\%d_\%H-\%M-\%S).log"

# Check if the script is already running
if pgrep -f $SCRIPT > /dev/null
then
    echo "A script is already running."
    exit 1
else
    echo "Script is not running. Starting now..."
    # Activate the environment
    source $ENV_PATH
    # Run the Python script and logs only the non-skipped runs.
    python $SCRIPT >> "$LOGFILE" 2>&1
fi
