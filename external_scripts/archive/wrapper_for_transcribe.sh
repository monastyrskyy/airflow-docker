#!/bin/bash

# Define the path to the environment and the main script
ENV_PATH="/home/maksym/Documents/whisper/whisper-env/bin/activate"
SCRIPT_PATH="/home/maksym/Documents/airflow-docker/external_scripts/transcribe.sh"

# Run the main script using the Python interpreter from the virtual environment
echo "Running the script with the activated environment..."
source "$ENV_PATH" && bash "$SCRIPT_PATH"
