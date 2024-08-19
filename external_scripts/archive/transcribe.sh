#!/bin/bash

# Define the path to the environment
ENV_PATH="/home/maksym/Documents/whisper/whisper-env/bin/activate"

# Activate the virtual environment
echo "Activating the virtual environment..."
source "$ENV_PATH"

# Check if the environment was activated successfully
if [[ "$VIRTUAL_ENV" != "" ]]; then
    echo "Virtual environment activated successfully: $VIRTUAL_ENV"

    # Your transcription script commands go here
    echo "Running transcription..."

    # Example of running your Python transcription script
    MP3_DIR="/home/maksym/Documents/whisper/files/azure/mp3/test"

    for mp3_file in "$MP3_DIR"/*.mp3*; do
        # Extract the base name (without extension) of the mp3 file
        base_name=$(basename "$mp3_file")
        base_name="${base_name%.mp3*}"
        

        # Check if there is no corresponding .srt file
        srt_file="${MP3_DIR}/transcriptions/${base_name}.srt"


        if [ -f "$srt_file" ]; then
            echo "${srt_file} already exists."
        else
            # If .srt file doesn't exist, run the whisper command
            echo "Im not seeing the ${srt_file} file, so running the whisper command."
            whisper "$mp3_file" --model large-v3 --output_dir "${MP3_DIR}/transcriptions" --device cuda
            
        fi
        #clear
    done
else
    echo "Failed to activate the virtual environment. Exiting..."
    exit 1
fi
