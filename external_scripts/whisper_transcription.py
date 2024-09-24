# ===================================================================================
#                      _____          _  _____                 
#                     |  __ \        | |/ ____|                
#                     | |__) |__   __| | (___  _   _ _ __ ___  
#                     |  ___/ _ \ / _` |\___ \| | | | '_ ` _ \ 
#                     | |  | (_) | (_| |____) | |_| | | | | | |
#                     |_|   \___/ \__,_|_____/ \__,_|_| |_| |_|
#                                                              
# ===================================================================================
#
# Script:      whisper_transcription.py
# Description: Checks the SQL database for non-transcribed episodes, takes one, 
#              transcribes it, and marks it as transcribed on Azure.
#
# ===================================================================================
# 
# Related DAG: none
#       - This is scheduled with a cronjob; airflow within Docker was too much trouble
#
# ===================================================================================
#
# Misc.:
#       - Tutorial: https://www.youtube.com/watch?v=UWOPQlxk-LM
#       - Wow, much much faster: https://github.com/Vaibhavs10/insanely-fast-whisper
# 
# ===================================================================================




# Libraries
from datetime import datetime
import whisper 
from whisper.utils import get_writer
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import random
import csv
import portalocker
from huggingface_hub import hf_hub_download
import torch
from transformers import pipeline
from transformers.utils import is_flash_attn_2_available



#####
# Section 1
#####
# Finding an episode locally
# Benefit is that transcription can happen without the internet as well.

# Directories for MP3s and transcriptions
mp3_root_directory = "/home/maksym/Documents/whisper/files/azure/mp3"
transcription_root_directory = "/home/maksym/Documents/whisper/files/azure/transcriptions"

# Probability threshold for random selection
selection_chance = 0.2

# Variables to store the selected MP3 file and its transcription location
selected_mp3 = None
transcription_location = None

# Picking a random subdirectory
subdirectories = []
for root, dirs, files in os.walk(mp3_root_directory):
    has_untranscribed_mp3 = any(file.endswith(".mp3") and not file.endswith("_transcribed.mp3") for file in files)
    # If the directory contains at least one untranscribed MP3 file, add it to the list
    if has_untranscribed_mp3:
        subdirectories.append(root)


selected_directory = random.choice(subdirectories)



# Walk through the MP3 directory and its subdirectories
for root, dirs, files in os.walk(selected_directory):
    for file in files:
        # Skip files that are already marked as transcribed
        if file.endswith(".mp3") and not file.endswith("_transcribed.mp3"):
            # Randomly decide if this file should be selected
            if random.random() < selection_chance:
                # If selected, set the MP3 file path and calculate the transcription directory
                selected_mp3 = os.path.join(root, file)
                
                # Get the relative path of the MP3 file and determine the transcription location
                relative_path = os.path.relpath(selected_mp3, mp3_root_directory)
                transcription_location = os.path.join(transcription_root_directory, os.path.dirname(relative_path))
                
                # Create the transcription directory if it doesn't exist
                os.makedirs(transcription_location, exist_ok=True)
                
                # Stop searching as we have found a random untranscribed file
                break
    if not selected_mp3:
        for file in files:
            if file.endswith(".mp3") and not file.endswith("_transcribed.mp3"):
                selected_mp3 = os.path.join(root, file)
                relative_path = os.path.relpath(selected_mp3, mp3_root_directory)
                transcription_location = os.path.join(transcription_root_directory, os.path.dirname(relative_path))
                os.makedirs(transcription_location, exist_ok=True)
                print(root)
                break
    else:
        break

title_local = os.path.splitext(os.path.basename(selected_mp3))[0]
mp3_location = transcription_location.replace('transcriptions', 'mp3')

# Output the result
if selected_mp3:
    print(f'MP3 file selected: {selected_mp3}')
    print(f'Transcription target directory: {transcription_location}')
    print(f'mp3 Locationory: {mp3_location}')
    print(f'title_local: {title_local}')






#####
# Section 2
#####
# Transcribing the episode

# Get the current date and time
current_datetime = datetime.now()

# Helper functions to format timestamps
def format_timestamp_srt(t):
    hours = int(t // 3600)
    minutes = int((t % 3600) // 60)
    seconds = int(t % 60)
    milliseconds = int((t - int(t)) * 1000)
    return f"{hours:02}:{minutes:02}:{seconds:02},{milliseconds:03}"

pipe = pipeline(
    "automatic-speech-recognition",
    model="openai/whisper-large-v3", # select checkpoint from https://huggingface.co/openai/whisper-large-v3#model-details
    torch_dtype=torch.float16,
    device="cuda:0", # or mps for Mac devices
    model_kwargs={"attn_implementation": "flash_attention_2"} if is_flash_attn_2_available() else {"attn_implementation": "sdpa"},
)


# Create the transcriptio directory for every file
output_dir = transcription_location + '/' + title_local + '/'
os.makedirs(output_dir, exist_ok=True)

# Read the audio file into bytes, so that ffmpeg is not weirded out by special characters in the file names
audio_file_path = os.path.join(mp3_location, title_local + '.mp3')
with open(audio_file_path, 'rb') as f:
    audio_bytes = f.read()

# Proceed with transcription using audio bytes
outputs = pipe(
    audio_bytes,
    chunk_length_s=30,
    batch_size=24,
    return_timestamps=True,
)

chunks = outputs.get('chunks', [])

# Writing to .srt file
with open(f"{output_dir}{title_local}.srt", "w", encoding="utf-8") as f:
    previous_end_time = 0.0  # Initialize previous_end_time
    default_duration = 1.0    # Default duration in seconds if end_time is None

    for i, chunk in enumerate(chunks):
        index = i + 1
        start_time = chunk['timestamp'][0]
        end_time = chunk['timestamp'][1]
        text = chunk['text'].strip()  # Remove leading and trailing whitespace

        # Provide default values if start_time or end_time is None
        if start_time is None:
            start_time = previous_end_time
        if end_time is None:
            end_time = start_time + default_duration

        start_timestamp = format_timestamp_srt(start_time)
        end_timestamp = format_timestamp_srt(end_time)

        f.write(f"{index}\n")
        f.write(f"{start_timestamp} --> {end_timestamp}\n")
        f.write(f"{text}\n\n")

        previous_end_time = end_time  # Update previous_end_time
print("Transcription saved to 'transcription.srt'.")

# Write the entire transcription to a single file
with open(f"{output_dir}{title_local}.txt", "w", encoding="utf-8") as f:
    f.write(outputs["text"])
print("Transcription saved to 'transcription.txt'.")






# Renaming the MP3 file after transcription
original_file_path = mp3_location + '/' + title_local + '.mp3'
new_file_path = mp3_location + '/' + title_local + '_transcribed.mp3'

# Renaming the file
os.rename(original_file_path, new_file_path)
print(f"Renamed {original_file_path} to {new_file_path}")







#####
# Section 3
#####
# Putting the transcribed episodes in a queue to be uploaded to Azure later

# The path to your CSV file
csv_file_path = '/home/maksym/Documents/airflow-docker/queues/transcriptions.csv'

# The line to append to the CSV
new_row = [f'{transcription_location}/{title_local}']
# Append the new row to the CSV file
with open(csv_file_path, mode='a', newline='') as file:
    portalocker.lock(file, portalocker.LOCK_EX)
    
    writer = csv.writer(file, delimiter=',')
    writer.writerow(new_row)
    
    # Unlock the file after writing
    portalocker.unlock(file)
