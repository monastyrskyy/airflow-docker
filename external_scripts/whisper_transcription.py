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
        selected_mp3 = os.path.join(root, file)
        print(root)
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


if mp3_location == "/home/maksym/Documents/whisper/files/azure/mp3/Conan-O’Brien-Needs-A-Friend": # supposedly a much faster model that's still very good
    model_path = hf_hub_download(repo_id = "distil-whisper/distil-large-v3-openai", filename="model.bin")
    model = whisper.load_model(model_path, device="cuda")
    print('Loaded the distilled model')
else:
    model = whisper.load_model('large-v3', device="cuda")
    print('Loaded regular model')


# Create the transcriptio directory for every file
output_dir = transcription_location + '/' + title_local + '/'
os.makedirs(output_dir, exist_ok=True)

result = model.transcribe(mp3_location + '/' + title_local + '.mp3')

writer = get_writer("all", output_dir)
writer(result, title_local)


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
