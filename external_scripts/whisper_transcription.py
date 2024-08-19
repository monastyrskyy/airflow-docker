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

# Load environment variables from .env file
load_dotenv("/home/maksym/Documents/airflow-docker/.env")
sql_server_name = os.environ["SQLServerName"]
database_name = os.environ["DBName"]
sql_username = os.environ["SQLUserName"]
sql_password = os.environ["SQLPass"]

# Construct the SQLAlchemy connection string
connection_string = f"mssql+pymssql://{sql_username}:{sql_password}@{sql_server_name}/{database_name}"
engine = create_engine(connection_string)


#####
# Section 1
#####
# Getting one episode that was not transcribed.

with engine.begin() as conn:
    # Check if the item already exists
    # ORDER BY NEWID() creates a random id for each row and sorts it. Basically randomizes which podcast is picked.
    check_query = text("SELECT top(1) title \
                       FROM rss_schema.rss_feed \
                       WHERE transcription_dt IS NULL \
                       AND download_flag_local = 'Y' AND \
                       podcast_title NOT IN ('Geschichten aus der Geschichte', 'Kino+') \
                       ORDER BY NEWID()")
    result = conn.execute(check_query).fetchall()

    title_sql = result[0][0]
    title_local = result[0][0].replace(' ', '-')

    # Walk through the directory and its subdirectories 
    # Once the file is found locally, its location will be noted and the transcription location created
    search_directory = "/home/maksym/Documents/whisper/files/azure/mp3"
    for root, dirs, files in os.walk(search_directory):
        for file in files:
            if file == title_local + '.mp3':  # Check if the file matches the title
                print(f"File found!")
                print(file)
    
                transcription_location = root.replace('mp3', 'transcriptions')
                mp3_location = root
                break
    print(f'transcription_location: {transcription_location}')
    print(f'mp3_location: {mp3_location}')





    #####
    # Section 2
    #####
    # Transcribing the episode

    # Get the current date and time
    current_datetime = datetime.now()
    model = whisper.load_model('large-v3', device="cuda")


    # Create the transcriptio directory for every file
    output_dir = transcription_location + '/' + title_local + '/'
    os.makedirs(output_dir, exist_ok=True)

    result = model.transcribe(mp3_location + '/' + title_local + '.mp3')

    writer = get_writer("all", output_dir)
    writer(result, title_local)





    #####
    # Section 3
    #####
    # Updating the record

    update_query = text("""
        UPDATE rss_schema.rss_feed
        SET transcription_dt = :current_datetime
        WHERE title = :title
    """)
    conn.execute(update_query, {
        'current_datetime': datetime.now(),
        'title': title_sql
    })
    print(f"Updated record for '{title_sql}' in the database.")
                            



