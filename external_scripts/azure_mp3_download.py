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
# Script:      azure_mp3_download.py
# Description: Downloads the mp3 files from Azure Blob Storage periodically, 
#              only the ones that are not already downloaded.
#
# ===================================================================================
# 
# Related DAG: azure_mp3_download_dag.py
#
# ===================================================================================
#
# Misc.:
#       - Tutorial: https://github.com/PraveenKS30/Access-Azure-Using-Python/blob/main/access_azure_storage_connectionString.py
# ===================================================================================




# Libraries
import pandas as pd
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine, text
from datetime import datetime
import re
from azure.storage.blob import BlobServiceClient

from datetime import datetime



pd.set_option('display.max_rows', 1000)



#####
# Section 1
#####
# Extracting the vocab.

# SQL variables and strings
# Load environment variables from .env file
load_dotenv("/home/maksym/Documents/airflow-docker/.env")
sql_server_name = os.environ["SQLServerName"]
database_name = os.environ["DBName"]
sql_username = os.environ["SQLUserName"]
sql_password = os.environ["SQLPass"]
connection_string = f"mssql+pymssql://{sql_username}:{sql_password}@{sql_server_name}/{database_name}"
engine = create_engine(connection_string)
connection_str = os.environ["AZURE_CONNECTION_STR"]
container_name = 'mp3'
blob_service_client = BlobServiceClient.from_connection_string(conn_str=connection_str)
container_client = blob_service_client.get_container_client(container_name)
download_directory = "/home/maksym/Documents/whisper/files/azure/mp3/"



with engine.begin() as conn:
    # Grab an episode that has been transcribed, but transcription is only still on local
    # transcription_location would be set to Azure once the upload has been done
    check_query = text("SELECT * \
                       FROM rss_schema.rss_feed \
                       WHERE download_flag_azure = 'Y' \
                       AND download_flag_local = 'N'")
    df = pd.read_sql_query(check_query, conn)





if df.empty:
    print("No new files to download.")
else:
    print('hi')

    for index, row in df.iterrows():
        podcast_title = row['podcast_title']
        episode_title = row['title']

        # Checking old and new naming conventions
        blob_podcast_title = re.sub(r'[^\w\-_\. ]', '_', podcast_title.replace(' ', '-'))
        blob_podcast_title_old = podcast_title.replace(' ', '-')
        blob_episode_title = re.sub(r'[^\w\-_\. ]', '_', episode_title.replace(' ', '-'))
        blob_episode_title_old = episode_title.replace(' ', '-')

        
        # Generate all possible blob paths
        blob_paths = {
            "new_new": f'mp3/{blob_podcast_title}/{blob_episode_title}.mp3',
            "old_new": f'mp3/{blob_podcast_title_old}/{blob_episode_title}.mp3',
            "old_old": f'mp3/{blob_podcast_title_old}/{blob_episode_title_old}.mp3',
            "new_old": f'mp3/{blob_podcast_title}/{blob_episode_title_old}.mp3'
        }


        # Initialize a flag to track if the blob exists
        blob_exists = False

        # Loop over all possible paths and check for existence
        for path_name, blob_path in blob_paths.items():
            blob_client = container_client.get_blob_client(blob_path)
            if blob_client.exists():
                blob_exists = True

                # Determine the local folder based on the sanitized podcast title
                local_folder_path = os.path.join(download_directory, blob_podcast_title)

                # Check if the folder exists, create it if not
                if not os.path.exists(local_folder_path):
                    os.makedirs(local_folder_path)

                # Define the full local path for the MP3 file
                local_file_path = os.path.join(local_folder_path, f"{blob_episode_title}.mp3")


                # Download the blob to the local file
                with open(local_file_path, "wb") as download_file:
                    download_data = blob_client.download_blob()
                    download_file.write(download_data.readall())
                
                print(f'Downloaded {local_file_path}.')


                # Update the database to set download_flag_azure to 'N'
                with engine.begin() as conn:
                    update_query = text("""
                        UPDATE rss_schema.rss_feed
                        SET download_flag_local = 'Y', download_dt_local = :current_datetime
                        WHERE podcast_title = :podcast_title
                        AND title = :episode_title
                    """)
                    conn.execute(update_query, {
                                                    'podcast_title': podcast_title,
                                                    'episode_title': episode_title,
                                                    'current_datetime': datetime.now()
                                                })
                    print(f'Updated corresponding record in the rss_schema.rss_feed table.')


                break  # Stop checking once we find an existing blob
        
        if not blob_exists:
            print(f'Did not find the blob on Azure for {podcast_title} and {episode_title}.')

