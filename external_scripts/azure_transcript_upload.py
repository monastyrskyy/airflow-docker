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
# Script:      azure_transcript_upload.py
# Description: Uploads the transcripts to Azure Blob Storage periodically, 
#              and changes the transcription_location to "Azure".
#
# ===================================================================================
# 
# Related DAG: azure_transcript_upload_dag.py
#
# ===================================================================================
#
# Misc.:
#       - Tutorial: https://github.com/PraveenKS30/Access-Azure-Using-Python/blob/main/access_azure_storage_connectionString.py
# ===================================================================================



# Libraries
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine, text



# Load environment variables from .env file
load_dotenv("/home/maksym/Documents/airflow-docker/.env")



# SQL variables and strings
sql_server_name = os.environ["SQLServerName"]
database_name = os.environ["DBName"]
sql_username = os.environ["SQLUserName"]
sql_password = os.environ["SQLPass"]
connection_string = f"mssql+pymssql://{sql_username}:{sql_password}@{sql_server_name}/{database_name}"
engine = create_engine(connection_string)



# Azure Blob storage variables and strings
connection_str = os.environ["AZURE_CONNECTION_STR"]
container_name = 'transcriptions'
blob_service_client = BlobServiceClient.from_connection_string(conn_str=connection_str)
container_client = blob_service_client.get_container_client(container_name)



# Function to upload files to Azure Blob Storage
def upload_file_to_blob(local_file_path, blob_name):
    blob_client = container_client.get_blob_client(blob_name)
    with open(local_file_path, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)



# Directory where transcriptions are locally
search_directory = "/home/maksym/Documents/whisper/files/azure/transcriptions"



with engine.begin() as conn:
    # Grab an episode that has been transcribed, but transcription is only still on local
    # transcription_location would be set to Azure once the upload has been done
    check_query = text("SELECT top(1) title \
                       FROM rss_schema.rss_feed \
                       WHERE transcription_location IS NULL \
                       AND transcription_dt IS NOT NULL")
    result = conn.execute(check_query).fetchall()

    if result == []:
        print("No new transcripts to upload.")
    else:
        title_sql = result[0][0]
        title_local = result[0][0].replace(' ', '-')
        # Edge-case check
        found_it = False
        # Walk through the directory tree
        for root, dirs, files in os.walk(search_directory):
            # Check if the current directory name matches the title_local
            if os.path.basename(root) == title_local:
                # Upload all files in this directory
                for file_name in files:
                    local_file_path = os.path.join(root, file_name)
                    # Generate the corresponding blob path (maintaining directory structure)
                    relative_path = os.path.relpath(local_file_path, search_directory)
                    blob_name = relative_path
                    # Upload the file to Azure Blob Storage
                    upload_file_to_blob(local_file_path, blob_name)
                    found_it = True
                print(f"Uploaded the transcription for '{title_sql}' is uploaded to Azure blob storage.")

                # Update the record in SQL:
                with engine.begin() as conn:
                    update_query = text("""
                        UPDATE rss_schema.rss_feed
                        SET transcription_location = 'Azure'
                        WHERE title = :title
                    """)
                    conn.execute(update_query, {
                        'title': title_sql
                    })
                    print(f"Updated record for '{title_sql}' in the database.")

                break
        if found_it == False:
            print(f"{title_sql}'s transcription_location IS NULL AND transcription_dt IS NOT NULL in the database, but it is not found locally.")
        


