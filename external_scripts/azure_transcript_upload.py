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
import pandas as pd
import portalocker
from datetime import datetime



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



# Function to upload individual files to Azure Blob Storage
def upload_file_to_blob(local_file_path, blob_name):
    blob_client = container_client.get_blob_client(blob_name)
    with open(local_file_path, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)




# Function to delete a specific row
def delete_row(file_path_to_delete):
    # Lock the file for exclusive access
    with open(csv_file_path, 'r+', newline='') as file:
        try:
            # Lock the file for exclusive access
            portalocker.lock(file, portalocker.LOCK_EX)

            # Read the CSV into a DataFrame
            df = pd.read_csv(file)

            # Filter the DataFrame to exclude the row with the given file_path
            df_filtered = df[df['location'] != file_path_to_delete]

            # Move the file pointer back to the start and truncate the file
            file.seek(0)
            file.truncate()

            # Write the filtered DataFrame back to the file
            df_filtered.to_csv(file, index=False)
        
        finally:
            # Unlock the file after writing
            portalocker.unlock(file)







# File path to your CSV
csv_file_path = '/home/maksym/Documents/airflow-docker/queues/transcriptions.csv'
search_directory = '/home/maksym/Documents/whisper/files/azure/transcriptions'
pd.set_option('display.max_colwidth', 120)
# Read the CSV file into a DataFrame
df = pd.read_csv(csv_file_path)


for index, row in df.iterrows():
    directory_path = row['location'].replace('""', '"')
    if os.path.exists(directory_path) and os.path.isdir(directory_path):
        # Iterate over all files in the directory
        for root, dirs, files in os.walk(directory_path):
            for file_name in files:
                # Full path to the file
                file_path = os.path.join(root, file_name)
                
                # Generate the corresponding blob path (relative to the search directory)
                relative_path = os.path.relpath(file_path, search_directory)
                blob_name = relative_path.replace("\\", "/")  # Ensure correct formatting for Azure
                
                # Upload the file to Azure Blob Storage
                upload_file_to_blob(file_path, blob_name)

        print(f"Uploaded all files in '{directory_path}' to Azure blob storage.")

        file_name_without_extension = os.path.basename(directory_path)


        # Update the record in SQL:
        with engine.begin() as conn:
            update_query = text("""
                UPDATE rss_schema.rss_feed
                SET transcription_location = 'Azure', transcription_dt = :current_datetime
                WHERE REPLACE(title, ' ', '-') LIKE CONCAT(:title, '%')
            """)
            conn.execute(update_query, {
                'title': file_name_without_extension,
                'current_datetime': datetime.now()
            })
            print(f"Updated record for '{file_name_without_extension}' in the database.")

        delete_row(row['location'])
        print(f"Deleted the record here from the queue file:{row['location']}")





