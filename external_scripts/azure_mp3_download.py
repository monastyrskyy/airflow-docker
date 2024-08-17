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
#              only keeping the ones that are not already downloaded.
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
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine, text
from datetime import datetime

# Load environment variables from .env file
load_dotenv("/home/maksym/Documents/airflow-docker/.env")
sql_server_name = os.environ["SQLServerName"]
database_name = os.environ["DBName"]
sql_username = os.environ["SQLUserName"]
sql_password = os.environ["SQLPass"]


# Construct the SQLAlchemy connection string
connection_string = f"mssql+pymssql://{sql_username}:{sql_password}@{sql_server_name}/{database_name}"
engine = create_engine(connection_string)


# Load environment variables from .env file
load_dotenv("/home/maksym/Documents/airflow-docker/.env")
connection_str = os.environ["AZURE_CONNECTION_STR"]
container_name = 'mp3'

# Set up
blob_service_client = BlobServiceClient.from_connection_string(conn_str=connection_str)
container_client = blob_service_client.get_container_client(container_name)

# Directory to save the downloaded blobs
download_directory = "/home/maksym/Documents/whisper/files/azure"

# Create the download directory if it doesn't exist
os.makedirs(download_directory, exist_ok=True)

for blob in container_client.list_blobs():
    download_file_path = os.path.join(download_directory, blob.name)

    # Check if the file already exists locally
    if not os.path.exists(download_file_path):
        # Ensure the directory exists by creating it
        os.makedirs(os.path.dirname(download_file_path), exist_ok=True)
        
        # Download the blob
        blob_client = container_client.get_blob_client(blob)
        with open(download_file_path, "wb") as download_file:
            download_data = blob_client.download_blob()
            download_file.write(download_data.readall())
        
        print(f"Downloaded {blob.name} to {download_file_path}")

        # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        # To be tested and monitored on Azure:
        # Update the record in SQL:
        with engine.begin() as conn:
            update_query = text("""
                UPDATE rss_schema.rss_feed
                SET download_flag_local = 'Y', download_dt_local = :current_datetime
                WHERE title = :title
            """)
            conn.execute(update_query, {
                'current_datetime': datetime.now(),
                'title': blob.name
            })
            print(f"Updated record for '{blob.name}' in the database.")


        
    else:
        print(f"File {download_file_path} already exists. Skipping download.")