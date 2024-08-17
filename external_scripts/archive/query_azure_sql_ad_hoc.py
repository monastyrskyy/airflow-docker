from dotenv import load_dotenv
import os
from datetime import datetime
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

search_directory = "/home/maksym/Documents/whisper/files/azure/mp3"

try:
    with engine.begin() as conn:
        # Check if the item already exists
        check_query = text("SELECT title FROM rss_schema.rss_feed WHERE download_flag_local = 'N'")
        result = conn.execute(check_query).fetchall()
        
        # If the item doesn't exist, insert it
        if result is None:
            print("something didn't work with the query or execution.")
        else:
            for row in result:
                print(row[0].replace(' ', '-')+'.mp3')

                title = row[0].replace(' ', '-')+'.mp3'
                file_found = False
                
                # Walk through the directory and its subdirectories
                for root, dirs, files in os.walk(search_directory):
                    for file in files:
                        if file == title:  # Check if the file matches the title
                            print(f"File found!")
                            print("   ")
                            update_query = text("""
                                UPDATE rss_schema.rss_feed
                                SET download_flag_local = 'Y', download_dt_local = :current_datetime
                                WHERE title = :title
                            """)
                            conn.execute(update_query, {
                                'current_datetime': datetime.now(),
                                'title': row[0]
                            })
                            print(f"Updated record for '{title}' in the database.")
                            break  # Stop searching for it in this loop
                    
                    if file_found:
                        break  # Exit the outer loop if the file is found

                if not file_found:
                    print(f"File '{title}' not found in the directory.")


except Exception as e:
    print(f'Connection issue, and/or this one {e}')





