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
# Script:      vocab_local_backup.py
# Description: As a nice little precaution, it downloads the table everyday and 
#              puts it into another github repo for version control.
#
# ===================================================================================
# 
# Related DAG: vocab_local_backup_dag.py
#
# ===================================================================================

# Libraries
import pandas as pd
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine, text

pd.set_option('display.max_rows', 1000)



#####
# Section 1
#####
# Download nouns table daily and push to github

load_dotenv("/home/maksym/Documents/airflow-docker/.env")
sql_server_name = os.environ["SQLServerName"]
database_name = os.environ["DBName"]
sql_username = os.environ["SQLUserName"]
sql_password = os.environ["SQLPass"]
connection_string = f"mssql+pymssql://{sql_username}:{sql_password}@{sql_server_name}/{database_name}"
engine = create_engine(connection_string)


# Directory where transcriptions are locally
output_dir = "/home/maksym/Documents/whisper/files/azure/nouns_and_frequencies"



with engine.begin() as conn:
    check_query = text("SELECT * \
                       FROM vocab.nouns")
    result = conn.execute(check_query).fetchall()

    if result == []:
        print("Something ain't right.")
    else:
        # Convert the result to a pandas DataFrame
        df = pd.DataFrame(result)

        # Define the output CSV file path
        output_file = os.path.join(output_dir, "nouns.csv")

        # Write the DataFrame to a CSV file
        df.to_csv(output_file, index=False)

        print(f"Data has been written to {output_file}")

with engine.begin() as conn:
    check_query = text("SELECT * \
                       FROM rss_schema.rss_feed")
    result = conn.execute(check_query).fetchall()

    if result == []:
        print("Something ain't right.")
    else:
        # Convert the result to a pandas DataFrame
        df = pd.DataFrame(result)

        # Define the output CSV file path
        output_file = os.path.join(output_dir, "episodes_table.csv")

        # Write the DataFrame to a CSV file
        df.to_csv(output_file, index=False)

        print(f"Data has been written to {output_file}")

with engine.begin() as conn:
    check_query = text("SELECT * \
                       FROM vocab.phrases")
    result = conn.execute(check_query).fetchall()

    if result == []:
        print("Something ain't right.")
    else:
        # Convert the result to a pandas DataFrame
        df = pd.DataFrame(result)

        # Define the output CSV file path
        output_file = os.path.join(output_dir, "phrases.csv")

        # Write the DataFrame to a CSV file
        df.to_csv(output_file, index=False)

        print(f"Data has been written to {output_file}")