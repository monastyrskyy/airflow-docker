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
# Script:      retroactive_verb_extraction_and_upload.py
# Description: Extracts the verbs from transcribed files along with useful info
#              and uploads it to the verbs table and marks this in the feed table.
#              This is for file that have been transcribed long ago.
#
# ===================================================================================
# 
# Related DAG: retroactive_verb_extraction_and_upload.py
#
# ===================================================================================





# Libraries
import spacy
import pandas as pd
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine, text
from datetime import datetime
import re
import sys










#####
# Section 1
#####
# Extracting the vocab.

pd.set_option('display.max_rows', 1000)
nlp = spacy.load('de_dep_news_trf')
load_dotenv("/home/maksym/Documents/airflow-docker/.env")


sql_server_name = os.environ["SQLServerName"]
database_name = os.environ["DBName"]
sql_username = os.environ["SQLUserName"]
sql_password = os.environ["SQLPass"]
connection_string = f"mssql+pymssql://{sql_username}:{sql_password}@{sql_server_name}/{database_name}"
engine = create_engine(connection_string)


# Directory where transcriptions are locally
search_directory = "/home/maksym/Documents/whisper/files/manual/german"




# Walk through the directory tree
for root, dirs, files in os.walk(search_directory):
    if "kino_plus_archive" in dirs:
        # Remove it from the list so os.walk will not traverse it
        dirs.remove("kino_plus_archive")
    for file in files:
        if file.endswith('.txt') and not file.endswith('_verbs_nouns_extracted.txt'):
            print(f"File found!")
            print(file)

            file_path = root + '/' + file
            break



# Sanity check
if file_path.endswith('_verbs_nouns_extracted.txt'):
    print("horrible condition is met. exiting...")
    sys.exit()

















#####
# Section 2
#####
# Extracting the verbs.

print(f'Extracting the verbs for this file: {file_path}')

with open(file_path, 'r') as file:
    file_content = file.read()

doc = nlp(file_content)

verb_details = {}

# Extract verbs with their tenses and aspects
# Extract verbs and get their lemma (which is close to the infinitive form)
for token in doc:
    if token.pos_ == 'VERB':  # Filter for verbs
        verb = token.text
        infinitive = token.lemma_  # Lemma is often the base form, close to infinitive
        key = infinitive  # Store based on the infinitive form
        
        if key in verb_details:
            verb_details[key] += 1
        else:
            verb_details[key] = 1

# Create a DataFrame from the dictionary
df = pd.DataFrame(list(verb_details.items()), columns=['verb', 'frequency'])
df = df[['verb', 'frequency']]

# Function to clean verbs (similar to nouns, if required)
def clean_verb(verb):
    return re.sub(r"[^\wäöüÄÖÜß-]", "", verb)

# Apply the cleaning function to the Verb column
df['verb'] = df['verb'].apply(clean_verb)
df['verb'] = df['verb'].str.lower()

# Perform aggregation to get distinct records
df = df.groupby(['verb']).agg({
    'frequency': 'sum'  
}).reset_index()

# Step 4: Drop any long non-sense words (if needed)
df = df[df['verb'].str.len() <= 250]

# Step 5: Adding last_updated_dt to be able to delete stale records
df['last_updated_dt'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

print(df.sort_values(by='frequency', ascending=False)[0:10])









#####
# Section 3
#####
# Writing the data to the SQL database.

if df.empty:
    print('df is empty')
    # Split the file into name and extension
    file_name, file_extension = os.path.splitext(file_path)

    # Append '_nouns_extracted' to the file name
    new_file_name = file_name + '_verbs_nouns_extracted' + file_extension

    # Rename the file
    os.rename(file_path, new_file_name)

    print(f"File renamed to: {new_file_name}")
else:
    # Construct the VALUES clause for the entire DataFrame
    # e.g. ('Auto', 'das', 'Sing', 'Autos', 2, the datetime goes here), ('Berufsleben', 'das', 'Sing', 'Berufsleben', 1, the datetime goes)
    values_clause = ", ".join(
        [f"('{row['verb']}', {row['frequency']}, '{row['last_updated_dt']}')"
        for _, row in df.iterrows()]
    )


    # Preventing a one-to-many join in the MERGE query below
    delete_duplicates = f"""
    DELETE FROM vocab.verbs
    WHERE verb in (
        SELECT verb
        FROM vocab.verbs
        group by verb
        having count(*) > 1
    );
    """

    # Execute the query in the database
    with engine.begin() as conn:
        conn.execute(text(delete_duplicates))
        print('Deleted any applicable duplicates, if there were any.')




    # Deleting stale records
    delete_stale_records = f"""
    DELETE FROM vocab.verbs
    WHERE 
        frequency = 1
        AND last_updated_dt < DATEADD(day, -7, GETDATE());
    """

    # Execute the query in the database
    with engine.begin() as conn:
        conn.execute(text(delete_stale_records))
        print('Deleted low frequency words that have not been updated.')

    # Construct the full SQL query with all rows in the VALUES clause
    merge_query = f"""
    MERGE INTO vocab.verbs AS target
    USING (VALUES {values_clause}) AS source (verb, frequency, last_updated_dt)
    ON target.verb = source.verb
    WHEN MATCHED THEN 
        UPDATE SET 
            target.frequency = target.frequency + source.frequency,
            target.last_updated_dt = source.last_updated_dt
    WHEN NOT MATCHED THEN
        INSERT (verb, frequency, last_updated_dt)
        VALUES (source.verb, source.frequency, source.last_updated_dt);
    """

    # Execute the query in the database
    with engine.begin() as conn:
        conn.execute(text(merge_query))
        print('Nouns are merged.')















    #####
    # Section 4
    #####
    # Finally, if nothing else throws an error, I'm renaming the local file to not ever extract nouns from it again.



    # Split the file into name and extension
    file_name, file_extension = os.path.splitext(file_path)

    # Append '_nouns_extracted' to the file name
    new_file_name = file_name + '_verbs_nouns_extracted' + file_extension

    # Rename the file
    os.rename(file_path, new_file_name)

    print(f"File renamed to: {new_file_name}")