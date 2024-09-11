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
# Script:      verb_extraction_and_upload.py
# Description: Extracts the verbs from transcribed files along with useful info
#              and uploads it to the verbs table and marks this in the feed table.
#
# ===================================================================================
# 
# Related DAG: verb_extraction_and_upload_dag.py
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

pd.set_option('display.max_rows', 1000)

nlp = spacy.load('de_dep_news_trf')






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


# Directory where transcriptions are locally
search_directory = "/home/maksym/Documents/whisper/files/azure/transcriptions"



with engine.begin() as conn:
    # Grab an episode that has been transcribed, but transcription is only still on local
    # transcription_location would be set to Azure once the upload has been done
    check_query = text("SELECT top(1) title \
                       FROM rss_schema.rss_feed \
                       WHERE verbs_extraction_flag = 'N' \
                       AND transcription_dt IS NOT NULL \
                       AND language IN ('de', 'de-DE')")
    result = conn.execute(check_query).fetchall()

    if result == []:
        print("No new vocab to upload.")
    else:
        title_sql = result[0][0]
        title_local = result[0][0].replace(' ', '-')
        print(f'file name: {title_local}')
        # Edge-case check
        found_it = False
        # Walk through the directory tree
        for root, dirs, files in os.walk(search_directory):
            if "Conan-O’Brien-Needs-A-Friend" in dirs:
                # Remove it from the list so os.walk will not traverse it
                dirs.remove("Conan-O’Brien-Needs-A-Friend")

            for file in files:
                if title_local.startswith(file.replace('.txt', '')):  # not using exact ==, because file could be "toyota_cor.txt" and title_local = "toyota_corolla"
                    print(f"File found!")
                    print(file)
        
                    file_path = root + '/' + file
                    break
        

















#####
# Section 2
#####
# Extracting the verbs.

print(f'Extracting the verbs for this this file: {file_path}')


with open(file_path, 'r') as file:
    file_content = file.read()

doc = nlp(file_content)


verb_details = {}

# Extract verbs with their genders, frequencies, and plurality
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

# Construct the VALUES clause for the entire DataFrame
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
    AND last_updated_dt < DATEADD(day, -14, GETDATE());
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
    # And update the record for this file
    update_query = text("""
        UPDATE rss_schema.rss_feed
        SET verbs_extraction_flag = 'Y', verbs_extraction_dt = :current_datetime
        WHERE title = :title
    """)
    conn.execute(update_query, {
        'current_datetime': datetime.now(),
        'title': title_sql
    })
    print(f"Updated record for '{title_sql}' in the database.")



