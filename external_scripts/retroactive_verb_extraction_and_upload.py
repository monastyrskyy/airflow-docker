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
# Script:      retroactive_noun_extraction_and_upload.py
# Description: Extracts the nouns from transcribed files along with useful info
#              and uploads it to the nouns table and marks this in the feed table.
#              This is for file that have been transcribed long ago.
#
# ===================================================================================
# 
# Related DAG: retroactive_noun_extraction_and_upload.py
#
# ===================================================================================





# Libraries
import spacy
import pandas as pd
from german_nouns.lookup import Nouns
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine, text
from datetime import datetime
import re
import sys

pd.set_option('display.max_rows', 1000)

nlp = spacy.load('de_dep_news_trf')

nouns = Nouns()






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
search_directory = "/home/maksym/Documents/whisper/files/manual/german"




# Walk through the directory tree
for root, dirs, files in os.walk(search_directory):
    if "kino_plus_archive" in dirs:
        # Remove it from the list so os.walk will not traverse it
        dirs.remove("kino_plus_archive")
    for file in files:
        if file.endswith('.txt') and not file.endswith('_nouns_extracted.txt'):
            print(f"File found!")
            print(file)

            file_path = root + '/' + file
            break





# Sanity check
if file_path.endswith('_nouns_extracted.txt'):
    print("horrible condition is met. exiting...")
    sys.exit()











#####
# Section 2
#####
# Extracting the nouns.

print(f'Extracting the nouns for this this file: {file_path}')


with open(file_path, 'r') as file:
    file_content = file.read()

doc = nlp(file_content)


noun_details = {}

# Extract nouns with their genders, frequencies, and plurality
for token in doc:
    if token.pos_ == 'NOUN':
        noun = token.text
        gender = token.morph.get('Gender')
        number = token.morph.get('Number')
        key = (noun, ','.join(gender) if gender else 'None', ','.join(number) if number else 'None')
        
        if key in noun_details:
            noun_details[key] += 1
        else:
            noun_details[key] = 1

# Create a DataFrame from the dictionary
df = pd.DataFrame(list(noun_details.items()), columns=['Noun_Gender_Number', 'Frequency'])
df[['Noun', 'Gender', 'Number']] = df['Noun_Gender_Number'].apply(pd.Series).fillna('NA')
df = df[['Noun', 'Gender', 'Number', 'Frequency']]



def get_plural(input):
    try:
        word = nouns[input]
        flexion = word[0]['flexion']
        
        # Check for 'nominativ plural' first
        if 'nominativ plural' in flexion:
            return flexion['nominativ plural']
        # If not found, check for 'nominativ plural 1'
        elif 'nominativ plural 1' in flexion:
            return flexion['nominativ plural 1']
        elif 'nominativ plural stark' in flexion:
            return flexion['nominativ plural stark']
        else:
            return 'NA'
    except KeyError:
        return 'NA'
    except (IndexError, TypeError):
        return 'NA'
    
def get_singular(input):
    try:
        word = nouns[input]
        flexion = word[0]['flexion']
        
        # Check for 'nominativ singular' first
        if 'nominativ singular' in flexion:
            return flexion['nominativ singular']
        # If not found, check for 'nominativ singular 1'
        elif 'nominativ singular 1' in flexion:
            return flexion['nominativ singular 1']
        elif 'nominativ singular stark' in flexion:
            return flexion['nominativ singular stark']
        else:
            return 'NA'
    except KeyError:
        return 'NA'
    except (IndexError, TypeError):
        return 'NA'
    
# Add the 'Plural' column
df['Plural'] = df['Noun'].apply(get_plural)
df['Singular'] = df['Noun'].apply(get_singular)


# In case the original word was plural, replacing it with the singular version
df.loc[(df['Number'] == 'Plur') & (df['Singular'] != 'NA'), 'Noun'] = df['Singular']
df.loc[(df['Number'] == 'Plur') & (df['Singular'] != 'NA'), 'Number'] = 'Sing'

# For when the base word is some declension (i.e. dem Menschen vs der Mensch)
df.loc[(df['Noun'] != df['Singular']) & (df['Singular'] != 'NA'), 'Noun'] = df['Singular']


# Function to remove problematic characters while keeping dashes and umlauts
# They break the SQL merge query
def clean_noun(noun):
    return re.sub(r"[^\wäöüÄÖÜß-]", "", noun)

# Apply the cleaning function to the Noun column
df['Noun'] = df['Noun'].apply(clean_noun)

# Perform aggregation to get distinct records
df = df.groupby(['Noun', 'Gender', 'Number']).agg({
    'Frequency': 'sum',  
    'Plural': 'min',  
    'Singular': 'min'
}).reset_index()


# Create the mapping dictionary
gender_to_article = {
    'Masc': 'der',
    'Fem': 'die',
    'Neut': 'das'
}

# Replace 'None' with 'NA' and apply the mapping
df['Article'] = df['Gender'].map(gender_to_article).fillna('NA').replace([None], 'NA')
df = df.drop(columns=['Gender'])

df = df[['Noun', 'Article', 'Number', 'Plural', 'Frequency']]





# the merge below is case insensitive. Adding this to remove any duplicates, regardless of case
# Create a temporary lowercase version of the DataFrame for grouping
df['Noun_lower'] = df['Noun'].str.lower()
df['Article_lower'] = df['Article'].str.lower()
df['Number_lower'] = df['Number'].str.lower()

# Step 2: Drop duplicates based on the lowercase columns
df_deduped = df.drop_duplicates(subset=['Noun_lower', 'Article_lower', 'Number_lower'])

# Step 3: Drop the temporary lowercase columns
df = df_deduped.drop(columns=['Noun_lower', 'Article_lower', 'Number_lower'])

# Step 4: Drop any long non-sense words. 
# Whisper generated this noun one time 'Too-many-tabs-to-many-tabs-to-many-tabs-to-many-tabs-to-many-tabs-to-many-tabs-to-many-tabs-to-many-'
# It broke the Airflow job
df = df[df['Noun'].str.len() <= 250]


# Step 5: Adding last_updated_dt to be able to delete stale records
df['last_updated_dt'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')


print(df.sort_values(by='Frequency', ascending=False)[0:10])













#####
# Section 3
#####
# Writing the data to the SQL database.

# Construct the VALUES clause for the entire DataFrame
# e.g. ('Auto', 'das', 'Sing', 'Autos', 2, the datetime goes here), ('Berufsleben', 'das', 'Sing', 'Berufsleben', 1, the datetime goes)
values_clause = ", ".join(
    [f"('{row['Noun']}', '{row['Article']}', '{row['Number']}', '{row['Plural']}', {row['Frequency']}, '{row['last_updated_dt']}')"
     for _, row in df.iterrows()]
)


# Preventing a one-to-many join in the MERGE query below
delete_duplicates = f"""
DELETE FROM vocab.nouns
WHERE Noun in (
    SELECT Noun
    FROM vocab.nouns
    group by Noun, Article, Number
    having count(*) > 1
);
"""

# Execute the query in the database
with engine.begin() as conn:
    conn.execute(text(delete_duplicates))
    print('Deleted any applicable duplicates, if there were any.')



# Pruning
pruning = f"""
delete t
FROM [vocab].[nouns] t
WHERE t.Frequency < 0.1 * (
    SELECT MAX(Frequency)
    FROM [vocab].[nouns]
    WHERE Noun = t.Noun
)
"""

# Execute the query in the database
with engine.begin() as conn:
    conn.execute(text(pruning))
    print('Pruned away relatively low frequencies of words.')

# Deleting stale records
delete_stale_records = f"""
DELETE FROM vocab.nouns
WHERE 
    Frequency = 1
    AND last_updated_dt < DATEADD(day, -1, GETDATE());
"""

# Execute the query in the database
with engine.begin() as conn:
    conn.execute(text(delete_stale_records))
    print('Deleted low frequency words that have not been updated.')

# Construct the full SQL query with all rows in the VALUES clause
merge_query = f"""
MERGE INTO vocab.nouns AS target
USING (VALUES {values_clause}) AS source (Noun, Article, Number, Plural, Frequency, last_updated_dt)
ON target.Noun = source.Noun AND target.Article = source.Article AND target.Number = source.Number
WHEN MATCHED THEN 
    UPDATE SET 
        target.Frequency = target.Frequency + source.Frequency,
        target.last_updated_dt = source.last_updated_dt
WHEN NOT MATCHED THEN
    INSERT (Noun, Article, Number, Plural, Frequency, last_updated_dt)
    VALUES (source.Noun, source.Article, source.Number, source.Plural, source.Frequency, source.last_updated_dt);
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
new_file_name = file_name + '_nouns_extracted' + file_extension

# Rename the file
os.rename(file_path, new_file_name)

print(f"File renamed to: {new_file_name}")
