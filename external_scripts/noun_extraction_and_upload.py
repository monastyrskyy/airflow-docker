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
# Script:      noun_extraction_and_upload.py
# Description: Extracts the nouns from transcribed files along with useful info
#              and uploads it to the nouns table and marks this in the feed table.
#
# ===================================================================================
# 
# Related DAG: noun_extraction_and_upload.py
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
search_directory = "/home/maksym/Documents/whisper/files/azure/transcriptions"



with engine.begin() as conn:
    # Grab an episode that has been transcribed, but transcription is only still on local
    # transcription_location would be set to Azure once the upload has been done
    check_query = text("SELECT top(1) title \
                       FROM rss_schema.rss_feed \
                       WHERE nouns_extraction_flag = 'N' \
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

# Display the updated DataFrame
# print(df)



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

print(df.sort_values(by='Frequency', ascending=False)[0:10])













#####
# Section 3
#####
# Writing the data to the SQL database.

# Construct the VALUES clause for the entire DataFrame
# e.g. ('Auto', 'das', 'Sing', 'Autos', 2), ('Berufsleben', 'das', 'Sing', 'Berufsleben', 1)
values_clause = ", ".join(
    [f"('{row['Noun']}', '{row['Article']}', '{row['Number']}', '{row['Plural']}', {row['Frequency']})"
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


# Construct the full SQL query with all rows in the VALUES clause
merge_query = f"""
MERGE INTO vocab.nouns AS target
USING (VALUES {values_clause}) AS source (Noun, Article, Number, Plural, Frequency)
ON target.Noun = source.Noun AND target.Article = source.Article AND target.Number = source.Number
WHEN MATCHED THEN 
    UPDATE SET target.Frequency = target.Frequency + source.Frequency
WHEN NOT MATCHED THEN
    INSERT (Noun, Article, Number, Plural, Frequency)
    VALUES (source.Noun, source.Article, source.Number, source.Plural, source.Frequency);
"""

# Execute the query in the database
with engine.begin() as conn:
    conn.execute(text(merge_query))

    # And update the record for this file
    update_query = text("""
        UPDATE rss_schema.rss_feed
        SET nouns_extraction_flag = 'Y', nouns_extraction_dt = :current_datetime
        WHERE title = :title
    """)
    conn.execute(update_query, {
        'current_datetime': datetime.now(),
        'title': title_sql
    })
    print(f"Updated record for '{title_sql}' in the database.")



