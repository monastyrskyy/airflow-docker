
from openai import OpenAI

# # Adding the tenses to the sample sentences if they are not there yet.

# 'präsens', 'präteritum', 'perfekt', 'imperativ'

def query_openai(verb, tense):

    if tense in "Perfekt":
        prompt = f"Gib mir für das Verb {verb} einen alltäglichen Satz im {tense}, und nichts weiteres."
    elif tense == "Imperativ":
        prompt = f"Gib mir für das Verb {verb} einen alltäglichen Satz im {tense} für du, ihr, und Sie, und nichts weiteres."
    else:
        prompt = f"Gib mir für das Verb {verb} jeweils einen alltäglichen Satz im {tense} für ich, du, er, und ihr, und nichts weiteres."

    chat_completion = client.chat.completions.create(
        messages=[
            {
                "role": "user",
                "content": prompt,
            },
            {
                "role": "system",
                "content": f"Hilfreicher Deutsch-Grammatik-Assistent.",
            }
        ],
        model="gpt-4o-mini",
    )

    return chat_completion.choices[0].message.content.replace("**", "")




client = OpenAI(
    # This is the default and can be omitted
    api_key=os.environ.get("OPENAI_API_KEY"),
)





