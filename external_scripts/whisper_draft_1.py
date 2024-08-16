# tutorial: https://www.youtube.com/watch?v=UWOPQlxk-LM
# now have to schedule this with airflow.

from datetime import datetime, timedelta
import whisper 

# Get the current date and time
current_datetime = datetime.now()
model = whisper.load_model('tiny')

# option = whisper.DecodingOptions(language='de', fp16=False)
result = model.transcribe('/home/maksym/Documents/whisper/files/50-states/transcriptions/aaaaaa/montana.mp3')

save_target = f'/home/maksym/Documents/whisper/files/50-states/transcriptions/aaaaaa/montana{current_datetime}.vtt'

with open(save_target, 'w') as file:
    for indx, segment in enumerate(result['segments']):
        file.write(str(indx + 1) + '\n')
        file.write(str(timedelta(seconds=segment['start'])) + ' --> ' + str(timedelta(seconds=segment['end'])) + '\n')
        file.write(segment['text'].strip() + '\n')
        file.write('\n')

print("A whisper ai script has been run from within a docker container.")
print("Model loaded.")