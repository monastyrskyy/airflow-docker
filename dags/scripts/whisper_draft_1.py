# tutorial: https://www.youtube.com/watch?v=UWOPQlxk-LM
# now have to schedule this with airflow.

from datetime import datetime, timedelta

import whisper 

model = whisper.load_model('tiny')

option = whisper.DecodingOptions(language='de', fp16=False)
result = model.transcribe('montana.mp3')

current_datetime = datetime.now()
save_target = f'montana{current_datetime}.vtt'

with open(save_target, 'w') as file:
    for indx, segment in enumerate(result['segments']):
        file.write(str(indx + 1) + '\n')
        file.write(str(timedelta(seconds=segment['start'])) + ' --> ' + str(timedelta(seconds=segment['end'])) + '\n')
        file.write(segment['text'].strip() + '\n')
        file.write('\n')

