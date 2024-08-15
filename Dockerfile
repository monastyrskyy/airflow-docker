FROM apache/airflow:2.9.3

# Install the additional Python package
RUN pip install openai-whisper

# Install ffmpeg
RUN apt-get update && apt-get install -y ffmpeg
