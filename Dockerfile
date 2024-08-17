FROM apache/airflow:2.9.3

# Install the additional Python package
RUN pip install torch torchvision torchaudio
RUN pip install openai-whisper
RUN pip install azure-storage-blob
RUN pip install python-dotenv
RUN pip install sqlalchemy pymssql


# Switch to the root user
USER root

# Install ffmpeg
RUN apt-get update && apt-get install -y ffmpeg && ffmpeg -version

# Switch to the root user
USER airflow