FROM apache/airflow:2.9.3

# Set the timezone environment variable
ENV TZ=Europe/Berlin

# Add root user if it's missing
RUN echo "root:x:0:0:root:/root:/bin/bash" >> /etc/passwd

# Install git and other dependencies
USER root
RUN apt-get update && apt-get install -y git && apt-get clean

# Switch back to the airflow user
USER airflow

# Install the additional Python packages
RUN pip install azure-storage-blob
RUN pip install python-dotenv
RUN pip install sqlalchemy pymssql
RUN pip install -U pip setuptools wheel
RUN pip install -U spacy
RUN python -m spacy download de_dep_news_trf
RUN pip install pandas
