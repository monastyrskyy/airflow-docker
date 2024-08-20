FROM apache/airflow:2.9.3

# Install the additional Python package
RUN pip install azure-storage-blob
RUN pip install python-dotenv
RUN pip install sqlalchemy pymssql
RUN pip install -U pip setuptools wheel
RUN pip install -U spacy
RUN python -m spacy download de_dep_news_trf
RUN pip install german-nouns
RUN pip install pandas