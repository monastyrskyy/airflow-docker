FROM apache/airflow:2.9.3

# Install the additional Python package
RUN pip install azure-storage-blob
RUN pip install python-dotenv
RUN pip install sqlalchemy pymssql
