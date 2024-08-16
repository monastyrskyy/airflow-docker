- planning to do the local transcription with Airflow and python files.
- Set up an airflow instance with Docker (surprisingly fast on startup; hardly noticeable)
    - used https://www.youtube.com/watch?v=aTaytcxy2Ck and https://www.youtube.com/watch?v=IH1-0hwFZRQ for initial setup.


Useful commands:

docker ps
    |
    --> shows the different airflow processes in docker


docker exec -it <id_goes_here> bash
    |
    --> goes inside the process in docker

docker-compose up
    |
    --> starts docker

docker-compose down
    |
    --> stops docker

docker build -t my_airflow_image .
    |
    --> builds my_airflow_image using Dockerfile
    |
    --> docker-compose.yaml has to now use the newly build image as its reference when spinning up a new docker instance



Instructions:
    on how to get airflow to run an outside python script (that requires its own env) 

1. Mount all the necessary drives to the docker image
    - /home/maksym/Documents/whisper:/home/maksym/Documents/whisper
    - /home/maksym/Documents/airflow-docker/external_scripts:/home/maksym/Documents/airflow-docker/external_scripts
    - /home/maksym/.cache/whisper:/home/airflow/.cache/whisper

    - not for whipser, the user is airflow, not maksym

2. Install all the dependencies from the local env in a docker image, and build it.
    FROM apache/airflow:2.9.3

    # Install the additional Python package
    RUN pip install openai-whisper

    # Switch to the root user
    USER root

    # Install ffmpeg
    RUN apt-get update && apt-get install -y ffmpeg && ffmpeg -version

    # Switch to the root user
    USER airflow


    - note that python commands have to be run with root, but pip cannot be

3. Build the image
4. Reference the new image when spinning up this docker instance (in docker-compose.yaml)
    x-airflow-common:
        &airflow-common
        image: my_airflow_image 