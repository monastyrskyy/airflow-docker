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