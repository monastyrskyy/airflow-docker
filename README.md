
```
===================================================================================

                      _____          _  _____                 
                     |  __ \        | |/ ____|                
                     | |__) |__   __| | (___  _   _ _ __ ___  
                     |  ___/ _ \ / _` |\___ \| | | | '_ ` _ \ 
                     | |  | (_) | (_| |____) | |_| | | | | | |
                     |_|   \___/ \__,_|_____/ \__,_|_| |_| |_|

                     Transciption with whisper-ai
                     Orchestration with Airflow in Docker

===================================================================================
```
- The local side of this project is done with Airflow and Docker.  
- This is where mp3 files from Azure will be regularly downloaded, transcribed, the transcription will be sent back to Azure, and finally update the SQL.
- Reason for using local dev for transcription is because these resources on Azure cost around $1/hour. 



### Useful commands:


`docker ps`  
&nbsp;&nbsp;&nbsp;&nbsp;|  
&nbsp;&nbsp;&nbsp;&nbsp;--> shows the different Airflow processes in Docker


`docker exec -it <id_goes_here> bash`  
&nbsp;&nbsp;&nbsp;&nbsp;|  
&nbsp;&nbsp;&nbsp;&nbsp;--> goes inside the process in docker

`docker-compose up`  
&nbsp;&nbsp;&nbsp;&nbsp;|  
&nbsp;&nbsp;&nbsp;&nbsp;--> starts docker

`docker-compose down`  
&nbsp;&nbsp;&nbsp;&nbsp;|  
&nbsp;&nbsp;&nbsp;&nbsp;--> stops docker

`docker build -t my_airflow_image .`  
&nbsp;&nbsp;&nbsp;&nbsp;|  
&nbsp;&nbsp;&nbsp;&nbsp;--> builds my_airflow_image using Dockerfile  
&nbsp;&nbsp;&nbsp;&nbsp;|  
&nbsp;&nbsp;&nbsp;&nbsp;--> docker-compose.yaml has to now use the newly build image as its reference when spinning up a new docker instance  

&nbsp;&nbsp;&nbsp;&nbsp;

### Instructions:
on how to get airflow to run an outside python script (that requires its own env) 

1. Mount all the necessary drives to the docker image
    - /home/maksym/Documents/whisper:/home/maksym/Documents/whisper
    - /home/maksym/Documents/airflow-docker/external_scripts:/home/maksym/Documents/airflow-docker/external_scripts
    - /home/maksym/.cache/whisper:/home/airflow/.cache/whisper

    - not for whipser, the user is airflow, not maksym

2. Install all the dependencies from the local env in a docker image, and build it.
    - note that python commands have to be run with root, but pip cannot be
    ```
        FROM apache/airflow:2.9.3

        # Install the additional Python package
        RUN pip install openai-whisper

        # Switch to the root user
        USER root

        # Install ffmpeg
        RUN apt-get update && apt-get install -y ffmpeg && ffmpeg -version

        # Switch to the root user
        USER airflow
    ```

    

3. Build the image
4. Reference the new image when spinning up this docker instance (in docker-compose.yaml)
    x-airflow-common:
        &airflow-common
        image: my_airflow_image 

### Misc

- Set up an airflow instance with Docker (surprisingly fast on startup; hardly noticeable)
- Used:
    - https://www.youtube.com/watch?v=aTaytcxy2Ck 
    - https://www.youtube.com/watch?v=IH1-0hwFZRQ 
    - https://www.youtube.com/watch?v=Z4wLw33fsJI
- **Problem:**  I wanted to trigger the run of the transcription script using Airflow, but my Docker instance didn't have the resources necessary to run the large transcription model.
    - The solution was to trigger a local script that would run on the underlying computer's resources, but would be triggered by Airflow from within Docker.
        - **Solution A** was to activate the local env of the script and then run the script. I was not able to do this.
        - **Solution B** was to download all the same dependencies as in the local env into the Docker image. This worked, and that's the solution I went with. 



### Misc updates:

### Updates, and notes:

28/8/2024

delete t
FROM [vocab].[nouns] t
WHERE t.Frequency < 0.1 * (
    SELECT MAX(Frequency)
    FROM [vocab].[nouns]
    WHERE Noun = t.Noun
)

This is a pruning operation because the noun and gender identification process is not perfect. I'm only taking the biggest one, to prune off any unneeded records. The query and explanation is from ChatGPT.

Let's say you have the following data for a noun Auto:

| Noun | Article | Frequency |
|------|---------|-----------|
| Auto | das     | 5852      |
| Auto | die     | 1         |
| Auto | der     | 7         |

Threshold Explanation:
0.1: This value is a multiplier. When you multiply the maximum frequency by 0.1, you are calculating 10% of that maximum value.
Comparison: The query compares the frequency of each row to this calculated value (10% of the maximum frequency for that noun). If the row's frequency is less than 10% of the maximum frequency, it is considered to be significantly lower and thus a candidate for deletion.
Example Scenario:

Maximum Frequency for Auto: 5852.
10% of Maximum Frequency: 5852 * 0.1 = 585.2.
In this case:

The frequency 1 for Auto, die is far below 585.2.
The frequency 7 for Auto, der is also far below 585.2.
Since both 1 and 7 are less than 10% of the maximum frequency (585.2), these records would be deleted by the query.