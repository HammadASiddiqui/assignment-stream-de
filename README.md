# KAFKA STREAM BASED SOLUTION
stream approach for the solution.
## Requirements 
1. Install docker
2. Install python
3. Install pip
3. Install the packages from the requirements.txt file in producer directory
## STEPS TO RUN
1. clone the project
2. open terminal
3. run the follwing command in the cloned folder directory
```
docker-compose up
```
4. cd into the producer directory
5. execute the producer
```
python3 producer.py
```
6. cd into the consumer directory
7.execute the consumer
```
python3 consumer.py
```
## About the solution
The docker-compose command basically lays the whole infrastructure required for the program to be executed:

For this approach the following components are required:

1. Kafka
2. Zookeeper
3. Postgres DB
4. Producer (Python program)
5. Consumer (Python program)

### Explanation
1. Firstly the docker spawns the containers for the zookeeper, Kafka and postgres DB (also creating the data tables required for the ETL). 
2. Then when the producer script gets executed it invokes the twitter API and pushes the data to the Kafka topic 'tweets_topic' for consumer to consume the API data. 
3. Then the consumer extracts the event data from kafka topic and after transforming the data ingests the relevant detials in the PostgresDB's Tables. 
4. This completes the ETL.

### How to query the DB for viewing ETL results

Execute the following commands on the terminal

```docker container ps```
view the DB containers CONTAINER ID and copy it 

```docker exec -it <copied CONTAINER ID> bash  ```
this will route your terminal to the postgres DB instance

```psql postgres://username:secret@localhost:5432/database```
this command will connect to your Database

## Design Diagram

Please find attached the design diagram for the solution.

![stream_de](https://user-images.githubusercontent.com/15999137/180238986-802bd022-2c64-4f74-b7fc-50e22b86c0e7.jpeg)
