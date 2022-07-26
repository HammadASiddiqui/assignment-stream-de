
import yaml
from yaml.loader import SafeLoader
from kafka import KafkaConsumer
from json import loads
from time import sleep
from sqlalchemy import *
import logging

logging.basicConfig(filename="consumer.log", 
					format='%(asctime)s %(message)s', 
					filemode='w') 

logger=logging.getLogger() 

logger.setLevel(logging.DEBUG)

#configuration of consumer on kafka topic
consumer = KafkaConsumer(
    'tweets_topic',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group-id',
    value_deserializer=lambda x: loads(x.decode('utf-8'))
)



def init_db(db_creds):
    #extract credentials from dictionary for connecting DB
    db=None
    db_string = 'postgresql://{}:{}@{}:{}/{}'.format(db_creds['db_user'], db_creds['db_pass'], db_creds['db_host'], db_creds['db_port'], db_creds['db_name'])
    try:
        db = create_engine(db_string)
    except:
        logger.error("[DB CONNECT ERR]")
    finally:
        return db


#insert tweet's data
def insert_data(tweet_json, tweets, users):
    #creating dictionary contaning info from the extracted tweets JSON
    tweet_dict = {'tweet_id': str(tweet_json['id_str']),
    'created_at': tweet_json['created_at'],
    'tweet_text': str(tweet_json['text']).replace("'",'').replace("%(D)",''),
    'tweet_language': str(tweet_json['lang']),
    'favorite_count': tweet_json['favorite_count'],
    'retweet_count': tweet_json['retweet_count'],
    'user_id': str(tweet_json['user']['id_str'])}

    #creating dictionary contaning info from the extracted tweets JSON for users
    user_dict= {
        'user_id': str(tweet_json['user']['id_str']),
        'user_name': str(tweet_json['user']['name']),
        'user_url': str(tweet_json['user']['url']),}

    #Sqlalchemy ORM based insert function
    tweet_ingest = tweets.insert()
    
    #execution of insertion statement
    try:
        tweet_ingest.execute(tweet_dict)
        logger.info("Tweet id:"+tweet_dict['tweet_id']+ " inserted")
    except Exception as e:
        logger.error('[ERROR] INGESTION ERROR '+str(e))
    
    #Sqlalchemy ORM based insert function
    user_ingest = users.insert()
    
    #execution of insertion statement
    try:
        user_ingest.execute(tweet_dict)
        logger.info("user id:"+user_dict['user_id']+ " inserted")
    except Exception as e:
        logger.error('[ERROR] INGESTION ERROR '+ str(e))

        
        
        
    
if __name__ == '__main__':
    print('application started')

    #reading DB credential file for extracting credentials
    try:
        with open('db_creds.yaml') as db_file:
            db_creds_dict = yaml.load(db_file, Loader=SafeLoader)
    except:
        logger.error("db_creds.yaml file not found")

    #create connection to DB
    db = init_db(db_creds_dict)

    #sqlalchemy function to relate metadata info from db with program based ORM structures
    metadata = MetaData(db)

    #structural defination for user table fro ORM based ingestion
    tweets = Table('tweets', metadata,
        Column('tweet_id', String, primary_key=True),
        Column('created_at', TIMESTAMP, primary_key=True),
        Column('tweet_text', String),
        Column('tweet_language', String),
        Column('favorite_count', Integer),
        Column('retweet_count', Integer),
        Column('raw_json', String),
        Column('user_id', String),
    )

    #structural defination for user table fro ORM based ingestion
    users = Table('users', metadata,
        Column('user_id', String, primary_key=True),
        Column('user_name', String),
        Column('user_url', String),
    )
    
    #consuming events from kafka topic
    for event in consumer:
        event_data = event.value

        # insert the data into the relevant tables
        insert_data(event_data, tweets, users)
        