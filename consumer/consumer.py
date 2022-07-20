
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


consumer = KafkaConsumer(
    'tweets_topic',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group-id',
    value_deserializer=lambda x: loads(x.decode('utf-8'))
)



def init_db(db_creds):
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
    tweet_dict = {'tweet_id': str(tweet_json['id_str']),
    'created_at': tweet_json['created_at'],
    'tweet_text': str(tweet_json['text']).replace("'",'').replace("%(D)",''),
    'tweet_language': str(tweet_json['lang']),
    'favorite_count': tweet_json['favorite_count'],
    'retweet_count': tweet_json['retweet_count'],
    'user_id': str(tweet_json['user']['id_str'])}

    user_dict= {
        'user_id': str(tweet_json['user']['id_str']),
        'user_name': str(tweet_json['user']['name']),
        'user_url': str(tweet_json['user']['url']),}

    tweet_ingest = tweets.insert()
    
    try:
        tweet_ingest.execute(tweet_dict)
        logger.info("Tweet id:"+tweet_dict['tweet_id']+ " inserted")
    except Exception as e:
        logger.error('[ERROR] INGESTION ERROR '+str(e))
    
    user_ingest = users.insert()
    
    try:
        user_ingest.execute(tweet_dict)
        logger.info("user id:"+user_dict['user_id']+ " inserted")
    except Exception as e:
        logger.error('[ERROR] INGESTION ERROR '+ str(e))

        
        
        
    
if __name__ == '__main__':
    print('application started')

    try:
        with open('db_creds.yaml') as db_file:
            db_creds_dict = yaml.load(db_file, Loader=SafeLoader)
    except:
        logger.error("db_creds.yaml file not found")

    db = init_db(db_creds_dict)

    metadata = MetaData(db)

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

    users = Table('users', metadata,
        Column('user_id', String, primary_key=True),
        Column('user_name', String),
        Column('user_url', String),
    )
    

    for event in consumer:
        event_data = event.value

        # insert the data into the relevant tables
        insert_data(event_data, tweets, users)
        