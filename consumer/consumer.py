from sqlalchemy import *
import yaml
from yaml.loader import SafeLoader
from kafka import KafkaConsumer
from json import loads
from time import sleep


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
        db = sqlalchemy.create_engine(db_string)
    except:
        print("[DB CONNECT ERR]")
    finally:
        return db

#insert user's data
def insert_into_users_table(tweet_json, db):
    user_dict= {
        'user_id': str(tweet_json['user']['id_str']),
        'user_name': str(tweet_json['user']['name']).replace("'",''),
        'user_url': str(tweet_json['user']['url']),
    }
    query = '''INSERT INTO users (user_id, user_name, user_url) 
    VALUES (
        '{user_id}',
        '{user_name}',
        '{user_url}'
    );'''.format(**user_dict)
    try:
        print("user inserted")
        db.execute(sqlalchemy.text(query))
    except:
        pass

#insert tweet's data
def insert_into_tweets_table(tweet_json, db):
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
        

        
        
        
    query = '''INSERT INTO tweets (tweet_id, created_at, tweet_text, tweet_language,favorite_count,  retweet_count, user_id) 
    VALUES (
        '{tweet_id}',
        timestamp '{created_at}',
        '{tweet_text}',
        '{tweet_language}',
        {favorite_count},
        {retweet_count},
        '{user_id}'
    );'''.format(**tweet_dict)
    try:
        print("Tweet inserted")
        db.execute(sqlalchemy.text(query))
    except:
        pass

    
if __name__ == '__main__':
    print('application started')

    with open('db_creds.yaml') as f:
        db_creds_dict = yaml.load(f, Loader=SafeLoader)
    
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
        # Do whatever you want
        insert_into_tweets_table(event_data, db)
        insert_into_users_table(event_data, db)