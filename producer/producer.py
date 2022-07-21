import requests
import tweepy
import yaml
from yaml.loader import SafeLoader
from time import sleep
from json import dumps
from kafka import KafkaProducer
import logging

<<<<<<< HEAD
=======
logging.basicConfig(filename="producer.log", 
					format='%(asctime)s %(message)s', 
					filemode='w') 

logger=logging.getLogger() 

logger.setLevel(logging.DEBUG)

>>>>>>> 17569e5 (basic insertion with consumer [done])
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: dumps(x).encode('utf-8')
)

#process tweets from pagenated data
def process_page(page_results):
    for i, tweet in enumerate(page_results):
<<<<<<< HEAD
        print("Sending>>>>>>>>>>>>>>>tweet#"+str(i))
=======
        logger.info("Sending>>>>>>>>>>>>>>>tweet#"+str(i))
>>>>>>> 17569e5 (basic insertion with consumer [done])
        producer.send('tweets_topic', value= tweet._json)
        
if __name__ == '__main__':
    print('application started')
<<<<<<< HEAD
    with open('api_auth.yaml') as f:
        keys = yaml.load(f, Loader=SafeLoader)
    
=======

    try:
        with open('api_auth.yaml') as api_auth_file:
            keys = yaml.load(api_auth_file, Loader=SafeLoader)
    except:
        logger.error("api_auth.yaml not found")

>>>>>>> 17569e5 (basic insertion with consumer [done])
    OAUTH_KEYS = {'consumer_key': keys['consumer_key'], 'consumer_secret': keys['consumer_secret'],
                  'access_token_key': keys['access_token_key'], 'access_token_secret': keys['access_token_secret']}
    auth = tweepy.OAuthHandler(
        OAUTH_KEYS['consumer_key'], OAUTH_KEYS['consumer_secret'])

    api = tweepy.API(auth)

    keyword = "ikea"
    tweets = []

    for i, page in enumerate(tweepy.Cursor(api.search_tweets, q=keyword, count=20, lang='en').pages()):
        process_page(page)
        print('-------------', i, '--------------')


