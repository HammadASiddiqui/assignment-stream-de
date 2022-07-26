import requests
import tweepy
import yaml
from yaml.loader import SafeLoader
from time import sleep
from json import dumps
from kafka import KafkaProducer
import logging

logging.basicConfig(filename="producer.log", 
					format='%(asctime)s %(message)s', 
					filemode='w') 

logger=logging.getLogger() 

logger.setLevel(logging.DEBUG)

#configuring the producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: dumps(x).encode('utf-8')
)

#process tweets from pagenated data
def process_page(page_results):
    #sending the tweets page-by-page data to the kafka topic
    for i, tweet in enumerate(page_results):
        logger.info("Sending>>>>>>>>>>>>>>>tweet#"+str(i))
        producer.send('tweets_topic', value= tweet._json)
        
if __name__ == '__main__':
    print('application started')

    #reading api auth credentials file for extracting auth keys
    try:
        with open('api_auth.yaml') as api_auth_file:
            keys = yaml.load(api_auth_file, Loader=SafeLoader)
    except:
        logger.error("api_auth.yaml not found")

    #creating auth keys from the read dictionary
    OAUTH_KEYS = {'consumer_key': keys['consumer_key'], 'consumer_secret': keys['consumer_secret'],
                  'access_token_key': keys['access_token_key'], 'access_token_secret': keys['access_token_secret']}
    auth = tweepy.OAuthHandler(
        OAUTH_KEYS['consumer_key'], OAUTH_KEYS['consumer_secret'])

    #setting auth for API calling
    api = tweepy.API(auth)

    keyword = "ikea"
    

    #iterate over the API results and pagenate them in size of 20 tweets per page
    for i, page in enumerate(tweepy.Cursor(api.search_tweets, q=keyword, count=20, lang='en').pages()):
        process_page(page)
        print('-------------', i, '--------------')


