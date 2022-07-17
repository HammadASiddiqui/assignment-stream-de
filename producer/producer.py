import requests
import tweepy
import yaml
from yaml.loader import SafeLoader
from time import sleep
from json import dumps
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: dumps(x).encode('utf-8')
)

#process tweets from pagenated data
def process_page(page_results):
    for i, tweet in enumerate(page_results):
        print("Sending>>>>>>>>>>>>>>>tweet#"+str(i))
        producer.send('tweets_topic', value= tweet._json)
        
if __name__ == '__main__':
    print('application started')
    with open('api_auth.yaml') as f:
        keys = yaml.load(f, Loader=SafeLoader)
    
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


