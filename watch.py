#!/usr/bin/env python3

import configparser
import sys
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import tweepy
import json
import pandas as pd
from time import time
import boto3

config = configparser.ConfigParser()
config.read('config.ini')

consumer_key = config['twitter']['consumer_key']
consumer_secret = config['twitter']['consumer_secret']
access_token = config['twitter']['access_token']
access_token_secret = config['twitter']['access_token_secret']

tweets = []

def get_args_words():
    return sys.argv[1].split(',')

def get_filtered_tweet(d):
    filtered = {
        'id': d['id'],
        'created_at': d['created_at'],
        'text': d['text'],
        'source': d['source'],
        'truncated': d['truncated'],
        'in_reply_to_status_id': d['in_reply_to_status_id'],
        'in_reply_to_user_id': d['in_reply_to_user_id'],
        'user_id': d['user']['id'],
        'user_name': d['user']['name'],
        'user_screen_name': d['user']['screen_name'],
        'user_location': d['user']['location'],
        'user_verified': d['user']['verified'],
        'user_followers_count': d['user']['followers_count'],
        'user_friends_count': d['user']['friends_count'],
        'user_listed_count': d['user']['listed_count'],
        'user_statuses_count': d['user']['statuses_count'],
        'user_created_at': d['user']['created_at'],
        'geo': d['geo'],
        'coordinates': d['coordinates'],
        'contributors': d['contributors'],
        'is_quote_status': d['is_quote_status'],
        'quote_count': d['quote_count'],
        'reply_count': d['reply_count'],
        'retweet_count': d['retweet_count'],
        'favorite_count': d['favorite_count'],
        'favorited': d['favorited'],
        'retweeted': d['retweeted'],
        'filter_level': d['filter_level'],
        'lang': d['lang']
    }

    return filtered

class StdOutListener(StreamListener):

    def __init__(self):
        self.counter = 1
    
    def on_data(self, data):
        try:
            tweet = get_filtered_tweet(json.loads(data))
            try:
                kinesis_client.put_record(
                                    StreamName=config['aws']['stream_name'],
                                    Data=json.dumps(tweet),
                                    PartitionKey=str(tweet['user_screen_name']))
                print(self.counter)
                self.counter += 1
            except (AttributeError, Exception) as e:
                print (e)
        except:
            print('Erro')

        return True

    def on_error(self, status):
        try:
            print(status)

            return True
        except:
            return True


if __name__ == '__main__':
    tags = get_args_words()

    kinesis_client = boto3.client('kinesis', 
                                  region_name=config['aws']['region_name'],
                                  aws_access_key_id=config['aws']['aws_access_key_id'],
                                  aws_secret_access_key=config['aws']['aws_secret_access_key'])

    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    stream = Stream(auth, l, tweet_mode= 'extended')
    stream.filter(track=tags)