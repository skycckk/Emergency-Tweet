#!/usr/bin/python

import happybase
from twython import Twython

from io import BytesIO
import os
import uuid

import urllib
import requests

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import ast
import json
from queryData import Query
from email.utils import parsedate
from time import strftime
from datetime import datetime
import time
import pytz

# connection = happybase.Connection('localhost', autoconnect=False)
# connection.open()
# tables = connection.tables()

# These are the table settings for Emergency Tweets
# tweets_table_name = 'emergency_tweets'
# tweets_table_name = 'test'
# families = {'cf:': dict()}
# table_exists = False
# for table in tables:
#     if table.decode() == tweets_table_name:
#         table_exists = True
# if table_exists is not True:
#     print('new table created!')
#     connection.create_table(tweets_table_name, families)

# print(connection.tables())

# tweets_table = connection.table(tweets_table_name)
#print(tweets_table.row('row1'))
#tweets_table.put('test1',{'lat':'0', 'long':'1', 'time': '12:00 AM', 'type':'robbery'})

CONSUMER_KEY = 'XXXX'
CONSUMER_SECRET = 'XXXX'
ACCESS_TOKEN = 'XXXX'
ACCESS_SECRET = 'XXXX'

twitter = Twython(CONSUMER_KEY,CONSUMER_SECRET,ACCESS_TOKEN, ACCESS_SECRET)

emergency_key_word1 = '{{{'
emergency_key_word2 = ''

#This is a basic listener that just prints received tweets to stdout.
class StdOutListener(StreamListener):

    hbase = Query()

    def on_data(self, data):
        tweet = json.loads(data)
        
        print(tweet)
        # 
        # print("\n\n\n\n")
        if 'text' not in tweet.keys():
            return True

        if emergency_key_word2 in tweet["text"] and tweet["geo"] is not None:
            print('*' * 20, tweet["text"], '*' * 20)
            row_key = uuid.uuid4().hex.encode('utf-8')
            tweet_text = tweet['text'].replace(emergency_key_word1, '').replace(emergency_key_word2, '')
            tweet_text = tweet_text.strip()
            print(tweet["geo"]["coordinates"])
            print(type(tweet["geo"]["coordinates"][0]))
            
            #tweets_table.put(str(tweet["id"]), {"lat": str(tweet["geo"]["coordinates"][0]), "long":str(tweet["geo"]["coordinates"][1]), "time": str(tweet["created_at"]), "type":str(tweet["text"])})
            response = requests.get("https://maps.googleapis.com/maps/api/staticmap?center="+str(tweet["geo"]["coordinates"][0])+","+str(tweet["geo"]["coordinates"][1])+"&zoom=15&size=400x400&markers=color:red%7Clabel:A%7C"+str(tweet["geo"]["coordinates"][0])+","+str(tweet["geo"]["coordinates"][1])+"&key=AIzaSyDlN9DGKpAhkywco2M8uB_xYa_DCV_gSg8")
            photo = BytesIO(response.content)
            response = twitter.upload_media(media=photo)
            twitter.update_status(status=tweet_text+ " is happening here", media_ids=[response['media_id']], geo_enabled = True, display_coordinates=True, lat = tweet["geo"]["coordinates"][0], long = tweet["geo"]["coordinates"][1])
            print(data)

            # unpack datetime
            tweet_date = tweet['created_at']
            tweet_date = parsedate(tweet_date)
            timestamp = time.mktime(tweet_date)
            la_tz = pytz.timezone('America/Los_Angeles')
            year_month_day_hr = datetime.fromtimestamp(timestamp, tz=la_tz).strftime('%Y-%m-%d-%H')
            year_month_day_hr = year_month_day_hr.split('-')

            if len(year_month_day_hr[1]) == 1:
                year_month_day_hr[1] = '0' + year_month_day_hr[1]
            if len(year_month_day_hr[2]) == 1:
                year_month_day_hr[2] = '0' + year_month_day_hr[2]
            if len(year_month_day_hr[3]) == 1:
                year_month_day_hr[3] = '0' + year_month_day_hr[3]

            self.hbase.put_value(row_key, 'datetime', 'year', year_month_day_hr[0])
            self.hbase.put_value(row_key, 'datetime', 'month', year_month_day_hr[1])
            self.hbase.put_value(row_key, 'datetime', 'day', year_month_day_hr[2])
            self.hbase.put_value(row_key, 'datetime', 'hr', year_month_day_hr[3])

            # write data into HBase
            self.hbase.put_value(row_key, 'tweet', 'text', tweet_text)
            self.hbase.put_value(row_key, 'tweet', 'id', tweet['id_str'])
            self.hbase.put_value(row_key, 'tweet', 'coord_lat', str(tweet['geo']['coordinates'][0]))
            self.hbase.put_value(row_key, 'tweet', 'coord_long', str(tweet['geo']['coordinates'][1]))
            self.hbase.put_value(row_key, 'tweet', 'name', tweet['user']['name'])
            self.hbase.put_value(row_key, 'tweet', 'location', tweet['user']['location'])
            self.hbase.put_value(row_key, 'tweet', 'country_code', tweet['place']['country_code'])
            self.hbase.put_value(row_key, 'tweet', 'place_name', tweet['place']['name'])


            # read data from HBase for TESTING
            values = self.hbase.get_values(row_key, 'tweet', ['name', 'coord_lat', 'coord_long', 'text'])
            print('get desired values', values)

        return True

    def on_error(self, status):
        print (status)


    def get_values(self, row_key, cf, cf_q):
        query_cfq = list()
        for qualifiers in cf_q:
            query_cfq.append((cf+':'+qualifiers).encode('utf-8'))

        row_dict = tweets_table.row(row_key, query_cfq)
        values = dict()
        for key in row_dict.keys():
            values[key.decode('utf-8')] = row_dict[key].decode('utf-8')
        return values


if __name__ == '__main__':

    #This handles Twitter authetification and the connection to Twitter Streaming API
    l = StdOutListener()
    auth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)
    stream = Stream(auth, l)

    #This line filter Twitter Streams to capture data by the keywords: 'python', 'javascript', 'ruby'
    stream.filter(track=[emergency_key_word1])




