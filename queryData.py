import happybase
from twython import Twython

from io import BytesIO
import os
import uuid


class Query:
    def __init__(self):
        print('constructor')
        self.__connection = self.init_hbase()


    def __del__(self):
        print('destructor')
        self.uninit_hbase()


    def get_values(self, row_key, cf, cf_q):
            query_cfq = list()
            for qualifiers in cf_q:
                query_cfq.append((cf+':'+qualifiers).encode('utf-8'))

            row_dict = self.__tweets_table.row(row_key, query_cfq)
            values = dict()
            for key in row_dict.keys():
                values[key.decode('utf-8')] = row_dict[key].decode('utf-8')
            return values


    def init_hbase(self):
        connection = happybase.Connection('localhost', autoconnect=False)
        connection.open()
        tables = connection.tables()

        # These are the table settings for Emergency Tweets
        tweets_table_name = 'emergency_tweets'
        # tweets_table_name = 'test'
        families = {'cf:': dict()}
        table_exists = False
        for table in tables:
            if table.decode() == tweets_table_name:
                table_exists = True
        if table_exists is not True:
            print('new table created!')
            connection.create_table(tweets_table_name, families)

        print(connection.tables())

        self.__tweets_table = connection.table(tweets_table_name)
        #print(tweets_table.row('row1'))
        #tweets_table.put('test1',{'lat':'0', 'long':'1', 'time': '12:00 AM', 'type':'robbery'})

        CONSUMER_KEY = ''
        CONSUMER_SECRET = ''
        ACCESS_TOKEN = '328959144-'
        ACCESS_SECRET = ''

        twitter = Twython(CONSUMER_KEY,CONSUMER_SECRET,ACCESS_TOKEN, ACCESS_SECRET)
        return connection


    def uninit_hbase(self):
        self.__connection.close()


if __name__ == '__main__':
    q = Query()

    values =  q.get_values(b'2e5dc9f9a4704d50b57cb540aa99caf0', 'cf', 
                           ['name', 'coord_lat', 'coord_long', 'text'])
    print(values)
