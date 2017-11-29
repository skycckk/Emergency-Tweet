import happybase
from twython import Twython

from io import BytesIO
import os
import uuid

__author__ = "Wei-Chung Huang"
__copyright__ = "Copyright 2017"
__version__ = "1.0.0"


class Query:
    def __init__(self):
        print('constructor')
        self.__connection = self.init_hbase()


    def __del__(self):
        print('destructor')
        self.uninit_hbase()


    def family_exist(self, cf):
        if cf.encode('utf-8') in self.__tweets_table.families().keys():
            return True
        else:
            return False


    def put_value(self, row_key, cf, cf_q, data_str):
        if not self.family_exist(cf):
            return

        data = {(cf+':'+cf_q).encode('utf-8'): data_str.encode('utf-8')}
        self.__tweets_table.put(row_key, data)


    def get_values(self, row_key, cf, cf_q):
        if not self.family_exist(cf):
            return

        query_cfq = list()
        for qualifiers in cf_q:
            query_cfq.append((cf+':'+qualifiers).encode('utf-8'))

        row_dict = self.__tweets_table.row(row_key, query_cfq)
        values = dict()
        for key in row_dict.keys():
            values[key.decode('utf-8')] = row_dict[key].decode('utf-8')
        return values


    def select_values(self, limit=None, columns=None, filter=None, include_timestamp=False):
        query_cfq = None
        if columns is not None:
            query_cfq = list()
            cf = columns[0]
            cf_q = columns[1]
            for qualifiers in cf_q:
                query_cfq.append((cf+':'+qualifiers).encode('utf-8'))

        scan = self.__tweets_table.scan(limit=limit, columns=query_cfq, filter=filter, 
                                        include_timestamp=include_timestamp)
        return scan


    def init_hbase(self):
        connection = happybase.Connection('localhost', autoconnect=False)
        connection.open()
        tables = connection.tables()

        # These are the table settings for Emergency Tweets
        tweets_table_name = 'emergency_tweets'
        
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

        CONSUMER_KEY = ''
        CONSUMER_SECRET = ''
        ACCESS_TOKEN = ''
        ACCESS_SECRET = ''

        twitter = Twython(CONSUMER_KEY,CONSUMER_SECRET,ACCESS_TOKEN, ACCESS_SECRET)
        return connection


    def uninit_hbase(self):
        self.__connection.close()


if __name__ == '__main__':
    q = Query()

    # Example 1
    # This example do a simple query with a value filter
    # step1: scan qualified tuples and get the row keys
    print('*' * 10, 'Example1', '*' * 10)
    scan = q.select_values(limit=10, columns=('cf', ['location']), 
                           filter="ValueFilter(=, 'binary:San Jose, CA')")

    # step2: project values with the keys
    column_family = 'cf'
    qualifiers = ['name', 'coord_lat', 'coord_long', 'text']
    for row_key, row_data in scan:
        values = q.get_values(row_key, column_family, qualifiers)
        print(values)

    # Example 2
    # Do a SELECT query with family qualifier and value 
    # Modify filter_str to the desired family and value
    print('*' * 10, 'Example2', '*' * 10)
    filter_str = "(QualifierFilter(=, 'binary:name') AND ValueFilter(=, 'binary:Wayne Huang')) OR \
                  (QualifierFilter(=, 'binary:location') AND ValueFilter(=, 'binary:San Jose, CA'))"
    scan = q.select_values(limit=10, 
                           filter=filter_str)
    qualifiers = ['name', 'id', 'text']
    for row_key, row_data in scan:
        values = q.get_values(row_key, column_family, qualifiers)
        print(values)

    # Example 3
    # Insert data
    q.put_value(b'0001', 'cf', 'test_q1', '1234')
    q.put_value(b'0001', 'cf', 'test_q2', '5678')
    q.put_value(b'0001', 'cf2', 'test_q1', '1111')  # this will fail because cf2 does not exist
    

