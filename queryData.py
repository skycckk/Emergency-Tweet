#!/usr/bin/python

import happybase

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
        """
        Test if the given column familiy exists.
        :param cf: string. Column family name
        :return: True if it exists
        """
        if cf.encode('utf-8') in self.__tweets_table.families().keys():
            return True
        else:
            return False


    def put_value(self, row_key, cf, cf_q, data_str):
        """
        Put data record in HBase with key and column family, qualifiers.
        :param row_key: byte array. Row key with type of byte array
        :param cf: string. Column family name
        :param cf_q: string. Column qualifier
        :param data_str: string. The data to put inserted
        """
        if not self.family_exist(cf):
            return

        if data_str is None:
            data = {(cf+':'+cf_q).encode('utf-8'): None}            
        else:
            data = {(cf+':'+cf_q).encode('utf-8'): data_str.encode('utf-8')}
        self.__tweets_table.put(row_key, data)


    def get_values(self, row_key, cf, cf_q):
        """
        Get values with a specific row key in HBase.
        :param row_key: byte array. Row key with type of byte array
        :param cf: string. Column family name.
        :param cf_q: list. A list of column qualifier string.
        :return: dictionay. The result with the key is cf:cf_q and the value is values.
                Those are decoded in type of string.
        """
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
        """
        Perform a SELECT query.
        :param limit: int. This number limits the number of results will be displayed
        :param columns: tuple. First element is the name of column family. Second element is a list of qualifiers.
                If the second element is [], then it will be treated as all qualifiers.
        :param filter: string. The filter followed the filtering format in HBase
        :param include_timestamp: boolean. Set true if you want to get the result with a timestamp
                Default is false.
        :return: Iterable Scan object. Get all selected row keys and row data(dict format)
        """
        query_cfq = None
        if columns is not None:
            query_cfq = list()
            cf = columns[0]
            cf_q = columns[1]
            if len(cf_q) == 0:  # accept all qualifiers
                query_cfq.append((cf).encode('utf-8'))
            else:
                for qualifiers in cf_q:
                    query_cfq.append((cf+':'+qualifiers).encode('utf-8'))
            
        scan = self.__tweets_table.scan(limit=limit, columns=query_cfq, filter=filter, 
                                        include_timestamp=include_timestamp)
        return scan


    def init_hbase(self):
        """
        Initialize HBase and create the connection
        """
        connection = happybase.Connection('localhost', autoconnect=False)
        connection.open()
        tables = connection.tables()

        # These are the table settings for Emergency Tweets
        # tweets_table_name = 'emergency_tweets'
        tweets_table_name = 'help_trump'
        
        families = {'tweet:': dict(), 'datetime:': dict()}
        table_exists = False
        for table in tables:
            if table.decode() == tweets_table_name:
                table_exists = True
        if table_exists is not True:
            print('new table created!')
            connection.create_table(tweets_table_name, families)

        print(connection.tables())

        self.__tweets_table = connection.table(tweets_table_name)
        return connection


    def uninit_hbase(self):
        self.__connection.close()


if __name__ == '__main__':
    q = Query()

    # Example 1
    # This example do a simple query with a value filter
    # step1: scan qualified tuples and get the row keys
    print('*' * 10, 'Example1', '*' * 10)
    scan = q.select_values(limit=10, columns=('tweet', ['location']), 
                           filter="ValueFilter(=, 'binary:San Jose, CA')")

    # step2: project values with the keys
    column_family = 'tweet'
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

    # Example 2.5
    # This example shows how to do a range search
    print('*' * 10, 'Example2.5', '*' * 10)
    filter_str = "SingleColumnValueFilter('datetime', 'year', =, 'binary:2017', true, false) AND \
                  SingleColumnValueFilter('datetime', 'month', =, 'binary:12', true, false) AND \
                  SingleColumnValueFilter('datetime', 'day', >, 'binary:01', true, false)"
    scan = q.select_values(limit=10, columns=('datetime', []), filter=filter_str)
    qualifiers = ['name']
    for row_key, row_data in scan:
        print(row_key)
        values = q.get_values(row_key, column_family, qualifiers)
        print(values)

    # Example 3
    # Insert data
    print('*' * 10, 'Example3', '*' * 10)
    q.put_value(b'0001', 'tweet', 'test_q1', '1234')
    q.put_value(b'0001', 'tweet', 'test_q2', '5678')
    q.put_value(b'0001', 'tweet2', 'test_q1', '1111')  # this will fail because tweet2 does not exist
    
