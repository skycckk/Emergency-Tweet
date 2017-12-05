#!/usr/bin/python

import random
from queryData import Query

__author__ = "Wei-Chung Huang"
__copyright__ = "Copyright 2017"
__version__ = "1.0.0"


if __name__ == '__main__':
    q = Query()
    emergent_words = ['robbery', 'help', 'mudder', 'shooting', 'battery', 
                      'accident', '911', 'Sexual assault', 'Theft']
    scan = q.select_values()
    cnt = 0
    for row_key, row_data in scan:
        key_word = emergent_words[random.randint(0, len(emergent_words) - 1)]
        q.put_value(row_key, 'tweet', 'text', key_word)
        cnt += 1
    print(cnt, 'records have been modifed!')
