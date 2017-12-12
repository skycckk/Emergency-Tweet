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
    random_years = ['2000', '2001', '2002', '2003', '2004', '2006', '2007', '2008', '2009', '2010', '2011', '2012','2017']                      
    scan = q.select_values()
    cnt = 0
    for row_key, row_data in scan:
        key_word = emergent_words[random.randint(0, len(emergent_words) - 1)]
        q.put_value(row_key, 'tweet', 'text', key_word)

        fake_year = random_years[random.randint(0, len(random_years) - 1)]
        q.put_value(row_key, 'datetime', 'year', fake_year)
        cnt += 1




    print(cnt, 'records have been modifed!')
