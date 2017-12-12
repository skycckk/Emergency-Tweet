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
    random_months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']                  
    random_years = ['2001', '2002', '2003', '2004', '2005', '2006', '2007', '2008', '2009', '2010', '2011', '2012',
                    '2013', '2014', '2015', '2016', '2017']
    scan = q.select_values()
    cnt = 0
    for row_key, row_data in scan:
        key_word = emergent_words[random.randint(0, len(emergent_words) - 1)]
        q.put_value(row_key, 'tweet', 'text', key_word)

        fake_month = random_months[random.randint(0, len(random_months) - 1)]
        q.put_value(row_key, 'datetime', 'month', fake_month)

        fake_year = random_years[random.randint(0, len(random_years) - 1)]
        q.put_value(row_key, 'datetime', 'year', fake_year)
        cnt += 1



    print(cnt, 'records have been modifed!')
