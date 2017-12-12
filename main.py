from flask import Flask, request, render_template
from queryData import * 
import json
app = Flask(__name__)

@app.route('/')
def index():
    d = []
    q = Query()
    emergency = ""
    # Example 1
    # This example do a simple query with a value filter
    # step1: scan qualified tuples and get the row keys
    print('*' * 10, 'Example1', '*' * 10)
    scan = q.select_values(limit=100, columns=('tweet', ['location']))

    # step2: project values with the keys
    column_family = 'tweet'
    qualifiers = ['name', 'coord_lat', 'coord_long','place_name','country_code','text']
    for row_key, row_data in scan:
        values = q.get_values(row_key, column_family, qualifiers)
        d.append(values)
        d.append({"emergency": emergency})
    print('test')    
    return render_template('index.html', data = d)

@app.route('/',methods=['POST'])
def apply_filter():
    location = request.form['location']
    location = location
    limit = request.form['limit']
    emergency = request.form['emergency']
    month = request.form['month']
    if(len(str(month))==1):
        month = '0'+month
    year = request.form['year']
    print("LIMIT IS "+limit)
    if not limit:
        limit = 100
    else:
        limit = int(limit)
    d = []
    q = Query()

    scan = None
    print(location)
    # if location set, apply filter, else don't
    
    scan = filter_by_starting_date(year, month, limit, q)
    

    # step2: project values with the keys
    column_family = 'tweet'
    qualifiers = ['name', 'coord_lat', 'coord_long','place_name','country_code','text','name']
    for row_key, row_data in scan:

        values = q.get_values(row_key, column_family, qualifiers)
       
        d.append(values)

    d.append({"emergency": emergency, "place_name": location})
    return render_template('index.html',data = d)


def filter_by_starting_date(year, month,limit, q):
    scan = None
    if not year and not month:
            scan = q.select_values(limit=limit, columns=('tweet', ['location']))
    else:
            filter_str = "SingleColumnValueFilter('datetime', 'year', =, 'binary:"+year+"', true, false)"

            print(filter_str)
            scan = q.select_values(limit=limit, columns=('datetime', []), filter=filter_str)
            print(scan)
    return scan
if __name__ == '__main__':
    app.run()
