import os
from flask import Flask,redirect,render_template,request
import time
import random
import urllib
import datetime
import json
import redis
import pickle
import hashlib
import pandas as pd
from pandas.util import hash_pandas_object
import pyarrow as pa
import statsmodels.api as sm
import binascii





app = Flask(__name__)


#r = redis.Redis(host='didigam.redis.cache.windows.net',
        #port=6379, db=0, password='zLkOwcSu1ZYC59ZztJE0tRlsHFWLvhxdPLzJsV8JQyQ=', ssl=True)
        
        
r = redis.Redis(host='localhost',
        port=6379, db=0,decode_responses=True)


@app.route("/")
def hello():
    return render_template('first.html')




@app.route('/searchcsv', methods=['GET', 'POST'])
def countmidnightdata2():
    # connect to DB2

    #cur = db2conn.cursor()

    if  request.method == 'POST':
        start = time.time()

        name = request.form['name1']
        #state = float(request.form['name2'])
        # we have a Db2 connection, query the database
       # sql = "SELECT * FROM earthdata "
        # Note that for security reasons we are preparing the statement first,
        # then bind the form input as value to the statement to replace the
        # parameter marker.

       # df = pd.read_sql_query(sql, conn)
        #print(df.info())
        df =pd.read_csv('quakes.csv', encoding='latin-1')
        df1=pd.read_csv('latlong.csv', encoding='latin-1')

        #df['time'] = pd.to_datetime(df['time'])
        #df['hour'] = df['time'].dt.hour
        #df1 = df[(df['hour'] > name) & (df['hour'] < state)].sort_values('hour')

        #hash =  hash_pandas_object(dfq)

        #key = hash
       # context = pa.default_serialization_context()

        #key =  hash_pandas_object(dfq)
        #list = []
        dfq = df1[df1.country == name]
        key =  hash_pandas_object(dfq).to_string()


        if (r.get(key)):
           print("redis cached")
           #dfs = context.deserialize(r.get("key"))
           #end = time.time()
           #exectime = end - start
           blob = r.get(key)
           dfs = pd.read_json(blob)
           print('from cacheeeeeeee')
           end = time.time()
           exect = end - start
           print(exect)

           return render_template('search.html', ci=[dfs.to_html(classes='data', header="true")])



        else:
             #r.set(key, pickle.dumps(list(dfq)) )
             #r.expire(key, 36);
             # r.set("key", context.serialize(dfq).to_buffer().to_pybytes())
              #end = time.time()
              #exectime = end - start
              for i in range(0,40):
              
                dfq = df1[df1.country == name]
                key =  hash_pandas_object(dfq).to_string()
              #list.append(key)
              #key =  binascii.hexlify(num)

              
                data = dfq.T.to_json() 
                r.set(key, data)
              print('NOTTTT from cacheeeeeeee')
              end = time.time()
              exect = end - start
              print(exect)



              return render_template('search.html', ci=[dfq.to_html(classes='data', header="true")])

        #ibm_db.close(db2conn)#added
    return render_template('search.html')




@app.route('/searchcsv', methods=['GET','POST'])
def countgt6():
    return countmidnightdata2()





if __name__ == '__main__':
  app.run()

