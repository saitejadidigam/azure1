import os
from flask import Flask,redirect,render_template,request
import pypyodbc
import pyodbc
import time
import random
import urllib
import datetime
import json
import redis
import pickle
import hashlib

app = Flask(__name__)

server = 'didigam.database.windows.net'
database = 'mySampleDatabase'
username = 'azureuser'
password = 'AMMAdad@143'
driver= '{ODBC Driver 17 for SQL Server}'
#trusted_connection='yes'

myHostname = "didigam.redis.cache.windows.net"
myPassword = "zLkOwcSu1ZYC59ZztJE0tRlsHFWLvhxdPLzJsV8JQyQ="




r = redis.Redis(host='didigam.redis.cache.windows.net',
        port=6380, db=0, password='zLkOwcSu1ZYC59ZztJE0tRlsHFWLvhxdPLzJsV8JQyQ=', ssl=True)



def randrange(rangfro=None,rangto=None,num=None):
    dbconn = pypyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT=1443;DATABASE='+database+';UID='+username+';PWD='+password)
   # dbconn = pypyodbc.connect(Driver={ODBC Driver 13 for SQL Server};Server=tcp:didigam.database.windows.net,1433;Database=mySampleDatabase;Uid=azureuser;Pwd={your_password_here};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;)

    rangto = rangto
    cursor = dbconn.cursor()
    start = time.time()
    for i in range(0,int(num)):
        #mag= round(random.uniform(rangfro, rangto),2)
        success="SELECT * from [earthq] where mag>'"+str(rangto)+"'"
        hash = hashlib.sha224(success.encode('utf-8')).hexdigest()
        key = "redis_cache:" + hash
        if (r.get(key)):
           print("redis cached")
        else:
           # Do MySQL query
           cursor.execute(success)
           data = cursor.fetchall()
           rows = []
           for j in data:
                rows.append(str(j))
           # Put data into cache for 1 hour
           r.set(key, pickle.dumps(list(rows)) )
           r.expire(key, 36)
           print("not from cache")

        cursor.execute(success)
    end = time.time()
    exectime = end - start
    return render_template('count.html', t=exectime)







@app.route('/')
def hello_world():
  return render_template('index.html')


@app.route('/getlat', methods=['GET'])
def randquery():
    rangfro = float(request.args.get('rangefrom1'))
    rangto = float(request.args.get('rangeto1'))
    num = float(request.args.get('nom1'))

    return randrange(rangfro,rangto,num)

if __name__ == '__main__':
  app.run()
