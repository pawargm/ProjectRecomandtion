import requests 
import json
#from websocket import create_connection
import time
import os
from pymongo import MongoClient



Path1 = "/TEST/MODEL1/"
Path2 = "/TEST/MODEL2/"

DB = "PATHDB"
PathCol = "PATHCOL"

def getClient():
        #Required to update IP Addres
    return MongoClient('mongodb://10.150.0.3:27017/')



def use_path():
    
    host = 'http://localhost:8998'
    data = {'file':'file:///home/gpawar916/TrainModel1.py','conf':{'spark.jars.packages':'org.mongodb.spark:mongo-spark-connector_2.11:2.2.5'}}
    headers = {'Content-Type': 'application/json'}
    r = requests.post(host + '/batches', data=json.dumps(data), headers=headers)
    batch_id = r.json()['id']
    output = "";
                            
    while True:
    
        response = requests.get(host+'/batches/'+str(batch_id),headers=headers)
        if response.json()['state'] == 'success':
            output = "success";
            break
        if response.json()['state'] == 'dead' or response.json()['state'] == 'fail':
            output = "fail";
            break
        time.sleep(20)
    
    if output == "success":
        print "SuucessFull"
    else:
        print "Failed"





use_path()


