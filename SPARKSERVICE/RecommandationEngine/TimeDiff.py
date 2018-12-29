from pymongo import MongoClient
import json, pprint, requests, textwrap

usrCollection = "TESTUSR"
hdfsmodelCollection = "MODELDB"
usrcol = "movie"
modelcol = "modeltest"

def getClient():
	#Required to update IP Address
	return MongoClient('mongodb://10.150.0.3:27017/')


def getTimeStamp(dbstr,colstr):

	client = getClient()
	db = client[dbstr]
	last_updated = db[colstr].find().sort("_id",-1).limit(1)
	time_stamp = []
	for doc in last_updated:
	#	print("\nLast_Updated2: "+str(doc))
		time_stamp.append(doc['_id'].generation_time)
		
	#print str(time_stamp)
	return time_stamp
	
def getTimeStampByUserID(dbatr,colstr,userid):

    client = getClient()
    db = client[dbstr]
    last_updated = db[colstr].find({"userID":userid})
    time_stamp = []

    for doc in lat_updated:
        time_stamp.append(doc['_id'].generation_time)
        break
    return time_stamp


def updateInterval():

	time_stmap_user = getTimeStamp(usrCollection,usrcol)
	time_stamp_model = getTimeStamp(hdfsmodelCollection,modelcol)

	if time_stamp_model > time_stmap_user:
		print("Modle is updated no need to update model again")
		return True
	else:
		diff = time_stmap_user[0] - time_stamp_model[0]
		if diff.days == 0:
			print("Zero days need to find diff in hours")
			hours, remindar = divmod(diff.seconds, 3600)
			if hours >= 1:
				print("Need to update Model")
				if updateTrainModel() == "success":
					return False
				else:
					return False
			else:
				print("No need to update model work with old one")
				return True
		

def updateTrainModel():
	host = 'http://localhost:8998'
	data = {'file':'file:///home/gpawar916/TrainModel.py','conf':{'spark.jars.packages':'org.mongodb.spark:mongo-spark-connector_2.11:2.2.5'}}
	headers = {'Content-Type': 'application/json'}
	r = requests.post(host + '/batches', data=json.dumps(data), headers=headers)
	r.json()
	
	batch_id = r['id']
	output = "";
	while True:
		response = requests.get(host+'/batches/'+str(batch_id),headers=headers)
		if response['state'] == 'success':
			output = "success";
			break
		if response['state'] == 'dead' or response['state'] == 'fail':
			output = "fail";
			break
		sleep(20)
		
	return output


	
