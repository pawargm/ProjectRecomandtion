# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from django.shortcuts import render

from django.http import JsonResponse
from django.http import HttpResponse
from TimeDiff import updateInterval
from TimeDiff import getClient
from TimeDiff import getTimeStamp
from TimeDiff import getTimeStampByUserID
from pymongo import MongoClient
from bson.json_util import dumps
from django.http import JsonResponse

import requests 
import json
#from websocket import create_connection
import time
import os

userRec = "RecommandationDB"
colRec = "RecommandationCol"


#IMPNOTE:==============
#1. Need to change IP address accordingly to mongodb setup
def get_connection():

	connection = getClient()
	return connection


def remove_by_id(db,col,userid):

	con = get_connection()
	db1 = con[db]	
	db1[col].remove({"userID":userid})


def data_present(userid):

	con = get_connection()
	db1 = con[userRec]
	lst_records = db1[colRec].find({"userID":str(userid)})

	if lst_records.count() != 0:
		return True
	else:
		return False


def delete_stale_result(req):

	#finding time stamp of last updated database 
	#time_stamp_model is for last modify trained model
	#time_stamp_recom is for last modifyed recommadation result database
        userID = int(req.GET['arg'])
	time_stamp_model = getTimeStamp("MODELDB","modeltest")
	time_stamp_recom = getTimeStampByUserID("RecommandationDB","RecommandationCol",userID)#getTimeStamp("RecommandationDB","RecommandationCol")

	#userID = int(req.GET['arg'])
        print len(time_stamp_model)
        print len(time_stamp_recom)
        if len(time_stamp_model) == 0 or len(time_stamp_recom) == 0:
            print "in null condition"
            return HttpResponse("predictiononly");

	#Check First required result is database
	if data_present(userID):

		if time_stamp_model > time_stamp_recom:
			print("Modle is updated need for predittionresult")
                        remove_by_id("RecommandationDB","RecommandationCol",userID)
			return HttpResponse("predictiononly");
		else:
			diff = time_stamp_recom[0] - time_stamp_model[0]
			if diff.days == 0:
				print("Zero days need to find diff in hours")
				hours, remindar = divmod(diff.seconds, 3600)
				if hours >= 1:
					print("Need to update Model")
					#need to change hdfs according to its path
					#os.system("hdfs dfs -rm /TEST/")
					#Remove record from result becoz it is too old to use i.e stale
					remove_by_id("RecommandationDB","RecommandationCol",userID)
					return HttpResponse("predictiononly")
				else:
					print("No need to update model work with data old one")
					return HttpResponse("justpolling")
			else:
				print("Days")
				print(diff.days)
				#need to modify path hdfs
				#os.system("hdfs dfs -rm /TEST/")
				remove_by_id("RecommandationDB","RecommandationCol",userID)
				return HttpResponse("predictiononly")

	else:
	
		if time_stamp_model > time_stamp_recom:
			print("Modle is updated no need to update model again")
			return HttpResponse("predictiononly");
		else:
			diff = time_stamp_recom[0] - time_stamp_model[0]
			if diff.days == 0:
				print("Zero days need to find diff in hours")
				hours, remindar = divmod(diff.seconds, 3600)
				if hours >= 1:
					#need to modify path of hdfs
					#os.system("hdfs dfs -rm /TEST/")
					print("Need to update Model")
					#remove_by_id("RecommandationDB","RecommandationCol",userID)
					return HttpResponse("predictiononly")
				else:
					print("No need to update model work with old one")
					return HttpResponse("predictiononly")
			else:
				print("Days")
				print(diff.days)
				#need to update path of hdfs
				#os.system("hdfs dfs -rm /TEST/")
				#remove_by_id("RecommandationDB","RecommandationCol",userID)
				return HttpResponse("predictiononly")
						


def train_predict_model(req):
			
	#print("Executing sleep=============================")
	#time.sleep(60);		
	#return HttpResponse("successful")
	
	#get userID from REST Call
	userID = int(req.GET['arg'])
	host = 'http://localhost:8998'
	data = {'file':'file:///home/gpawar916/TrainModel.py','conf':{'spark.jars.packages':'org.mongodb.spark:mongo-spark-connector_2.11:2.2.5'},"args":[userID]}
	headers = {'Content-Type': 'application/json'}
	r = requests.post(host + '/batches', data=json.dumps(data), headers=headers)
        print "reslut=================="
        print r
        print r.json()
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
		#https://stackoverflow.com/questions/19674311/json-serializing-mongodb
		# Return Data searchecd in mongo
		if prediction(userID) == "success":
			return HttpResponse("successful")
		else:
			return HttpResponse("failed")
	else:
		return HttpResponse("failed")

def predict_model(req):	

	#get userid for which required to get recommandation
	#time.sleep(60);
	#return HttpResponse("successful")
	userid = int(req.GET['arg'])
	host = 'http://localhost:8998'
	data = {'file':'file:///home/gpawar916/PredictionAndSave.py','conf':{'spark.jars.packages':'org.mongodb.spark:mongo-spark-connector_2.11:2.2.5'},"args":[userid]}
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
		return HttpResponse("successful")
	else:
		return HttpResponse("failed")

def prediction(userid):	

	host = 'http://localhost:8998'
	data = {'file':'file:///home/gpawar916/PredictionAndSave.py','conf':{'spark.jars.packages':'org.mongodb.spark:mongo-spark-connector_2.11:2.2.5'},"args":[userid]}
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
		return "success"
	else:
		return "fail"


	


def response_springboot(req):

	response = {'test':'success'}
	time.sleep(10)
	return JsonResponse(response)

def response_springbootarg(req):
	print"==============="
	print req.GET['arg']
	#response = {'args':req.GET['arg']}
	user_id = req.GET['arg']
	connection = getClient()
	db = connection[userRec]
	lst_records = db[colRec].find({"userID":str(user_id)})

	if lst_records.count() != 0:
		lst = []
		for doc in lst_records:
			tmp = dumps(doc)
			lst.append(tmp)
		res_dir = {'data':lst}
		return JsonResponse(res_dir)
	else:
		response = {'data':'not_present'}
		return JsonResponse(response)

def try_websock(req):
	ws = create_connection("wss://127.0.0.1:8000/testarg/")
	res = ws.recv()
	print(res)
	ws.close()

def get_recommandation(req):


	user_id = req.POST['userid']
	if updateInterval():
		#Model is already updated
		connection = getClient()
		db = connection[userRec]
		lst_records = db[colRec].find({"user_id":user_id})
		
		#User Id present in database then use this result as final output else use saved model to predict recommandation
		
		if lst_records.count() !=0:
			
			lst = []
			for doc in lst_records:
				tmp = dumps(doc)
				lst.append(tmp)
			res_dir = {'data':lst}
			return JsonResponse(res_dir)
			#https://stackoverflow.com/questions/19674311/json-serializing-mongodb
			#return Json Response for DATA searched in Mongo
		else:
			
			host = 'http://localhost:8998'
			data = {'file':'file:///home/gpawar916/TrainModel.py','conf':{'spark.jars.packages':'org.mongodb.spark:mongo-spark-connector_2.11:2.2.5'},"args":[user_id]}
			headers = {'Content-Type': 'application/json'}
			r = requests.post(host + '/batches', data=json.dumps(data), headers=headers)
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
			if output == "success":
				#https://stackoverflow.com/questions/19674311/json-serializing-mongodb
				# Return Data searchecd in mongo
				lst_records = db[colRec].find({"user_id":user_id})
				if len(lst_records) == 0:
					#return that data not saved in mongo
					res_dir = {'data':[]}
					return JsonResponse(res_dir)
				else:
					lst = []
					for doc in lst_records:
						tmp = dumps(doc)
						lst.append(tmp)
					
					res_dir = {'data':lst}
					return JsonResponse(res_dir)
					#return Json Response for DATA searched in Mongo
			else:
				res_dir = {'data':[]}
				return JsonResponse(res_dir)
