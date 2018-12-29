from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.mllib.recommendation import ALS
from pymongo import MongoClient
import os

Path1 = "/TEST/MODEL1/"
Path2 = "/TEST/MODEL2/"

DB = "PATHDB"
PathCol = "PATHCOL"



sc = SparkContext(appName = "TrainedModel")
spark = SparkSession.builder.appName('recommend').config("spark.mongodb.input.uri","mongodb://10.150.0.3:27017/newdb123.rating").getOrCreate()
data1 = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri","mongodb://10.150.0.3:27017/newdb123.rating").load()
	
data1.createOrReplaceTempView("temp1")
data = spark.sql("SELECT userID,tutID,rating from temp1")
(training,test) = data.randomSplit([0.8,0.2])
test.createOrReplaceTempView("tmp2")
tstrdd = spark.sql("SELECT userID,tutID from tmp2")

print("--"*50)
print("\n Training of model start\n")
model = ALS.train(training,8,10)

client = MongoClient('mongodb://10.150.0.3/')
db = client[DB]
res = db[PathCol].find()

tmppath = ""
newpath = ""

for doc in res:
    tmppath =  str(doc['path'])
    break

if tmppath == Path1:
    newpath = Path2
else:
    newpath = Path1


model.save(sc,newpath)
print "Saved new Model to Pth"+newpath
db[PathCol].update({"path":tmppath},{"path":newpath})
print "Modified path in DB"
os.system("hdfs dfs -rmr "+tmppath)
print "Removed Old Model"


client = MongoClient('mongodb://10.150.0.3:27017/')
db = client["MODELDB"]
db["modeltest"].insert({"model":"updated"})

print("--"*50)
print("\n tarining is over data cleaning start\n")
