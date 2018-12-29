from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.mllib.recommendation import ALS
from pymongo import MongoClient


sc = SparkContext(appName = "TrainedModel")
spark = SparkSession.builder.appName('recommend').config("spark.mongodb.input.uri","mongodb://10.150.0.4:27017/csvmongo.colcsv").getOrCreate()
data1 = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri","mongodb://10.150.0.4:27017/csvmongo.colcsv").load()
	
data1.createOrReplaceTempView("temp1")
data = spark.sql("SELECT userID,tutID,rating from temp1")
(training,test) = data.randomSplit([0.8,0.2])
test.createOrReplaceTempView("tmp2")
tstrdd = spark.sql("SELECT userID,tutID from tmp2")

print("--"*50)
print("\n Training of model start\n")
model = ALS.train(training,8,10)

model.save(sc,"hdfs://sparkm-m:8020/TEST/MODEL")

client = MongoClient('mongodb://<IP>:27017/')
db = client["MODELDB"]
db["modeltest"].insert({"model":"updated"})

print("--"*50)
print("\n tarining is over data cleaning start\n")