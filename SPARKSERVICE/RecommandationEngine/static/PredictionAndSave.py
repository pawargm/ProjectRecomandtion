import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.mllib.recommendation import ALS
from pyspark.mllib.recommendation import MatrixFactorizationModel
from pymongo import MongoClient

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

print("UserID\n")
print(sys.argv)
userID = int(sys.argv[1])
print("userID  ========== "+str(userID)+"\n")
print("Start Recommandation\n")
tst = tstrdd.rdd.map(list)
tmp = tst.filter(lambda x:x[0]==userID).map(lambda x:(x[1])).collect()
try1 = tst.filter(lambda rating: rating[1] not in tmp).map(lambda x:(userID,x[1])).distinct()

print("--"*50)
print("\nGet cleaning is done\n")

#model = ALS.train(training,8,10)

#print("--"*50)
#print("\nMdel tarined\n")

client = MongoClient('mongodb://10.150.0.3:27017/')
db = client[DB]
res = db[PathCol].find()

tmppath = ""
for doc in res:
    tmppath =  str(doc['path'])
    break
print "Get Path From DB"+tmppath+"\n"
model = MatrixFactorizationModel.load(sc, tmppath)
result = model.predictAll(try1)
lst_prediction = result.takeOrdered(10,key=lambda x: -x[2])
print("--"*50)
print("\nGot all required result\n")
print("\n"+str(lst_prediction)+"\n")



client = MongoClient('mongodb://10.150.0.3:27017/')
db = client["RecommandationDB"]
res = db["RecommandationCol"]

print "Writing to DB"

lst_tmp = []
for i in lst_prediction:
    print("UserID: "+str(i.user)+"TutID: "+str(i.product)+"Rating: "+str(i.rating)+"\n")
    db["RecommandationCol"].insert({"userID":str(i.user),"tutID":str(i.product),"rating":str(i.rating)})
    tmp_tuple = (str(i.user),str(i.product),str(i.rating))
    lst_tmp.append(tmp_tuple)

print("\n"+str(lst_tmp)+" Writing to database over\n")
#recomandation_data = spark.createDataFrame(lst_tmp,["usrID","TutID","rating"])
#recomandation_data.write.format("com.mongodb.spark.sql.DefaultSource").mode("append").option("uri","mongodb://10.150.0.3:27017").option("database","RecommandationDB").option("collection", "RecommandationCol").save()
