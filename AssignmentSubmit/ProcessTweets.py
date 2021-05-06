import time
import json
from pyspark.streaming import StreamingContext
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, Row

#   There is a jupyter notebook I did this work in first, I'm just 
#   transfering it here.
from pyspark.ml.feature import Tokenizer, StopWordsRemover, CountVectorizer, IDF, HashingTF, StringIndexer
from pyspark.ml import Pipeline,PipelineModel
from pyspark.ml.classification import LogisticRegression 
import re

#this is for the database store requirement
#nice library for Google's Firebase!
import pyrebase

#read the config file
firebaseConfig = {}
with open("config.json","r") as key:
    key = json.load(key)['firebaseConfig']
    firebaseConfig = {
        "apiKey": key['apiKey'],
        "authDomain": key['authDomain'],
        "databaseURL": key['databaseURL'],
        "projectId": key['projectId'],
        "storageBucket": key['storageBucket'],
        "messagingSenderId": key['messagingSenderId'],
        "appId": key['appId'],
        "measurementId": key['measurementId']  
    }

firebase = pyrebase.initialize_app(firebaseConfig)
db = firebase.database()

sc = SparkContext("local[10]", "Tweets") # My PC has 12 cores :)
ssc = StreamingContext(sc, 2)
IP = "localhost"
Port = 5555

spark = SparkSession(sc)
lines = ssc.socketTextStream(IP, Port)

#prep data
input = sc.textFile("./train.csv")
dataTrain = ( input.map(lambda x: (x.split('","')[0][1],x.split('","')[5])) # get sentiment and text
    .map(lambda x: (x[0],re.sub(r'@[^\s]+','',x[1])))  #remove mentions
    .map(lambda x: (x[0],re.sub(r"\S*http?:\S*",'',x[1])))  #remove urls
    .map(lambda x: (x[0],re.sub(r'[^A-Za-z0-9 ]','',x[1])))  #remove special
    .map(lambda x: (x[0],re.sub(r'[ ]{2,}',' ',x[1])))  #remove double+ spaces
    .map(lambda x: (x[0],x[1].strip().lower())) #strip space, force to lowercase
    .map(lambda x: (x[0],x[1]))
       )

#again, written in a notebook. 
#I had trouble saving the model and loading it, so we're just gonna
#follow the exact method, and train it on start. 
#obviously, bad practice. But the jupyter notebook shows we get 87% accuracy
tokenizer = Tokenizer().setInputCol('text').setOutputCol('words')
remover = StopWordsRemover(inputCol='words',outputCol='clean')
TF = HashingTF(numFeatures=2**16,inputCol="clean",outputCol='tf')
IDF = IDF(inputCol='tf',outputCol="features")
StringIndex = StringIndexer(inputCol="sentiment",outputCol="label")
Model = LogisticRegression(maxIter=300)

dfTrain = dataTrain.toDF(['sentiment','text'])

Model = LogisticRegression(maxIter=300)
pipeline = Pipeline(stages=[tokenizer,remover,TF,IDF,StringIndex,Model])

pipelineFit = pipeline.fit(dfTrain)

#pass lines of DF to firebase
def toDB(line):
    db.push(line.asDict())
    print(line.asDict())

#pass tweets through pipeline
def processTweet(tweet):
    try:
        tweet = tweet.map(lambda w: Row(text=w))
        df = spark.createDataFrame(tweet)

        print("Predict!")
        predict = pipelineFit.transform(df)
        ofInterest = predict.select("text","prediction")
        ofInterest.foreach(toDB)
    
    except:
        print("Failure to read/predict")



lines.filter(lambda x:len(x)>0).foreachRDD(processTweet)
ssc.start()
ssc.awaitTermination()
