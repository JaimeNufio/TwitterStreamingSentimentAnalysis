# TweetRead.py
# This first python script doesn’t use Spark at all:
import os
import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import socket
import json
 
#read keys from a seperate file, for github privacy
with open("config.json","r") as key:
    keys = json.load(key)
    access_token = keys['access_token']
    access_secret = keys['access_secret']
    consumer_key = keys['consumer_key']
    consumer_secret = keys['consumer_secret']

#Boiler plate code. Socket Logic 
class TweetsListener(StreamListener):

    def __init__(self, csocket):
        self.client_socket = csocket

    #to me, it makes sense to clean data before it's passed to spark.
    #reduce workload in the long run by doing it once
    def clean_tweet(tweet):
        tweet = re.sub(r'@[^\s]+','',tweet) # remove @mentions
        tweet = re.sub()r"\S*http?:\S*",'',tweet) #remove URL
        tweet = re.sub(r'[^A-Za-z0-9 ]','',tweet) #remove special
        tweet = re.sub(r'[ \n\r]{2,}',' ',tweet) #remove exccess spaces
        tweet = tweet.strip().lower() #cleanup space remove, force lower

    def on_data(self, data):
        try:
            tweet = json.loads(data)
            tweet = clean_tweet(tweet['text'])
            print("Tweet: {}".format(tweet))

            self.client_socket.sendall(tweet.encode('utf-8'))
            return True
        except BaseException as e:
            print("Error on_data: %s" % str(e))
        return True
 
    def on_error(self, status):
        print(status)
        return True
 
def sendData(c_socket):
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)
 
    twitter_stream = Stream(auth, TweetsListener(c_socket))
    twitter_stream.filter(track=['trump'])
 
if __name__ == "__main__":
    s = socket.socket()     
    host = "localhost"      
    port = 5555            
    s.bind((host, port))
 
    print("Escuchando sobre puerto: {}".format(port))
 
    s.listen(1)                 
    c, addr = s.accept()        
 
    #print( "Encontro: {}".format((addr[0])))
 
    sendData( c )
