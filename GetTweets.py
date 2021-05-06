import os
import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import socket
import json
import re
 
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
    def clean_tweet(self,tweet):
        tweet = re.sub(r'^RT ','',tweet) # remove the RT from retweets
        tweet = re.sub(r'@[^\s]+','',tweet) # remove @mentions
        tweet = re.sub(r"\S*https?:\S*",'',tweet) #remove URL
        tweet = re.sub(r'\\+u\S*','',tweet) #unicode
        tweet = re.sub(r'[^A-Za-z0-9 ]','',tweet) #remove special
        tweet = re.sub(r'[ \n\r]{2,}',' ',tweet) #remove exccess spaces
        tweet = tweet.strip().lower() #cleanup space remove, force lower
        return tweet

    def on_data(self, data):
        print("ONDATA")
        try:
            #Dump the json
            tweet = json.loads(data)
            #print(tweet)

            #Retweets are annoyingly differently formatted
            if 'RetweetedStatus' in tweet:
                print("Retweet:")
                tweet = tweet['retweeted_status']['extended_tweet']['full_text']
            else:
                print("Original Tweet:")
                if 'extended_tweet' in tweet:
                    tweet = tweet['extended_tweet']['full_text']
                else:
                    tweet = tweet['text']
            print("Raw Tweet:",tweet)
            tweet = self.clean_tweet(str(tweet))
            print("Cleaned Tweet: {}".format(tweet))

            tweet+="\r\n"

            self.client_socket.sendall(tweet.encode('utf-8'))
            return True
        except Exception as e:
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
 
    print("Listening on Port {}.".format(port))
 
    s.listen(1)                 
    c, addr = s.accept()        
 
    sendData( c )
