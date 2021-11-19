# Python Sentiment Analysis - Spark Streaming

<img src="https://upload.wikimedia.org/wikipedia/commons/thumb/f/f3/Apache_Spark_logo.svg/1200px-Apache_Spark_logo.svg.png" height="100"></img>
<img src="https://logos-world.net/wp-content/uploads/2020/04/Twitter-Logo.png" height="100"></img>

# Introduction
Sentiment Analysis is the process of using natural language Programming (NLP) techniques to understand the sentiment of input data. 

The training set used in this project consisted of thousands of tweets manually labeled by sentiment, with a numerical score.

The goal of the project is to develop a model to effectively determine sentiment of new tweets, as well as being able to do it with streaming data.

# Work-Flow
1. Training Data is cleaned up leveraging Spark (Urls, Stopword removal, tokenization, TF, IDF, StringIndexing)
1. Model is trained
1. Twitter's Streaming API feeds into Spark Streaming Model
1. Spark makes determinations with live streaming data straight from Twitter.com

<br>
<img src="https://spark.apache.org/docs/latest/img/streaming-arch.png" height="200"></img> 

