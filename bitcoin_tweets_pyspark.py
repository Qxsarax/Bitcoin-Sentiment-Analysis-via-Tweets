!pip install vaderSentiment

import pandas as pd
import re
import matplotlib.pyplot as plt
import seaborn as sns

from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer # for sentiment analysis

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql import types as t

spark = SparkSession.builder.appName('BitcoinTweets').getOrCreate()
# Inizialize a spark session

!wget https://proai-datasets.s3.eu-west-3.amazonaws.com/bitcoin_tweets.csv

dataset = pd.read_csv('/databricks/driver/bitcoin_tweets.csv', delimiter=",") 
# I import the dataframe that I will use for the analysis

df = spark.createDataFrame(dataset)
# Converting pandas df to Spark df

df = df.write.saveAsTable('bitcoin_tweets_analysis')

# I eliminate null data and duplicates, and select the columns that I need for the analysis

df = df.dropDuplicates(subset=['timestamp', 'text'])
df = df.dropna(subset=['timestamp', 'text'])

df = df.select('id', 'timestamp', 'replies', 'likes', 'retweets', 'text')
