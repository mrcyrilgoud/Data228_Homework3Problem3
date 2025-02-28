

import logging
import praw
import re
import time
import json
from kafka import KafkaProducer
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

spark = SparkSession.builder \
    .appName("RedditSentimentPipeline") \
    .config("spark.sql.warehouse.dir", "hdfs://localhost:10000/user/hive/warehouse") \
    .config("spark.sql.hive.metastore.uris", "thrift://localhost:10000") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sql("CREATE DATABASE IF NOT EXISTS hw3_db")
spark.sql("USE hw3_db")

spark.sql("""
CREATE TABLE IF NOT EXISTS reddit_sentiment (
    title STRING,
    sentiment STRING,
    url STRING
) STORED AS PARQUET
""")

analyzer = SentimentIntensityAnalyzer()

def clean_text(text):
    text = re.sub(r"http\S+|www\S+|https\S+", '', text, flags=re.MULTILINE)  # Remove URLs
    text = re.sub(r"[^\w\s]", '', text)  # Remove punctuation
    return text.strip()

def get_sentiment(text):
    sentiment_score = analyzer.polarity_scores(text)
    if sentiment_score['compound'] >= 0.05:
        return 'positive'
    elif sentiment_score['compound'] <= -0.05:
        return 'negative'
    else:
        return 'neutral'

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

reddit = praw.Reddit(
    client_id="",
    client_secret="",
    user_agent=""
)

print("🚀 Streaming Reddit posts...")

keyword = "advice"
try:
    for submission in reddit.subreddit("all").stream.submissions():
        title = clean_text(submission.title)

        if keyword.lower() in title.lower():
            sentiment = get_sentiment(title)
            data = {"title": title, "sentiment": sentiment, "url": submission.url}
            producer.send("reddit_stream", value=data)
            print(f"📌 Published: {data}")

            time.sleep(2)

except KeyboardInterrupt:
    print("❌ Stream stopped by user.")

# Kafka Consumer & Spark Streaming
schema = StructType([
    StructField("title", StringType(), True),
    StructField("sentiment", StringType(), True),
    StructField("url", StringType(), True)
])

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "reddit_stream") \
    .load()

df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# Write to Console
query = df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

def write_to_hive(batch_df, batch_id):
    batch_df.write.format("hive").mode("append").saveAsTable("reddit_sentiment")

df.writeStream \
    .foreachBatch(write_to_hive) \
    .option("checkpointLocation", "/tmp/hive_checkpoint") \
    .start()

query.awaitTermination()

