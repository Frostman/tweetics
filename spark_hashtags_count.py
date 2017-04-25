#!/usr/bin/env python

# based on https://github.com/JasonSanchez/spark-streaming-twitter-kafka/blob/master/spark-stream-tweets.py

import json
import sys

import pyspark
from pyspark import streaming
from pyspark.streaming import kafka


def get_people_with_hashtags(tweet):
    data = json.loads(tweet)
    try:
        hashtags = ["#" + hashtag["text"] for hashtag in data['entities']['hashtags']]
        if not hashtags:
            return ()
        author = data['user']['screen_name']
        mentions = ["@" + user["screen_name"] for user in data['entities']['user_mentions']]
        people = mentions + [author]
        return (people, hashtags)
    except KeyError:
        return ()


def filter_out_unicode(x):
    hashtags = []
    for hashtag in x[1]:
        try:
            hashtags.append(str(hashtag))
        except UnicodeEncodeError:
            pass
    return (x[0], hashtags)


def flatten(x):
    all_combinations = []
    for person in x[0]: # people
        for hashtag in x[1]: # hashtags
            main_author_flag = 0 if "@" in person else 1
            all_combinations.append((hashtag, (main_author_flag, {person})))

    return all_combinations


if __name__ == "__main__":
    if len(sys.argv) != 7:
        print "Usage: spark_hashtags_count.py <spark_master> <zk_quorum> <topic_name> <min_hashtag_counts> <batch_duration> <save_to>"
        print "Example: spark_hashtags_count.py local[4] zk-kafka-1-0.zk-kafka-1:2181,zk-kafka-1-1.zk-kafka-1:2181,zk-kafka-1-2.zk-kafka-1:2181 twitter-stream 0 5 hdfs://hdfs-namenode:8020/demo"
        print "<spark_master> - spark master to use: local[4] or spark://HOST:PORT"
        print "<zk_quorum> - zk quorum to connect: zk-kafka-1-0.zk-kafka-1:2181,zk-kafka-1-1.zk-kafka-1:2181,zk-kafka-1-2.zk-kafka-1:2181"
        print "<topic_name> - kafka topic name: twitter-stream"
        print "<min_hashtag_counts> - filter out hashtags with less then specified count"
        print "<batch_duration> - spark streaming batch duration ~ how often data will be written"
        print "<save_to> - save as text files to: hdfs://hdfs-namenode:8020/demo"
        exit(-1)

    spark_master = sys.argv[1]
    zk_quorum = sys.argv[2]
    topic_name = sys.argv[3]
    min_hashtag_counts = int(sys.argv[4])
    batch_duration = int(sys.argv[5])
    save_to = sys.argv[6]

    sc = pyspark.SparkContext("local[2]", appName="TweeTics")
    ssc = streaming.StreamingContext(sc, batch_duration)

    tweets = kafka.KafkaUtils.createStream(ssc, zk_quorum, "tweetics-consumer", {topic_name: 1})

    # tweet -> Kafka -> Spark -> parse json -> ([people], [hashtags])
    #    (w/o unicode and filter out unused hashtags)
    # -> flatten to (hashtag, (main_author, {person}))
    # -> reduce by hashtags to list of authors and count of tweets
    #    (filtered out hashtags with less then specified count)

    lines = tweets.map(lambda x: get_people_with_hashtags(x[1])).filter(lambda x: len(x) > 0)
    hashtags = lines.map(filter_out_unicode)
    flat_hashtags = hashtags.flatMap(flatten)
    hash_tag_authors_and_counts = flat_hashtags.reduceByKey(lambda a, b: (a[0] + b[0], a[1] | b[1]))
    top_hashtags = hash_tag_authors_and_counts.filter(lambda x: x[1][0] >= min_hashtag_counts)
    top_hashtags = top_hashtags.transform(lambda rdd: rdd.sortByKey(ascending=False, keyfunc=lambda x: x[1][0]))

    top_hashtags.pprint()
    top_hashtags.saveAsTextFiles(save_to)

    ssc.start()
    ssc.awaitTermination()
