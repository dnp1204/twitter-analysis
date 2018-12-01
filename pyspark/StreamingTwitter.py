import sys

from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
import pyspark.sql.functions as f
from pyspark.sql import SparkSession


if __name__ == "__main__":
    spark = SparkSession.builder\
        .appName("Find popular hastag in tweets")\
        .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/twitter.hashtags-stream")\
        .getOrCreate()

    logger = spark._jvm.org.apache.log4j
    logger.LogManager.getRootLogger().setLevel(logger.Level.FATAL)

    lines = spark \
        .readStream \
        .format("socket") \
        .option("host", "localhost") \
        .option("port", 7000) \
        .load()

    words = lines \
        .select(f.explode(f.split(lines.value, " ")).alias("word")).createTempView("words")
    words2 = spark.sql("SELECT * from words WHERE word LIKE '#_%'")

    wordCounts = words2.groupBy("word").count().sort('count', ascending=False)

    def foreach_batch_function(df, epoch_id):
        df.show()
        df.write.format("com.mongodb.spark.sql").mode("overwrite").save()

    query = wordCounts \
        .writeStream \
        .outputMode("complete") \
        .foreachBatch(foreach_batch_function) \
        .start()

    query.awaitTermination()
