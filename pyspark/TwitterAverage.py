# To run this example use:
# ./bin/spark-submit --master "local[4]"  \
#                    --conf "spark.mongodb.input.uri=mongodb://127.0.0.1/test.coll?readPreference=primaryPreferred" \
#                    --conf "spark.mongodb.output.uri=mongodb://127.0.0.1/test.coll" \
#                    --packages org.mongodb.spark:mongo-spark-connector_2.11:2.2.0 \
#                    introduction.py

import pyspark.sql.functions as f
from pyspark.sql import SparkSession

if __name__ == "__main__":

    spark = SparkSession.builder\
        .appName("Find popular word in tweets")\
        .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/twitter.tweets")\
        .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/twitter.average")\
        .getOrCreate()

    # set log level
    logger = spark._jvm.org.apache.log4j
    logger.LogManager.getRootLogger().setLevel(logger.Level.FATAL)

    # read from MongoDB collection
    df = spark.read.format("com.mongodb.spark.sql").load()
    df.printSchema()

    words = df.withColumn('word', f.explode(
        (f.split(f.col('text'), ' ')))).createTempView("words")

    word_average = spark.sql("SELECT * from words WHERE word LIKE '_%'") \
        .groupBy('word') \
        .count() \
        .select(f.mean(f.col('count')).alias('word_average')) \
        .withColumn('row_index', f.monotonically_increasing_id())

    word_average.show()

    emojis = df.select(f.explode(f.col('emojis')).alias('emoji')) \
        .groupBy('emoji') \
        .count() \
        .select(f.mean(f.col('count')).alias('emoji_average')) \
        .withColumn('row_index', f.monotonically_increasing_id())

    emojis.show()

    hashtags = df.select(f.explode(f.col('hashtags')).alias('hashtag')) \
        .groupBy('hashtag') \
        .count() \
        .select(f.mean(f.col('count')).alias('hashtags_average')) \
        .withColumn('row_index', f.monotonically_increasing_id())

    hashtags.show()

    locations = df.select(f.col('location')) \
        .groupBy('location') \
        .count() \
        .select(f.mean(f.col('count')).alias('locations_average')) \
        .withColumn('row_index', f.monotonically_increasing_id())

    locations.show()

    result = word_average \
        .join(emojis, on=["row_index"]) \
        .join(hashtags, on=["row_index"]) \
        .join(locations, on=["row_index"]) \
        .sort("row_index").drop("row_index")

    print('Saving result to database...')
    result.write.format("com.mongodb.spark.sql").mode("overwrite").save()
