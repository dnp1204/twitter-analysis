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
        .appName("Find popular location in twitter")\
        .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/twitter.tweets")\
        .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/twitter.locations")\
        .getOrCreate()

    # set log level
    logger = spark._jvm.org.apache.log4j
    logger.LogManager.getRootLogger().setLevel(logger.Level.FATAL)

    # read from MongoDB collection
    df = spark.read.format("com.mongodb.spark.sql").load()
    df.printSchema()

    locations = df.select(f.col('location')) \
        .groupBy('location') \
        .count() \
        .sort('count', ascending=False)

    locations.show()

    print('Saving result to database...')
    locations.write.format("com.mongodb.spark.sql").mode("overwrite").save()
