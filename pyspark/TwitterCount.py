# To run this example use:
# ./bin/spark-submit --master "local[4]"  \
#                    --conf "spark.mongodb.input.uri=mongodb://127.0.0.1/test.coll?readPreference=primaryPreferred" \
#                    --conf "spark.mongodb.output.uri=mongodb://127.0.0.1/test.coll" \
#                    --packages org.mongodb.spark:mongo-spark-connector_2.11:2.2.0 \
#                    introduction.py

import pyspark.sql.functions as f
from pyspark.sql import SparkSession


def read_tweets(spark):
    # read from MongoDB collection
    df = spark.read.format("com.mongodb.spark.sql").load()
    df.printSchema()
    return df


def save_to_mongodb(result, collection_name):
    print('Saving ' + collection_name + ' result to database...\n')
    result.write.format("com.mongodb.spark.sql") \
        .option("spark.mongodb.output.uri", "mongodb://127.0.0.1/twitter." + collection_name) \
        .mode("overwrite") \
        .save()


def find_popular_words(df):
    df.withColumn('word', f.explode(
        (f.split(f.col('text'), ' ')))).createTempView("words")

    result = spark.sql("SELECT * from words WHERE word LIKE '_%'") \
        .groupBy('word') \
        .count() \
        .sort('count', ascending=False)

    result.show()

    save_to_mongodb(result, "words")

    return result


def find_popular_emoji(df):
    emojis = df.select(f.explode(f.col('emojis')).alias('emoji')) \
        .groupBy('emoji') \
        .count() \
        .sort('count', ascending=False)

    emojis.show()

    save_to_mongodb(emojis, "emojis")

    return emojis


def find_popular_locations(df):
    locations = df.select(f.col('location')) \
        .groupBy('location') \
        .count() \
        .sort('count', ascending=False)

    locations.show()

    save_to_mongodb(locations, "locations")

    return locations


def find_popular_hashtags(df):
    hashtags = df.select(f.explode(f.col('hashtags')).alias('hashtag')) \
        .groupBy('hashtag') \
        .count() \
        .sort('count', ascending=False)

    hashtags.show()

    save_to_mongodb(hashtags, "hashtags")

    return hashtags


def find_average(avg_dfs):
    temps = []

    for avg_df in avg_dfs:
        df = avg_df[0]
        column_name = avg_df[1]

        temp = df.select(f.mean(f.col('count')).alias(column_name)) \
            .withColumn('row_index', f.monotonically_increasing_id())
        temp.show()
        temps.append(temp)

    if len(temps) > 0:
        result = temps[0]

        for temp in temps[1:]:
            result = result.join(temp, on=["row_index"]) \

        result = result.drop("row_index")
        result.show()

        save_to_mongodb(result, "average")

        return result

    return None


if __name__ == "__main__":

    spark = SparkSession.builder\
        .appName("Find popular word in tweets")\
        .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/twitter.tweets")\
        .getOrCreate()

    # set log level
    logger = spark._jvm.org.apache.log4j
    logger.LogManager.getRootLogger().setLevel(logger.Level.FATAL)

    df = read_tweets(spark)

    words = find_popular_words(df)
    emoji = find_popular_emoji(df)
    locations = find_popular_locations(df)
    hashtags = find_popular_hashtags(df)

    avg_dfs = [[words, 'word_avg'], [emoji, 'emoji_avg'], [
        locations, 'location_avg'], [hashtags, 'hashtags_avg']]

    find_average(avg_dfs)
