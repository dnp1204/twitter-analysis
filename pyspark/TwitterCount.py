# To run this example use:
# ./bin/spark-submit --master "local[4]"  \
#                    --conf "spark.mongodb.input.uri=mongodb://127.0.0.1/test.coll?readPreference=primaryPreferred" \
#                    --conf "spark.mongodb.output.uri=mongodb://127.0.0.1/test.coll" \
#                    --packages org.mongodb.spark:mongo-spark-connector_2.11:2.2.0 \
#                    introduction.py

import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.ml.feature import StopWordsRemover, Tokenizer


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


def find_popular_words_remove_stopwords(df):
    stopWords = [
        'a',
        'about',
        'above',
        'after',
        'again',
        'against',
        'all',
        'am',
        'an',
        'and',
        'any',
        'are',
        'as',
        'at',
        'be',
        'because',
        'been',
        'before',
        'being',
        'below',
        'between',
        'both',
        'but',
        'by',
        'could',
        'did',
        'do',
        "don't",
        "don’t",
        'does',
        'doing',
        'down',
        'during',
        'each',
        'few',
        'for',
        'from',
        'further',
        'had',
        'has',
        'have',
        'having',
        'he',
        "he'd",
        'he’d',
        "he'll",
        'he’ll',
        "he's",
        'he’s',
        'her',
        'here',
        "here's",
        'here’s',
        'hers',
        'herself',
        'him',
        'himself',
        'his',
        'how',
        "how's",
        'how’s',
        'i',
        "i'd",
        'i’d',
        "i'll",
        'i’ll',
        "i'm",
        'i’m',
        "i've",
        'i’ve',
        'if',
        'in',
        'into',
        'is',
        'it',
        "it's",
        "it’s",
        'its',
        'itself',
        "let's",
        'let’s',
        'me',
        'more',
        'most',
        'my',
        'myself',
        'no',
        'nor',
        'of',
        'on',
        'once',
        'only',
        'or',
        'other',
        'ought',
        'our',
        'ours',
        'ourselves',
        'out',
        'over',
        'own',
        'same',
        'she',
        "she'd",
        'she’d',
        "she'll",
        'she’ll',
        "she's",
        'she’s',
        'should',
        'so',
        'some',
        'such',
        'than',
        'that',
        "that's",
        'that’s',
        'the',
        'their',
        'theirs',
        'them',
        'themselves',
        'then',
        'there',
        "there's",
        'there’s',
        'these',
        'they',
        "they'd",
        'they’d',
        "they'll",
        'they’ll',
        "they're",
        'they’re',
        "they've",
        'they’ve',
        'this',
        'those',
        'through',
        'to',
        'too',
        'under',
        'until',
        'up',
        'very',
        'was',
        'we',
        "we'd",
        'we’d',
        "we'll",
        'we’ll',
        "we're",
        'we’re',
        "we've",
        'we’ve',
        'were',
        'what',
        "what's",
        'what’s',
        'when',
        "when's",
        'when’s',
        'where',
        "where's",
        'where’s',
        'which',
        'while',
        'who',
        "who's",
        'who’s',
        'whom',
        'why',
        "why's",
        'with',
        'would',
        'you',
        "you'd",
        'you’d',
        "you'll",
        'you’ll',
        "you're",
        'you’re',
        "you've",
        'you’ve',
        'your',
        'yours',
        'yourself',
        'yourselves',
        'just',
        'not',
        'will',
        'get',
        'got',
        'really',
        'like',
        'can',
        'go',
        'see',
        'good',
        'great',
        'want',
        'know',
        'now',
        'one',
        'two'
    ]

    tokenizer = Tokenizer(inputCol="text", outputCol="raw_tokens")
    remover = StopWordsRemover(
        inputCol="raw_tokens", outputCol="filtered", stopWords=stopWords)

    tokenized = tokenizer.transform(df)
    removed = remover.transform(tokenized)

    removed.select(f.explode(f.col('filtered')).alias('word')) \
        .createTempView("important_words")

    result = spark \
        .sql("SELECT * from important_words WHERE word LIKE '_%' AND word NOT LIKE '%#%' AND word NOT LIKE '%@%' AND word NOT LIKE '%&%' AND word NOT LIKE '%-%'") \
        .groupBy('word') \
        .count() \
        .sort('count', ascending=False)

    result.show()

    save_to_mongodb(result, "important-words")

    return result


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

        save_to_mongodb(result, "average_counts")

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
    important_words = find_popular_words_remove_stopwords(df)

    avg_dfs = [
        [words, 'word_avg'],
        [emoji, 'emoji_avg'],
        [locations, 'location_avg'],
        [hashtags, 'hashtags_avg'],
        [important_words, 'important_words_avg']
    ]

    find_average(avg_dfs)
