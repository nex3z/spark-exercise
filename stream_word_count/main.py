import logging

from pyspark import SparkContext
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as funs


def main():
    spark = SparkSession.builder \
        .master('local') \
        .appName('nyc-taxi') \
        .config('spark.executor.memory', '1gb') \
        .getOrCreate()
    sc: SparkContext = spark.sparkContext
    sc.setLogLevel('WARN')
    logger.info("app_id = {}".format(sc.applicationId))

    df_line = spark.readStream.format('kafka') \
        .option('kafka.bootstrap.servers', 'localhost:9092') \
        .option('subscribe', 'word-count') \
        .option('startingOffsets', 'latest') \
        .load() \
        .selectExpr('CAST(value AS STRING)')

    df_word = df_line \
        .select(funs.explode(funs.split(df_line.value, " ")).alias("word"))

    df_word = df_word \
        .withColumn('word', funs.regexp_replace('word', '[^a-zA-Z0-9]', '')) \
        .filter(df_word['word'] != '') \
        .selectExpr('LOWER(word) AS word') \
        .withColumn('process_time', funs.current_timestamp())

    df_grouped = df_word.groupBy(funs.window('process_time', '20 seconds', '10 seconds'), 'word').count()

    write_stream(df_grouped)


def write_stream(df: DataFrame) -> None:
    df.writeStream \
        .outputMode('complete') \
        .format('console') \
        .option('truncate', False) \
        .start() \
        .awaitTermination()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,format='%(asctime)s %(levelname)s %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    logger = logging.getLogger('main')
    main()
