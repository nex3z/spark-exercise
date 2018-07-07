from pyspark.sql import SparkSession, types, Row, functions
from graphframes import GraphFrame


def main():
    spark = SparkSession.builder.appName('graph-frame').getOrCreate()
    spark.sparkContext.setCheckpointDir('./checkpoints')
    spark.sparkContext.setLogLevel("ERROR")

    v = spark.createDataFrame([
      ("a", "Alice", 34),
      ("b", "Bob", 36),
      ("c", "Charlie", 30),
      ("d", "Dell", 30),
    ], ["id", "name", "age"])

    e = spark.createDataFrame([
      ("a", "b", "friend"),
      ("c", "d", "follow"),
    ], ["src", "dst", "relationship"])

    g = GraphFrame(v, e)
    connected = g.connectedComponents()
    connected.show()


if __name__ == '__main__':
    main()
