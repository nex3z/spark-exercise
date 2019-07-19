package com.nex3z.examples.spark.stream.passengercount;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;

import static org.apache.spark.sql.functions.*;

@Slf4j
public class Main {
    private static final String SERVERS = "localhost:9092";
    private static final String TOPIC = "Passenger";
    private static final String TOPIC_OUTPUT = "PassengerCount";

    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession
                .builder()
                .appName("PassengerCount")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
        spark.conf().set("spark.sql.streaming.checkpointLocation", "./checkpoint");

        Dataset<Row> dfKafka = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", SERVERS)
                .option("subscribePattern", TOPIC)
                .load()
                .selectExpr("CAST(value AS STRING)");

        Dataset<Passenger> dfPassenger = dfKafka.as(Encoders.STRING())
                .map(new PassengerMapper(), Encoders.bean(Passenger.class));

        Dataset<Row> dfPassengerWithWatermark = dfPassenger
                .selectExpr("shopName", "passengerId", "arriveTime")
                .withColumn("arriveTime", to_timestamp(col("arriveTime"), "yyyyMMddHHmmss"))
                .withWatermark("arriveTime", "10 seconds");

        Dataset<Row> dfCount = dfPassengerWithWatermark
                .groupBy(col("shopName"), window(col("arriveTime"), "5 seconds", "5 seconds"))
                .agg(count("shopName").as("count"));

        dfCount.selectExpr("CAST(shopName AS STRING) AS key", "CAST(count AS STRING) AS value")
                .writeStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", SERVERS)
                .option("topic", "PassengerCount")
                .start();

        dfCount.writeStream()
                .outputMode(OutputMode.Append())
                .format("console")
                .option("truncate", false)
                .start()
                .awaitTermination();
    }

}
