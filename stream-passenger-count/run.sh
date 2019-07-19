#!/usr/bin/env bash
mvn package
$SPARK_HOME/bin/spark-submit \
    target/stream-passenger-count-1.0-SNAPSHOT-jar-with-dependencies.jar
