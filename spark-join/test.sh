#!/bin/bash

## http://localhost:18080/api/v1/applications

sbt assembly
spark-submit \
--master spark://172.20.17.1:7077 \
--conf spark.executor.memory=8g \
--conf spark.dynamicAllocation.executorIdleTimeout=10000 \
--class com.truongtpa.tmp /home/fit/Documents/apache-spark-scala/spark-join/target/scala-2.12/spark-join-assembly-0.1.0-SNAPSHOT.jar