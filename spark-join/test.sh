#!/bin/bash

export PATH_JAR_BUILD=/home/fit/Documents/apache-spark-scala/spark-join/target/scala-2.12/spark-join-assembly-0.1.0-SNAPSHOT.jar
export PATH_JAR_HDFS=/jar/demo1-assembly-0.1.0-SNAPSHOT.jar

sbt assembly
hadoop fs -put -f $PATH_JAR_BUILD $PATH_JAR_HDFS

for((i=0; i<5; i++))
do
  spark-submit \
  --master spark://172.20.17.1:7077 \
  --conf spark.executor.memory=8g \
  --conf spark.dynamicAllocation.executorIdleTimeout=10000 \
  --class com.truongtpa.Alluxio.ClusterScenario1 $PATH_JAR_BUILD
done