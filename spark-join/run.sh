#!/bin/bash

export PATH_JAR_BUILD=/home/fit/Documents/apache-spark-scala/spark-join/target/scala-2.12/spark-join-assembly-0.1.0-SNAPSHOT.jar
export PATH_JAR_HDFS=hdfs://172.20.9.30:9000/jar/demo1-assembly-0.1.0-SNAPSHOT.jar

sbt assembly
hadoop fs -put -f $PATH_JAR_BUILD $PATH_JAR_HDFS

runSpark () {
  for((i = 0; i < 10; i++))
  do
    spark-submit \
    --master spark://172.20.17.1:7077 \
    --conf spark.dynamicAllocation.executorIdleTimeout=10000 \
    --class com.truongtpa.$1 $2
  done
}

runSpark JoinS3.Scenario1 $PATH_JAR_BUILD
runSpark JoinS3.Scenario2 $PATH_JAR_BUILD
runSpark JoinS3.Scenario3 $PATH_JAR_BUILD
runSpark JoinS3.Scenario4 $PATH_JAR_BUILD