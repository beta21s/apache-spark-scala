#!/bin/bash

export PATH_JAR_BUILD=/home/ubuntu/Documents/apache-spark-scala/spark-join/target/scala-2.12/spark-join-assembly-0.1.0-SNAPSHOT.jar
export PATH_JAR_HDFS=hdfs://172.20.9.30:9000/jar/spark-join-assembly-0.1.0-SNAPSHOT.jar
export STUDY=Study2b

sbt assembly
hadoop fs -put -f $PATH_JAR_BUILD $PATH_JAR_HDFS

runSpark () {
  for((i = 0; i < 10; i++))
  do
    spark-submit \
    --master spark://172.20.6.10:7077 \
    --conf spark.dynamicAllocation.executorIdleTimeout=10000 \
    --conf spark.cleaner.referenceTracking.cleanCheckpoints=true \
    --class com.truongtpa.$1 $2

    bash delete-works.sh

  done
}

runSpark $STUDY.Scenario1 $PATH_JAR_HDFS
runSpark $STUDY.Scenario2 $PATH_JAR_HDFS
runSpark $STUDY.Scenario3 $PATH_JAR_HDFS
runSpark $STUDY.Scenario4 $PATH_JAR_HDFS
