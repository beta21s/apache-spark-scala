#!/bin/bash

if [ $1 -eq 1 ]
then
  sbt assembly

  hadoop fs -put -f /home/fit/Documents/apache-spark-scala/spark-join/target/scala-2.12/spark-join-assembly-0.1.0-SNAPSHOT.jar /jar/demo1-assembly-0.1.0-SNAPSHOT.jar
fi

# Kich bn 01
if [ "$2" -eq 1 ]
then
  for((i = 0; i < 10; i++))
  do
    spark-submit \
    --master spark://172.20.17.1:7077 \
    --conf spark.executor.memory=8g \
    --conf spark.dynamicAllocation.executorIdleTimeout=10000 \
    --class com.truongtpa.JoinK8s.ClusterScenario1 /home/fit/Documents/apache-spark-scala/spark-join/target/scala-2.12/spark-join-assembly-0.1.0-SNAPSHOT.jar
  done
fi

# Kich bn 02
if [ "$2" -eq 1 ]
then
  for((i = 0; i < 10; i++))
  do
    spark-submit \
    --master spark://172.20.17.1:7077 \
    --conf spark.executor.memory=8g \
    --conf spark.dynamicAllocation.executorIdleTimeout=10000 \
    --class com.truongtpa.JoinK8s.ClusterScenario2 /home/fit/Documents/apache-spark-scala/spark-join/target/scala-2.12/spark-join-assembly-0.1.0-SNAPSHOT.jar
  done
fi

## Kich bn 03
if [ "$2" -eq 1 ]
then
  for((i = 0; i < 10; i++))
  do
    spark-submit \
    --master spark://172.20.17.1:7077 \
    --conf spark.executor.memory=12g \
    --conf spark.dynamicAllocation.executorIdleTimeout=10000 \
    --class com.truongtpa.JoinK8s.ClusterScenario3 /home/fit/Documents/apache-spark-scala/spark-join/target/scala-2.12/spark-join-assembly-0.1.0-SNAPSHOT.jar
  done
fi

# Kich bn 04
if [ "$2" -eq 1 ]
then
  for((i = 0; i < 10; i++))
  do
    spark-submit \
    --master spark://172.20.17.1:7077 \
    --conf spark.executor.memory=12g \
    --conf spark.dynamicAllocation.executorIdleTimeout=10000 \
    --class com.truongtpa.JoinK8s.ClusterScenario4 /home/fit/Documents/apache-spark-scala/spark-join/target/scala-2.12/spark-join-assembly-0.1.0-SNAPSHOT.jar
  done
fi