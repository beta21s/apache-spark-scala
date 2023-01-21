#!/bin/bash

if [ $1 -eq 1 ]
then
  sbt assembly

  hadoop fs -put -f /home/fit/Desktop/demo1/target/scala-2.12/demo1-assembly-0.1.0-SNAPSHOT.jar /jar/demo1-assembly-0.1.0-SNAPSHOT.jar
  aws --endpoint-url http://172.20.9.10:9000 s3 cp /home/fit/Desktop/demo1/target/scala-2.12/demo1-assembly-0.1.0-SNAPSHOT.jar s3://sources/demo1-assembly-0.1.0-SNAPSHOT.jar
fi

if [ "$2" -eq 1 ]
then
  for((i = 0; i < 1; i++))
  do
    spark-submit \
    --master spark://172.20.17.1:7077 \
    --conf spark.executor.memory=8g \
    --conf spark.dynamicAllocation.executorIdleTimeout=1000 \
    --class com.truongtpa.JoinNoFilter /home/fit/Desktop/demo1/target/scala-2.12/demo1-assembly-0.1.0-SNAPSHOT.jar
  done
fi

#spark-submit \
#--master spark://172.20.17.1:7077 \
#--class com.truongtpa.JoinBloom hdfs://master:9000/jar/demo1-assembly-0.1.0-SNAPSHOT.jar

#--class com.truongtpa.JoinBloom hdfs://master:9000/jar/demo1-assembly-0.1.0-SNAPSHOT.jar
#Data Equal: L: 5368865, R: 5366269, Equal: 5366239, Time: 9