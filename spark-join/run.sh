#!/bin/bash

export PATH_JAR_BUILD=/home/ubuntu/Documents/apache-spark-scala/spark-join/target/scala-2.12/spark-join-assembly-0.1.0-SNAPSHOT.jar
export PATH_JAR_S3=s3://source/

sbt assembly
aws --endpoint-url http://172.20.9.10:9000 s3 cp $PATH_JAR_BUILD $PATH_JAR_S3

runSpark () {
  for((i = 0; i < 1; i++))
  do
    spark-submit \
    --master spark://172.20.6.10:7077 \
    --conf spark.dynamicAllocation.executorIdleTimeout=10000 \
    --conf spark.hadoop.fs.s3a.endpoint=http://172.20.9.10:9000 \
    --conf spark.hadoop.fs.s3a.access.key=z28lmtYfRoaZf2gB \
    --conf spark.hadoop.fs.s3a.secret.key=mM7gBO7M1AaD1NnkoBsk5u1zvRvFR7S8 \
    --conf spark.hadoop.fs.s3a.path.style.access=true \
    --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
    --class com.truongtpa.$1 $2
  done
}

runSpark JoinS3.Scenario1 s3a://source/spark-join-assembly-0.1.0-SNAPSHOT.jar
#runSpark JoinS3.Scenario2 $PATH_JAR_BUILD
#runSpark JoinS3.Scenario3 $PATH_JAR_BUILD
#runSpark JoinS3.Scenario4 $PATH_JAR_BUILD