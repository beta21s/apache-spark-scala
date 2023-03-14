#!/bin/bash

export PATH_JAR_BUILD=/home/ubuntu/Documents/apache-spark-scala/spark-join/target/scala-2.12/spark-join-assembly-0.1.0-SNAPSHOT.jar
export PATH_JAR_HDFS=hdfs://172.20.9.30:9000/jar/spark-join-assembly-0.1.0-SNAPSHOT.jar

export TOKEN=`cat tolken.txt`
export caCertFile=/home/ubuntu/Documents/apache-spark-scala/spark-join/selfsigned_certificate.pem
export STUDY=Study2b
#export IMAGE_NAME=gcr.io/spark-operator/spark:v3.0.0
export IMAGE_NAME=truong96/spark-k8s:v3.0.1

sbt assembly
hadoop fs -put -f $PATH_JAR_BUILD $PATH_JAR_HDFS

runSpark () {
  for((i = 0; i < 10; i++))
  do
    spark-submit \
    --master k8s://https://172.20.6.10:6443  \
    --deploy-mode cluster \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
    --conf spark.kubernetes.container.image=$IMAGE_NAME \
    --conf spark.kubernetes.authenticate.submission.caCertFile=$caCertFile \
    --conf spark.kubernetes.authenticate.submission.oauthToken=$TOKEN \
    --class com.truongtpa.$1 $2
  done
}

runSpark $STUDY.Scenario1 $PATH_JAR_HDFS
runSpark $STUDY.Scenario2 $PATH_JAR_HDFS
runSpark $STUDY.Scenario3 $PATH_JAR_HDFS
runSpark $STUDY.Scenario4 $PATH_JAR_HDFS
