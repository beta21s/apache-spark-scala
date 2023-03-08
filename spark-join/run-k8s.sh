#!/bin/bash

export PATH_JAR_BUILD=/home/ubuntu/Documents/apache-spark-scala/spark-join/target/scala-2.12/spark-join-assembly-0.1.0-SNAPSHOT.jar
export PATH_JAR_HDFS=hdfs://172.20.9.30:9000/jar/spark-join-assembly-0.1.0-SNAPSHOT.jar

export TOKEN=eyJhbGciOiJSUzI1NiIsImtpZCI6Ik9JVjl2c3pGTWFZNjJaNTJna3JQRkdqUm1YNENuTmtJcjNkN1RRMVhnQmMifQ.eyJhdWQiOlsiaHR0cHM6Ly9rdWJlcm5ldGVzLmRlZmF1bHQuc3ZjLmNsdXN0ZXIubG9jYWwiXSwiZXhwIjoxNjgwMDQ3MjQ1LCJpYXQiOjE2NzgyOTA0NDUsImlzcyI6Imh0dHBzOi8va3ViZXJuZXRlcy5kZWZhdWx0LnN2Yy5jbHVzdGVyLmxvY2FsIiwia3ViZXJuZXRlcy5pbyI6eyJuYW1lc3BhY2UiOiJrdWJlcm5ldGVzLWRhc2hib2FyZCIsInNlcnZpY2VhY2NvdW50Ijp7Im5hbWUiOiJhZG1pbi11c2VyIiwidWlkIjoiNmEyNjQ5ODItOTk1Mi00OTdmLTg4NDMtYTBlYzg1Y2RjNmFjIn19LCJuYmYiOjE2NzgyOTA0NDUsInN1YiI6InN5c3RlbTpzZXJ2aWNlYWNjb3VudDprdWJlcm5ldGVzLWRhc2hib2FyZDphZG1pbi11c2VyIn0.ophxGRXjGwP8qGbVIPZkHhNe-OnjzK2A9n_3RItHqDqyt0BRz2r6Rmaqn2UXRtl_M7gNH74ky2Dy-JBDLV_FMOBjscCs5WSj0BWt749fx_8XQmzedSImyp9qt-52IUngecrZ19WVM3yGVmqbzuTp5S4C-YisZe5d1yaXmUCsP30NjLX56nb8fY7vEA4VbVuQupXqMk0qRVws3LYoWuoZwEnaQcp7yOiWJr13dNo28dkswpYX0VpyKtgWEluyJC1ZS2jBiLjYcYaWfQsOUzyU50LRnrS-4iVp7iHweMcqoqGk5d0Phb9y2NN_RkRPBWoKM14ARTqT-dm8nwQVlDA1Ng
export caCertFile=/home/ubuntu/Documents/apache-spark-scala/spark-join/kube-apiserver-chain.pem

sbt assembly
hadoop fs -put -f $PATH_JAR_BUILD $PATH_JAR_HDFS

runSpark () {
  for((i = 0; i < 1; i++))
  do
    spark-submit \
    --master k8s://https://172.20.6.10:6443  \
    --deploy-mode cluster \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
    --conf spark.kubernetes.container.image=gcr.io/spark-operator/spark:v3.1.1 \
    --conf spark.kubernetes.authenticate.submission.caCertFile=$caCertFile \
    --conf spark.kubernetes.authenticate.submission.oauthToken=$TOKEN \
    --conf spark.executor.instances=2 \
    --conf spark.executor.cores=5 \
    --conf spark.executor.memory=12g \
    --conf spark.driver.memory=4g \
    --class com.truongtpa.$1 $2
  done
}

runSpark JoinS3.Scenario1 $PATH_JAR_HDFS
#runSpark JoinS3.Scenario2 $PATH_JAR_HDFS
#runSpark JoinS3.Scenario3 $PATH_JAR_HDFS
#runSpark JoinS3.Scenario4 $PATH_JAR_HDFS