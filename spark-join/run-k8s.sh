#!/bin/bash

export PATH_JAR_BUILD=/home/fit/Documents/apache-spark-scala/spark-join/target/scala-2.12/spark-join-assembly-0.1.0-SNAPSHOT.jar
export PATH_JAR_HDFS=hdfs://172.20.9.30:9000/jar/demo1-assembly-0.1.0-SNAPSHOT.jar
export TOLKEN=eyJhbGciOiJSUzI1NiIsImtpZCI6IkR0MFIwNS1KM3lVMERuRTA5a2FaWk5Vb2xzSW5jZEtEZVlMZkM5R3BSYUkifQ.eyJhdWQiOlsiaHR0cHM6Ly9rdWJlcm5ldGVzLmRlZmF1bHQuc3ZjLmNsdXN0ZXIubG9jYWwiXSwiZXhwIjoxNjc5MzU0OTQ0LCJpYXQiOjE2Nzc1OTgxNDQsImlzcyI6Imh0dHBzOi8va3ViZXJuZXRlcy5kZWZhdWx0LnN2Yy5jbHVzdGVyLmxvY2FsIiwia3ViZXJuZXRlcy5pbyI6eyJuYW1lc3BhY2UiOiJrdWJlcm5ldGVzLWRhc2hib2FyZCIsInNlcnZpY2VhY2NvdW50Ijp7Im5hbWUiOiJhZG1pbi11c2VyIiwidWlkIjoiNGZlNmI2YjUtMmNhZS00ZWFmLThmYmMtODQ4ZjRiMTczMjM5In19LCJuYmYiOjE2Nzc1OTgxNDQsInN1YiI6InN5c3RlbTpzZXJ2aWNlYWNjb3VudDprdWJlcm5ldGVzLWRhc2hib2FyZDphZG1pbi11c2VyIn0.d39nmj8rh8Gy_IRBLcv_nxQ-4zferx4VGQprSXDSIqWjK_5sbP_oOrlNAB0VoIF7xgpa_9rod8Hq8h6-l6BN9Ewt453fwmNAHgx1KZpJ-iIpktRsgvZjc-emTyPn_dj9i9JZ21Wi8vQl8L4ZyEEBBITZFfF2tYL1b2viabpszPxfouXwf0O2BcyjQcBn_TO6TpsBqmCbm2zZ6EvcdQAANk12AgCCgZlidg7hD9vTJmlZU8KQJ8KYzWSv_eJuiAVLn4Xq0yv5ZitWuG9FkJYh6N-9ho2fkcZyfShaKm6aStAR-mN4hM5XOuRIFF3_y_QCXNJDRX9hiNzMyXQLa11Rnw

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
    --conf spark.kubernetes.authenticate.submission.caCertFile=/home/certificate.pem \
    --conf spark.kubernetes.authenticate.submission.oauthToken=$TOLKEN \
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