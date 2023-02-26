#!/bin/bash

export PATH_JAR_BUILD=/home/fit/Documents/apache-spark-scala/spark-join/target/scala-2.12/spark-join-assembly-0.1.0-SNAPSHOT.jar
export PATH_JAR_HDFS=hdfs://172.20.9.30:9000/jar/demo1-assembly-0.1.0-SNAPSHOT.jar
export TOLKEN=eyJhbGciOiJSUzI1NiIsImtpZCI6IkR0MFIwNS1KM3lVMERuRTA5a2FaWk5Vb2xzSW5jZEtEZVlMZkM5R3BSYUkifQ.eyJhdWQiOlsiaHR0cHM6Ly9rdWJlcm5ldGVzLmRlZmF1bHQuc3ZjLmNsdXN0ZXIubG9jYWwiXSwiZXhwIjoxNjc5MTU3MTE0LCJpYXQiOjE2Nzc0MDAzMTQsImlzcyI6Imh0dHBzOi8va3ViZXJuZXRlcy5kZWZhdWx0LnN2Yy5jbHVzdGVyLmxvY2FsIiwia3ViZXJuZXRlcy5pbyI6eyJuYW1lc3BhY2UiOiJrdWJlcm5ldGVzLWRhc2hib2FyZCIsInNlcnZpY2VhY2NvdW50Ijp7Im5hbWUiOiJhZG1pbi11c2VyIiwidWlkIjoiNGZlNmI2YjUtMmNhZS00ZWFmLThmYmMtODQ4ZjRiMTczMjM5In19LCJuYmYiOjE2Nzc0MDAzMTQsInN1YiI6InN5c3RlbTpzZXJ2aWNlYWNjb3VudDprdWJlcm5ldGVzLWRhc2hib2FyZDphZG1pbi11c2VyIn0.PuGzrqVg72UpxKLF2hjt6ngRuF7AveiIJM5ER9cJmK_Sw0m1Wp98yDMvE4qFXS3VEI5_Z38k45Ya295oyW_VkM7RgDJwt5WqHIEvft7Zclq-5TO6M80yd0EnMVUbd5XH-9DqGdH-EwhgqzSbfBzQBaekhSxKzrx3Im-DH_9DYTzb9fTX9zH_pAGd6YfW4Yb1k7IPlrJ5Okjxsw5IPhU26Wbd_04mk00iNI9-6ePq-w8iz_sIwyToi04YQ5i6JPaLCQRVECsYrMzffzPk8g9SBBbzmDwhZlbhSXk_JO4T4PMKk2U7N1MMKcCzz2ngYx9URowFcJO3PgQm1bkUJJ6khA

sbt assembly
hadoop fs -put -f $PATH_JAR_BUILD $PATH_JAR_HDFS

runSpark () {
  for((i = 0; i < 10; i++))
  do
    spark-submit \
    --master k8s://https://172.20.8.10:6443  \
    --deploy-mode cluster \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
    --conf spark.kubernetes.container.image=gcr.io/spark-operator/spark:v3.1.1 \
    --conf spark.kubernetes.authenticate.submission.caCertFile=/home/certificate.pem \
    --conf spark.kubernetes.authenticate.submission.oauthToken=$TOLKEN \
    --class com.truongtpa.$1 $2
  done
}

runSpark JoinS3.Scenario1 $PATH_JAR_HDFS
runSpark JoinS3.Scenario2 $PATH_JAR_HDFS
runSpark JoinS3.Scenario3 $PATH_JAR_HDFS
runSpark JoinS3.Scenario4 $PATH_JAR_HDFS