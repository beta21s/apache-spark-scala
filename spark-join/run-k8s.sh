#!/bin/bash

sbt assembly

hadoop fs -put -f /home/fit/Documents/apache-spark-scala/spark-join/target/scala-2.12/spark-join-assembly-0.1.0-SNAPSHOT.jar /jar/demo1-assembly-0.1.0-SNAPSHOT.jar

export NAME="NoFilter-HDFS-K8s-10GB-10GB"
#export NAME="NoFilter-HDFS-K8s-20GB-10GB"
#export NAME="NoFilter-HDFS-K8s-30GB-20GB"
#export NAME="NoFilter-HDFS-K8s-50GB-30GB"

export CLASS=com.truongtpa.JoinK8s.BFHDFS

export TOKEN=eyJhbGciOiJSUzI1NiIsImtpZCI6IkR0MFIwNS1KM3lVMERuRTA5a2FaWk5Vb2xzSW5jZEtEZVlMZkM5R3BSYUkifQ.eyJhdWQiOlsiaHR0cHM6Ly9rdWJlcm5ldGVzLmRlZmF1bHQuc3ZjLmNsdXN0ZXIubG9jYWwiXSwiZXhwIjoxNjc3MDc2MjgyLCJpYXQiOjE2NzUzMTk0ODIsImlzcyI6Imh0dHBzOi8va3ViZXJuZXRlcy5kZWZhdWx0LnN2Yy5jbHVzdGVyLmxvY2FsIiwia3ViZXJuZXRlcy5pbyI6eyJuYW1lc3BhY2UiOiJrdWJlcm5ldGVzLWRhc2hib2FyZCIsInNlcnZpY2VhY2NvdW50Ijp7Im5hbWUiOiJhZG1pbi11c2VyIiwidWlkIjoiNGZlNmI2YjUtMmNhZS00ZWFmLThmYmMtODQ4ZjRiMTczMjM5In19LCJuYmYiOjE2NzUzMTk0ODIsInN1YiI6InN5c3RlbTpzZXJ2aWNlYWNjb3VudDprdWJlcm5ldGVzLWRhc2hib2FyZDphZG1pbi11c2VyIn0.jl-9abzwRfqNO1-ZHWsgvGMmZ1bJFpw6oOdUyg5uDwmKc0pQCaKOlOR94SClO01xDl1-fNqVZFL3g6RIdDIe0eNfC4FEfVVk7t4fhSGyYQRPJRd0YGEifgo2o6WW0fE95W3l2OvDPih97aOUpV-5codj-tChPnngVB9e_EfdT9yvis23itUtqoT-1Tv2Tof51oCAai21JnBwVMfG7n9BiOPfrV5U7cxjUQEqfoHPHYkyFx_CCUIMRplt2AmhPYxfDTfVm26CAVzyVCs-qp-t4f-j3SAHTRp7BwBkXKHnu1Ocr3prSHolL9e0fok0DJgSQVWKYO8ryvDGzGOq9oK8_w

for((i=0; i<9; i++))
do
spark-submit  \
    --master k8s://https://172.20.8.10:6443  \
    --deploy-mode cluster \
    --name $NAME \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
    --conf spark.kubernetes.container.image=gcr.io/spark-operator/spark:v3.1.1 \
    --conf spark.kubernetes.authenticate.submission.caCertFile=/home/certificate.pem \
    --conf spark.kubernetes.authenticate.submission.oauthToken=$TOKEN \
    --class $CLASS \
    hdfs://172.20.17.1:9000/jar/demo1-assembly-0.1.0-SNAPSHOT.jar
done