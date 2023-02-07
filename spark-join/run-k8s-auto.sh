#!/bin/bash

sbt assembly
hadoop fs -put -f /home/fit/Documents/apache-spark-scala/spark-join/target/scala-2.12/spark-join-assembly-0.1.0-SNAPSHOT.jar /jar/demo1-assembly-0.1.0-SNAPSHOT.jar
export TOKEN=eyJhbGciOiJSUzI1NiIsImtpZCI6IkR0MFIwNS1KM3lVMERuRTA5a2FaWk5Vb2xzSW5jZEtEZVlMZkM5R3BSYUkifQ.eyJhdWQiOlsiaHR0cHM6Ly9rdWJlcm5ldGVzLmRlZmF1bHQuc3ZjLmNsdXN0ZXIubG9jYWwiXSwiZXhwIjoxNjc3NDM0MzgyLCJpYXQiOjE2NzU2Nzc1ODIsImlzcyI6Imh0dHBzOi8va3ViZXJuZXRlcy5kZWZhdWx0LnN2Yy5jbHVzdGVyLmxvY2FsIiwia3ViZXJuZXRlcy5pbyI6eyJuYW1lc3BhY2UiOiJrdWJlcm5ldGVzLWRhc2hib2FyZCIsInNlcnZpY2VhY2NvdW50Ijp7Im5hbWUiOiJhZG1pbi11c2VyIiwidWlkIjoiNGZlNmI2YjUtMmNhZS00ZWFmLThmYmMtODQ4ZjRiMTczMjM5In19LCJuYmYiOjE2NzU2Nzc1ODIsInN1YiI6InN5c3RlbTpzZXJ2aWNlYWNjb3VudDprdWJlcm5ldGVzLWRhc2hib2FyZDphZG1pbi11c2VyIn0.hIh822VDz-7_fRJL87avaAyI_IZUOVgbakxXfGsGiJqX0IM9__MGGMEaBl5PxfPtSp3Ub2x4aiDRvnR9FsRZyfPBbZ6vNA8NGubjitRPeuqoLy4ihHb9sYal5jbJ89zcrTEjsOW3UqF3RFrGp4csV0hg-ryenvULQc228STDASxflxPezhe0XnH2aQ1LUfbzS5DUJ-BGRQOTWJM_v5XInSH5CrgOUcfCrlWIvMj4DvroyXMfLEv_0RIcjlXY4II8cwkPvVMsiKYcW35A4LsNUqFveTI-O05hzghqqOJJalxdySXBBzUiSh0fF0qSvFknhsSgo34Za6FsZ4VlvG4gvg

export PACKEAGE=com.truongtpa.NoFTJoin
for((i=0; i<10; i++))
do
spark-submit  \
    --master k8s://https://172.20.8.10:6443  \
    --deploy-mode cluster \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
    --conf spark.kubernetes.container.image=gcr.io/spark-operator/spark:v3.1.1 \
    --conf spark.kubernetes.authenticate.submission.caCertFile=/home/certificate.pem \
    --conf spark.kubernetes.authenticate.submission.oauthToken=$TOKEN \
    --class $PACKEAGE.ClusterScenario1 \
    hdfs://172.20.17.1:9000/jar/demo1-assembly-0.1.0-SNAPSHOT.jar
done

for((i=0; i<10; i++))
do
spark-submit  \
    --master k8s://https://172.20.8.10:6443  \
    --deploy-mode cluster \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
    --conf spark.kubernetes.container.image=gcr.io/spark-operator/spark:v3.1.1 \
    --conf spark.kubernetes.authenticate.submission.caCertFile=/home/certificate.pem \
    --conf spark.kubernetes.authenticate.submission.oauthToken=$TOKEN \
    --class $PACKEAGE.ClusterScenario2 \
    hdfs://172.20.17.1:9000/jar/demo1-assembly-0.1.0-SNAPSHOT.jar
done

for((i=0; i<10; i++))
do
spark-submit  \
    --master k8s://https://172.20.8.10:6443  \
    --deploy-mode cluster \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
    --conf spark.kubernetes.container.image=gcr.io/spark-operator/spark:v3.1.1 \
    --conf spark.kubernetes.authenticate.submission.caCertFile=/home/certificate.pem \
    --conf spark.kubernetes.authenticate.submission.oauthToken=$TOKEN \
    --class $PACKEAGE.ClusterScenario3 \
    hdfs://172.20.17.1:9000/jar/demo1-assembly-0.1.0-SNAPSHOT.jar
done

export CLASS=com.truongtpa.JoinK8s.ClusterScenario4
for((i=0; i<10; i++))
do
spark-submit  \
    --master k8s://https://172.20.8.10:6443  \
    --deploy-mode cluster \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
    --conf spark.kubernetes.container.image=gcr.io/spark-operator/spark:v3.1.1 \
    --conf spark.kubernetes.authenticate.submission.caCertFile=/home/certificate.pem \
    --conf spark.kubernetes.authenticate.submission.oauthToken=$TOKEN \
    --class $PACKEAGE.ClusterScenario4 \
    hdfs://172.20.17.1:9000/jar/demo1-assembly-0.1.0-SNAPSHOT.jar
done