for((i=2; i<9; i++))
do
  hdfs dfs -get hdfs://172.20.9.30:9000/join-80/file0$i
  aws --endpoint-url http://172.20.9.10:9000 s3 cp /home/fit/hdfss3/file0$i s3://join-data-80/file0$i
  rm file0$i
done