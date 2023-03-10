package com.truongtpa
package JoinK8s

import breeze.util.BloomFilter
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object BFHDFS {

  def checkContains(rdd: RDD[String], text: String): Boolean = {
    val contains = rdd.filter(item => item == text)
    return contains.count() > 0
  }

  def main(args: Array[String]): Unit = {

    val startTimeMillis = System.currentTimeMillis()

    val appName = "BF HDFS K8s 10GB 10GB"
//    val appName = "BF HDFS K8s 20GB 10GB"
//    val appName = "BF HDFS K8s 30GB 20GB"
//    val appName = "BF HDFS K8s 50GB 30GB"

    val filename = "hdfs-k8s-scenarios-1.parquet"
//    val filename = "hdfs-k8s-scenarios-2.parquet"
//    val filename = "hdfs-k8s-scenarios-3.parquet"
//    val filename = "hdfs-k8s-scenarios-4.parquet"

    val spark: SparkSession = SparkSession.builder()
      .appName(appName)
      .config("spark.executor.memory", "12g")
      .config("spark.driver.maxResultSize", "30g")
//      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    import spark.implicits._

    var rddL: RDD[String] = spark.sparkContext.emptyRDD[String]
     for (index <- 0 to 0) {
   //  for (index <- 0 to 1) {
//    for (index <- 0 to 2) {
    // for (index <- 0 to 4) {
      println("Read file" + index)
      val path = "hdfs://172.20.9.30:9000/join-80/file0" + index
      val tmp: RDD[String] = sc.textFile(path).map(item => item.split(",")(0))
      rddL = tmp.union(rddL)
    }

    var rddR: RDD[String] = spark.sparkContext.emptyRDD[String]
     for (index <- 1 to 1) {
    // for (index <- 2 to 2) {
//    for (index <- 3 to 4) {
    //for (index <- 5 to 8) {
        println("Read file" + index)
        val path = "hdfs://172.20.9.30:9000/join-80/file0" + index
        val tmp: RDD[String] = sc.textFile(path).map(item => item.split(",")(0))
        rddR = tmp.union(rddR)
    }

    val bf = rddL.mapPartitions { iter =>
      val bf = BloomFilter.optimallySized[String](20000000, 0.0001)
      iter.foreach(i => bf += i)
      Iterator(bf)
    }.reduce(_ | _)

    val rdds = rddR.filter(item => bf.contains(item))

    var rs = appName + ": "
    rs += "L: " + rddL.count()
    rs += ", NumPartitions: " + rddL.getNumPartitions
    rs += ", Partitions.length: " + rddL.partitions.length
    rs += ", R: " + rddR.count()
    rs += ", NumPartitions: " + rddR.getNumPartitions
    rs += ", Partitions.length: " + rddR.partitions.length
    rs += ", Equal: " + rdds.count()
    rs += ", Time: " + (System.currentTimeMillis() - startTimeMillis) / 1000

    // Ghi KQ
    Seq(rs)
      .toDF()
      .write.mode("append")
      .parquet("hdfs://172.20.9.30:9000/result/" + filename)

    print(rs)
    sc.stop()
  }
}
