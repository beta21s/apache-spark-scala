package com.truongtpa
package JoinCluster

import breeze.util.BloomFilter
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object NoFilterHDFS {

  def checkContains(rdd: RDD[String], text: String): Boolean = {
    val contains = rdd.filter(item => item == text)
    return contains.count() > 0
  }

  def main(args: Array[String]): Unit = {

    val startTimeMillis = System.currentTimeMillis()

    val appName = "NoFilter HDFS Cluster 30GB 20GB"
    val filename = "no-filter-hdfs-cluster-scenarios-3.parquet"

    val spark: SparkSession = SparkSession.builder()
      .appName(appName)
      .config("spark.executor.memory", "12g")
      .config("spark.driver.maxResultSize", "30g")
//      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    import spark.implicits._

    var rddL: RDD[String] = spark.sparkContext.emptyRDD[String]
    for (index <- 0 to 2) {
      println("Read file" + index)
      val path = "hdfs://172.20.9.30:9000/join-80/file0" + index
      val tmp: RDD[String] = sc.textFile(path).map(item => item.split(",")(0))
      rddL = tmp.union(rddL)
    }

    var rddR: RDD[String] = spark.sparkContext.emptyRDD[String]
    for (index <- 3 to 4) {
      println("Read file" + index)
      val path = "hdfs://172.20.9.30:9000/join-80/file0" + index
      val tmp: RDD[String] = sc.textFile(path).map(item => item.split(",")(0))
      rddR = tmp.union(rddR)
    }

    val dfL = rddL.toDF()
    val dfR = rddR.toDF()

    val rdds = dfR.join(dfL, dfL("value") === dfR("value"), "leftsemi")

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
  }
}
