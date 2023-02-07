package com.truongtpa
package BFJoin

import breeze.util.BloomFilter
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object ClusterScenario3 {
  def main(args: Array[String]): Unit = {

    val startTimeMillis = System.currentTimeMillis()
    val env = "cluster"
    val appName = "scn3-bf-" + env
    val filename = "scn3-bf-" + env + ".parquet"
    // 30GB 20GB

    val spark = SparkSession.builder()
      // .master("local[*]")
      .config("spark.executor.memory", "12g")
      .config("spark.driver.maxResultSize", "30g")
      .appName(appName)
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