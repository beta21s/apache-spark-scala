package com.truongtpa
package BFJoin

import breeze.util.BloomFilter
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object ClusterScenario1 {
  def main(args: Array[String]): Unit = {

    val startTimeMillis = System.currentTimeMillis()
    val env = "cluster"
    val appName = "scn1-bf-" + env
    val filename = "scn1-bf-" + env + ".parquet"
    // 10GB 10GB

    val spark = SparkSession.builder()
      .master("local[*]")
      .config("spark.executor.memory", "12g")
      .config("spark.driver.maxResultSize", "30g")
      .appName(appName)
      .getOrCreate()

    val sc = spark.sparkContext
    import spark.implicits._

    var rddL: RDD[String] = spark.sparkContext.emptyRDD[String]
    for (index <- 0 to 0) {
      println("Read file" + index)
      val path = "hdfs://172.20.9.30:9000/join-80/file0" + index
      val tmp: RDD[String] = sc.textFile(path)
      rddL = tmp.union(rddL)
    }

    rddL.toDF().show(false)

    sc.stop()
  }
}