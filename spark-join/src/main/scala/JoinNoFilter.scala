package com.truongtpa

import breeze.util.BloomFilter
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object JoinNoFilter {

  def checkExis(ds: RDD[String], text: String): Boolean = {
    val tong = ds.filter(item => text == item).count()
    if (tong > 0)
      return true
    else
      return false
  }

  def main(args: Array[String]): Unit = {

    val startTimeMillis = System.currentTimeMillis()

    val spark: SparkSession = SparkSession.builder()
      .appName("Join Bloom Filter HDFS")
//      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    var rddL: RDD[String] = spark.sparkContext.emptyRDD[String]
    for (index <- 1 to 1) {
      println("Read file" + index)
      val path = "hdfs://172.20.9.30:9000/join-80/file0" + index
      val tmp: RDD[String] = sc.textFile(path).map(item => item.split(",")(0))
      rddL = tmp.union(rddL)
    }

    var rddR: RDD[String] = spark.sparkContext.emptyRDD[String]
    for (index <- 2 to 2) {
      println("Read file" + index)
      val path = "hdfs://172.20.9.30:9000/join-80/file0" + index
      val tmp: RDD[String] = sc.textFile(path).map(item => item.split(",")(0))
      rddR = tmp.union(rddR)
    }

    import spark.implicits._
    val dfL = rddL.toDF()
    val dfR = rddR.toDF()

    val rddKQ = dfR.join(dfL, dfL("value") === dfR("value"), "leftsemi")
    rddKQ.show()

    println(
        ", Diff: " + rddKQ.count()
        + ", Time: " + (System.currentTimeMillis() - startTimeMillis) / 1000
    )
  }
}
