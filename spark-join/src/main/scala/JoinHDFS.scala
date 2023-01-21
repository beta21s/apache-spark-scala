package com.truongtpa

import breeze.util.BloomFilter
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.util.sketch

object JoinHDFS {

  def checkContains(rdd: RDD[String], text: String): Boolean = {
    val contains = rdd.filter(item => item == text)
    return contains.count() > 0
  }

  def main(args: Array[String]): Unit = {

    val startTimeMillis = System.currentTimeMillis()
    val spark: SparkSession = SparkSession.builder()
      .appName("Join Bloom Filter HDFS")
      .config("spark.executor.memory", "8g")
      .config("spark.driver.maxResultSize", "8g")
      .master("local[*]")
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

    import org.apache.spark.sql.Dataset
    import spark.implicits._

//    val contains = rddL.filter(item => item == "entryNum03753223516").count()

//    val ds = rddL.toDS()
//    val filter1 = ds.stat.bloomFilter("value", 20000000, 0.03)
//    print(filter1.mightContainString("ABC"))

//    val bf = rddL.mapPartitions { iter =>
//      val bf = BloomFilter.optimallySized[String](20000000, 0.001)
//      iter.foreach(i => bf += i)
//      Iterator(bf)
//    }.reduce(_ | _)
//
    val rdds = rddR.filter(item => checkContains(rddL, item))

    println(
      "Data Diff: L: " + rddL.count()
        + ", R: " + rddR.count()
        + ", Diff: " + rdds.count()
        + ", Time: " + (System.currentTimeMillis() - startTimeMillis) / 1000
    )
  }
}
