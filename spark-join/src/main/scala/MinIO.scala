package com.truongtpa

import breeze.util.BloomFilter
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object MinIO {
  def main(args: Array[String]): Unit = {

    val startTimeMillis = System.currentTimeMillis()

    lazy val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Scenario-3-minio").getOrCreate()

    val s3accessKeyAws = "z28lmtYfRoaZf2gB"
    val s3secretKeyAws = "mM7gBO7M1AaD1NnkoBsk5u1zvRvFR7S8"
    val connectionTimeOut = "600000"
    val s3endPointLoc: String = "http://172.20.9.10:9000"

    spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", s3endPointLoc)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", s3accessKeyAws)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", s3secretKeyAws)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.connection.timeout", connectionTimeOut)
    spark.sparkContext.hadoopConfiguration.set("spark.sql.debug.maxToStringFields", "100")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.path.style.access", "true")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.connection.ssl.enabled", "false")

    import spark.implicits._
    val sc = spark.sparkContext

    // Tap du lieu ben trai
//    var rddL: RDD[String] = spark.sparkContext.emptyRDD[String]
//    for (a <- 1 to 14) {
//      println("Read file" + a)
//      val s3path = "s3a://join-data/file" + a
//      val tmp: RDD[String] = sc.textFile(s3path).map(item => item.split(",")(0))
//      rddL = tmp.union(rddL)
//    }

    var rddL = spark.read.parquet("s3a://join-data/scenario-4-left.parquet")
    print(rddL.count())
//    rddL.toDF().show()
//
//    var rddR: RDD[String] = spark.sparkContext.emptyRDD[String]
//    for (a <- 11 until 15) {
//      println("Read file" + a)
//      val s3path = "s3a://join-data/file" + a
//      val tmp: RDD[String] = sc.textFile(s3path).map(item => item.split(",")(0))
//      rddR = tmp.union(rddR)
//    }
//
//    val bfl = rddL.mapPartitions { iter =>
//      val bf = BloomFilter.optimallySized[String](1000000, 0.001)
//      iter.foreach(i => bf += i)
//      Iterator(bf)
//    }.reduce(_ | _)
//
//    val rdds = rddR.filter(item => bfl.contains(item))
//
//    val endTimeMillis = System.currentTimeMillis()
//    val durationSeconds = (endTimeMillis - startTimeMillis) /1000
//
//    println(
//      "Length: " + rddL.count() + ", KQ: " + rdds.count() + ", Time: " + durationSeconds
//    )

    sc.stop()
  }
}