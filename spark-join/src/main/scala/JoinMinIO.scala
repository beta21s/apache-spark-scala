package com.truongtpa

import breeze.util.BloomFilter
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

object JoinMinIO {

  def readRedis(spark: SparkSession, table: String): DataFrame = {
    return spark.read
      .format("org.apache.spark.sql.redis")
      .schema(
        StructType(Array(
          StructField("value", StringType))
        )
      )
      .option("scan.count", "500000")
      .option("table", table)
      .load()
  }

  def atJoinBloom(rdd: RDD[String]): BloomFilter[String] = {
    val expect = 1000000
    val positive = 0.001
    val bfl = rdd.mapPartitions { iter =>
      val bf = BloomFilter.optimallySized[String](expect, positive)
      iter.foreach(i => bf += i)
      Iterator(bf)
    }.reduce(_ | _)
    return bfl
  }

  def main(args: Array[String]): Unit = {

    val startTimeMillis = System.currentTimeMillis()

    lazy val spark = SparkSession.builder()
//      .master("local[*]")
      .appName("Scenario-4-parquet-minio-k8s").getOrCreate()

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

    val sc = spark.sparkContext
    import spark.implicits._

    var rddL: RDD[String] = spark.sparkContext.emptyRDD[String]
    for (index <- 1 until 15) {
      println("Read file" + index)
      val path = "s3a://join-data/file" + index
      val tmp: RDD[String] = sc.textFile(path).map(item => item.split(",")(0))
      rddL = tmp.union(rddL)
    }

    var rddR: RDD[String] = spark.sparkContext.emptyRDD[String]
    for (index <- 16 until 28) {
      println("Read file" + index)
      val path = "s3a://join-data/file" + index
      val tmp: RDD[String] = sc.textFile(path).map(item => item.split(",")(0))
      rddR = tmp.union(rddR)
    }

    val bf = rddL.mapPartitions { iter =>
      val bf = BloomFilter.optimallySized[String](1000000, 0.001)
      iter.foreach(i => bf += i)
      Iterator(bf)
    }.reduce(_ | _)

    val rdds = rddR.filter(item => bf.contains(item))

    println(
      "Data Diff: L: " + rddL.count()
        + ", R: " + rddR.count()
        + ", Diff: " + rdds.count()
        + ", Time: " + (System.currentTimeMillis() - startTimeMillis) / 1000
    )

    sc.stop()
  }
}