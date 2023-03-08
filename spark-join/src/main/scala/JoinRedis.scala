package com.truongtpa

import breeze.util.BloomFilter
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types._

object JoinRedis {

  def readRedis(spark: SparkSession, table: String): DataFrame = {
    return spark.read
      .format("org.apache.spark.sql.redis")
      .schema(
        StructType(Array(
          StructField("value", StringType))
        )
      )
      .option("scan.count", "50000")
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

    // Tinh thoi gian
    val startTimeMillis = System.currentTimeMillis()

    val spark: SparkSession = SparkSession.builder()
      .appName("Join in Redis")
//      .master("local[*]")
      .config("spark.redis.host", "172.20.9.20")
      .config("spark.redis.port", "6379")
      .getOrCreate()

    val sc = spark.sparkContext
    import spark.implicits._

    val rddL = readRedis(spark, "file1")
    for (index <- 2 to 3) {
      rddL.union(readRedis(spark, "file" + index))
    }

    val rddR = readRedis(spark, "file5")

    // KQ
    val blrddL = atJoinBloom(rddL.as[String].rdd)
    val sourceRddR = rddR.as[String].rdd
    val kq = sourceRddR.filter(item => blrddL.contains(item))

    val endTimeMillis = System.currentTimeMillis()
    val durationSeconds = (endTimeMillis - startTimeMillis) / 1000

    println(
      "KQ: " + kq.count() +
        "Time: " + durationSeconds
    )

    sc.stop()
  }
}