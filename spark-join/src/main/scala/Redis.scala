package com.truongtpa

import breeze.util.BloomFilter
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

object Redis {
  case class employee(name: String, value: Int)

  def main(args: Array[String]): Unit = {
    val startTimeMillis = System.currentTimeMillis()

    val spark: SparkSession = SparkSession.builder()
      .appName("Redis")
//      .master("local[*]")
      .config("spark.redis.host", "172.20.9.20")
      .config("spark.redis.port", "6379")
      .getOrCreate()

    import spark.implicits._
    val sc = spark.sparkContext

    val rdR = spark.read
      .format("org.apache.spark.sql.redis")
      .schema(
        StructType(Array(
          StructField("value", StringType))
        )
      )
      .option("table", "file1")
      .load()

    val endTimeMillis = System.currentTimeMillis()
    val durationSeconds = (endTimeMillis - startTimeMillis) / 1000
    println(rdR.count() + ", Time: " + durationSeconds)

 //   rdR.show()

//    val rdL = spark.read
//      .format("org.apache.spark.sql.redis")
//      .schema(
//        StructType(Array(
//          StructField("value", StringType))
//        )
//      )
//      .option("table", "left")
//      .load()
//
//    val rDDL = rdL.as[String].rdd
//    val bfl = rDDL.mapPartitions { iter =>
//      val bf = BloomFilter.optimallySized[String](1000000, 0.001)
//      iter.foreach(i => bf += i)
//      Iterator(bf)
//    }.reduce(_ | _)
//
//    val sourceRddR = rdR.as[String].rdd
//    val kq = sourceRddR.filter(item => bfl.contains(item))
//    val endTimeMillis = System.currentTimeMillis()
//    val durationSeconds = (endTimeMillis - startTimeMillis) / 1000
//    println(
//      "Left: " + rdR.count() + ",  Right: " + rdL.count() + ", KQ: " + kq.count() + ", Time: " + durationSeconds
//    )
//    sc.stop()
  }
}