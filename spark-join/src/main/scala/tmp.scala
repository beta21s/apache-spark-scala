package com.truongtpa

import breeze.util.BloomFilter
import com.redislabs.provider.redis.toRedisContext
import org.apache.spark.sql.SparkSession
import com.redislabs.provider.redis._
import com.redislabs.provider.redis._
import com.truongtpa.JoinHDFS.checkContains
import org.apache.spark.rdd.RDD

object tmp {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .appName("tmp")
      .master("local[*]")
      .getOrCreate()

    import spark.sqlContext.implicits._

//    val path = "/home/fit/Documents/apache-spark-scala/spark-join/datasource/"
//    val rddr = spark.sparkContext.textFile(path + "r.txt").map(item => item.split(",")(0))
//    val rddl = spark.sparkContext.textFile(path + "l.txt").map(item => item.split(",")(0))
//
//    val bf = rddr.mapPartitions { iter =>
//      val bf = BloomFilter.optimallySized[String](20000000, 0.001)
//      iter.foreach(i => bf += i)
//      Iterator(bf)
//    }.reduce(_ | _)
//
//    val rdds = rddl.filter(item => bf.contains(item))
//
//    println(
//      "R: " + rdds.count()
//    )

    val path = "/home/fit/Documents/apache-spark-scala/spark-join/datasource/"
    val rddr = spark.sparkContext.textFile(path + "r.txt").toDF()
    val rddl = spark.sparkContext.textFile(path + "l.txt").toDF()

    rddl.join(rddr, rddr("value") === rddl("value"), "leftsemi").show()
  }
}