package com.truongtpa

import breeze.util.BloomFilter
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object Tools {
  def readS3A(sc: SparkContext, fileName: String): RDD[String] = {
    val path = "s3a://data-join/file" + fileName
    return sc.textFile(path).map(item => item.split(",")(0))
  }

  def rdd2BF(rdd: RDD[String]): BloomFilter[String] = {
    return rdd.mapPartitions { iter =>
      val bf = BloomFilter.optimallySized[String](20000000, 0.0001)
      iter.foreach(i => bf += i)
      Iterator(bf)
    }.reduce(_ | _)
  }
}

object Scenarios1 {
  val name = "scenarios1"
  val left = "left"
  val right = "right"
}

