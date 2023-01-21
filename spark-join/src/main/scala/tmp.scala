package com.truongtpa

import com.redislabs.provider.redis.toRedisContext
import org.apache.spark.sql.SparkSession
import com.redislabs.provider.redis._
import com.redislabs.provider.redis._
import org.apache.spark.rdd.RDD

object tmp {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .appName("Redis")
      .master("local[*]")
      .getOrCreate()

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

    val s3path = "hdfs://172.20.9.30:9000/join-80/file00"
    val tmp: RDD[String] = sc.textFile(s3path)
    tmp.toDF().show(false)
  }
}