package com.truongtpa
package JoinS3

import breeze.util.BloomFilter
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

object Scenario4 {
  def main(args: Array[String]): Unit = {

    /*
    dataset 01: 50GB
    dataset 02: 30GB
   */

    val appName = "k8-scenario4-2nd"
    val filename = appName + ".parquet"

    val spark = SparkSession.builder()
      // .master("local[*]")
      .config("spark.executor.memory", "12g")
      .config("spark.driver.maxResultSize", "90g")
      .appName(appName)
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

    // Read file from S3 with capacity is 50GB
    var rddL: RDD[String] = spark.sparkContext.emptyRDD[String]
    for (index <- 0 to 9) {
      println("Read file" + f"$index%02d")
      val path = "s3a://join-80/file" + f"$index%02d"
      val tmp: RDD[String] = sc.textFile(path).map(item => item.split(",")(0))
      rddL = tmp.union(rddL)
    }

    // Create filter with BF
    val bf = rddL.mapPartitions { iter =>
      val bf = BloomFilter.optimallySized[String](20000000, 0.0001)
      iter.foreach(i => bf += i)
      Iterator(bf)
    }.reduce(_ | _)

    // Read file from S3 with capacity is 30GB
    var coutRS : Long = 0
    for (index <- 10 to 16) {
      println("Read file" + f"$index%02d")
      val path = "s3a://join-80/file" + f"$index%02d"
      val tmp: RDD[String] = sc.textFile(path)
        .map(item => item.split(",")(0))
        .filter(item => bf.contains(item))
      coutRS = coutRS + tmp.count()
    }

    var rs = appName + ": "
    rs += "Result: " + coutRS

    // Ghi KQ
    Seq(rs)
      .toDF()
      .write.mode("append")
      .parquet("hdfs://172.20.9.30:9000/result/" + filename)

    print(rs)
    sc.stop()
  }
}