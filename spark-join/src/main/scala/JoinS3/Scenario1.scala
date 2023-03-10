package com.truongtpa
package JoinS3

import breeze.util.BloomFilter
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object Scenario1 {
  def main(args: Array[String]): Unit = {

    /*
    dataset 01: 5GB
    dataset 02: 10GB
   */

    val appName = "k8a-scenario1-2nd"

    val spark = SparkSession.builder()
      .master("local[*]")
      .config("spark.executor.memory", "12g")
      .config("spark.driver.maxResultSize", "30g")
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

    // Read file from S3 with capacity is 5GB
    var rddL: RDD[String] = spark.sparkContext.emptyRDD[String]
    for (index <- 1 to 1) {
      val fileName = f"$index%02d"
      rddL = Tools.readS3A(sc, fileName).union(rddL)
    }

    // Create filter with BF
    val BF = Tools.rdd2BF(rddL)

    // Read file from S3 with capacity is 10GB
    var coutRS : Long = 0
    for (index <- 2 to 3) {
      val fileName = f"$index%02d"
      coutRS = coutRS + Tools.readS3A(sc, fileName).filter(item => BF.contains(item)).count()
    }

    print("Result: " + coutRS)
    sc.stop()
  }
}