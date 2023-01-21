package com.truongtpa

import breeze.util.BloomFilter
import com.truongtpa.JoinRedis.readRedis
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{rand, randn}

object save2Redis {

  def main(args: Array[String]): Unit = {

    val startTimeMillis = System.currentTimeMillis()
    val spark: SparkSession = SparkSession.builder()
      .appName("Upload to Redis cluster")
//      .master("local[*]")
      .config("spark.redis.host", "172.20.9.20")
      .config("spark.redis.port", "6379")
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

    for (index <- 1 to 28) {

      val s3path = "s3a://join-data/file" + index
      val tmp: RDD[String] = sc.textFile(s3path).map(item => item.split(",")(0))

      tmp.toDF().write
        .format("org.apache.spark.sql.redis")
        .option("table", "file" + index)
        .mode(SaveMode.Append)
        .save()

    }
  }
}

//df_left.write.format("org.apache.spark.sql.redis")
//.option("table", filename)
//.save()

//    val ds = spark.range(2000000)
//    val randDS = ds
//      .withColumn("value", rand(100))
//      .drop("id").as[Double]
//
//    randDS.show
//
//    randDS.toDF()
//      .write.format("org.apache.spark.sql.redis")
//      .option("table", "f")
//      .save()


//
//    val personSeq = Seq(Person("John", 30), Person("Peter", 45))
//    val df = spark.createDataFrame(personSeq)
//
//    df.write
//      .format("org.apache.spark.sql.redis")
//      .option("table", "bc")
//      .mode(SaveMode.Append)
//      .save()
//
//    // Tap du lieu ben trai
//    var rddL: RDD[String] = spark.sparkContext.emptyRDD[String]
