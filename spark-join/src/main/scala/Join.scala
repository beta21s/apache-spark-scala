package com.truongtpa

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Join {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("SparkByExamples.com")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    val rddL1: RDD[String] = sc.textFile("hdfs://master:9000/selfjoin/file1").map(item => item.split(",")(0))
    val rddL3: RDD[String] = sc.textFile("hdfs://master:9000/selfjoin/file3").map(item => item.split(",")(0))
    val rddL5: RDD[String] = sc.textFile("hdfs://master:9000/selfjoin/file5").map(item => item.split(",")(0))
    val rddL7: RDD[String] = sc.textFile("hdfs://master:9000/selfjoin/file7").map(item => item.split(",")(0))
    val rddL = rddL1.union(rddL3).union(rddL5).union(rddL7)

    val rddR1: RDD[String] = sc.textFile("hdfs://master:9000/selfjoin/file2").map(item => item.split(",")(0))
    val rddR2: RDD[String] = sc.textFile("hdfs://master:9000/selfjoin/file4").map(item => item.split(",")(0))
    val rddR = rddR1.union(rddR2)

    import spark.implicits._
    rddL.toDF().write.mode("overwrite")
      .option("parquet.bloom.filter.enabled#value", "true")
      .option("parquet.bloom.filter.expected.ndv#value", "1000000")
      .parquet(s"hdfs://master:9000/parquet/L")

    rddR.toDF().write.mode("overwrite")
      .option("parquet.bloom.filter.enabled#value", "true")
      .option("parquet.bloom.filter.expected.ndv#value", "1000000")
      .parquet(s"hdfs://master:9000/parquet/R")

    val startTimeMillis = System.currentTimeMillis()
    val tableL = spark.read.format("parquet").load("hdfs://master:9000/parquet/L/*")
    tableL.createOrReplaceTempView("table")

    val tableR = spark.read.format("parquet").load("hdfs://master:9000/parquet/R/*")
    tableR.createOrReplaceTempView("table")

//    val datadifL = tableL.join(tableR, tableL("value") === tableR("value"), "leftanti")
    val datadifR = tableR.join(tableL, tableR("value") === tableL("value"), "leftanti")
//    val data = tableL.join(tableR, tableL("value") === tableR("value"), "inner")

    println("Data Diff: L: " + rddL.count()
      + ", R: " + rddR.count()
//      + ", DifL: " + datadifL.count()
      + ", DifR: " + datadifR.count()
//      + ", Equal: " + data.count()
    )
  }
}