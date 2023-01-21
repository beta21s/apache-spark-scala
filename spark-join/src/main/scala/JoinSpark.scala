package com.truongtpa

import breeze.util.BloomFilter
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object JoinSpark {
  def main(args: Array[String]): Unit = {
    val startTimeMillis = System.currentTimeMillis()
    val spark: SparkSession = SparkSession.builder()
      .appName("Join Bloom Filter")
      .getOrCreate()

    val sc = spark.sparkContext
    import spark.implicits._

    val rddL1: RDD[String] = sc.textFile("hdfs://master:9000/selfjoin/file1").map(item => item.split(",")(0))
    val rddL3: RDD[String] = sc.textFile("hdfs://master:9000/selfjoin/file3").map(item => item.split(",")(0))
//    val rddL5: RDD[String] = sc.textFile("hdfs://master:9000/selfjoin/file5").map(item => item.split(",")(0))
//    val rddL7: RDD[String] = sc.textFile("hdfs://master:9000/selfjoin/file7").map(item => item.split(",")(0))
//    val rddL9: RDD[String] = sc.textFile("hdfs://master:9000/selfjoin/file9").map(item => item.split(",")(0))
//    val rddL11: RDD[String] = sc.textFile("hdfs://master:9000/selfjoin/file11").map(item => item.split(",")(0))
//    val rddL13: RDD[String] = sc.textFile("hdfs://master:9000/selfjoin/file13").map(item => item.split(",")(0))
//    val rddL15: RDD[String] = sc.textFile("hdfs://master:9000/selfjoin/file15").map(item => item.split(",")(0))
//    val rddL17: RDD[String] = sc.textFile("hdfs://master:9000/selfjoin/file17").map(item => item.split(",")(0))
//    val rddL19: RDD[String] = sc.textFile("hdfs://master:9000/selfjoin/file19").map(item => item.split(",")(0))
//    val rddL21: RDD[String] = sc.textFile("hdfs://master:9000/selfjoin/file21").map(item => item.split(",")(0))
//    val rddL23: RDD[String] = sc.textFile("hdfs://master:9000/selfjoin/file23").map(item => item.split(",")(0))
    val rddL = rddL1.union(rddL3).toDF()
//  .union(rddL5)
//      .union(rddL7).toDF()
//  .union(rddL9).union(rddL11).toDF()
//      .union(rddL13).union(rddL15).toDF()
//      .union(rddL17).union(rddL17)
//      .union(rddL19).union(rddL21)
//      .union(rddL23).toDF()

    val rddR1: RDD[String] = sc.textFile("hdfs://master:9000/selfjoin/file2").map(item => item.split(",")(0))
    val rddR2: RDD[String] = sc.textFile("hdfs://master:9000/selfjoin/file4").map(item => item.split(",")(0))
    val rddR = rddR1.union(rddR2).toDF()

//    val rdds = rddR.join(rddL, rddL("value") === rddR("value"), "leftanti")
    val rdds = rddR.join(rddL, rddL("value") === rddR("value"), "inner")
    val endTimeMillis = System.currentTimeMillis()
    val durationSeconds = (endTimeMillis - startTimeMillis) / 1000

    var equals = true
    if (equals == false) {
      println(
        "Data Diff: L: " + rddL.count()
          + ", R: " + rddR.count()
          + ", Diff: " + rdds.count()
          + ", Time: " + durationSeconds
      )
    } else {
      println(
        "Data Equal: L: " + rddL.count()
          + ", R: " + rddR.count()
          + ", Equal: " + rdds.count()
          + ", Time: " + durationSeconds
      )
    }

  }
}
