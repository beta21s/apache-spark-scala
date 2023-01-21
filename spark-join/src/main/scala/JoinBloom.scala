package com.truongtpa

import breeze.util.BloomFilter
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object JoinBloom {
  def main(args: Array[String]): Unit = {

    val startTimeMillis = System.currentTimeMillis()
    val spark: SparkSession = SparkSession.builder()
      .appName("Join Bloom Filter")
      .getOrCreate()

    val sc = spark.sparkContext

    val rddL1: RDD[String] = sc.textFile("hdfs://172.20.17.1:9000/selfjoin/file1").map(item => item.split(",")(0))
    val rddL3: RDD[String] = sc.textFile("hdfs://172.20.17.1:9000/selfjoin/file3").map(item => item.split(",")(0))
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
    val rddL = rddL1.union(rddL3)
//  .union(rddL5)
//      .union(rddL7).union(rddL9).union(rddL11)
//      .union(rddL13).union(rddL15)
//      .union(rddL17).union(rddL17)
//      .union(rddL19).union(rddL21).union(rddL23)

    val rddR1: RDD[String] = sc.textFile("hdfs://172.20.17.1:9000/selfjoin/file2").map(item => item.split(",")(0))
    val rddR2: RDD[String] = sc.textFile("hdfs://172.20.17.1:9000/selfjoin/file4").map(item => item.split(",")(0))
    val rddR = rddR1.union(rddR2)

    val bf = rddL.mapPartitions { iter =>
      val bf = BloomFilter.optimallySized[String](1000000, 0.001)
      iter.foreach(i => bf += i)
      Iterator(bf)
    }.reduce(_ | _)

    var equals = true
    val rdds = rddR.filter(item => bf.contains(item) == equals)

    val endTimeMillis = System.currentTimeMillis()
    val durationSeconds = (endTimeMillis - startTimeMillis) / 1000

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
