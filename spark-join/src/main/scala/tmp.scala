package com.truongtpa

import breeze.util.BloomFilter
import org.apache.spark.sql.SparkSession
import com.truongtpa.JoinHDFS.checkContains
import org.apache.spark.rdd.RDD

object tmp {
  def main(args: Array[String]): Unit = {

    val startTimeMillis = System.currentTimeMillis()
    val spark: SparkSession = SparkSession.builder()
      .appName("tmp")
      .master("local[*]")
      .config("spark.driver.extraClassPath", "/home/fit/Documents/alluxio-2.9.1/client/alluxio-2.9.1-client.jar")
      .config("spark.executor.extraClassPath", "/home/fit/Documents/alluxio-2.9.1/client/alluxio-2.9.1-client.jar")
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

//    val path = "/home/fit/Documents/apache-spark-scala/spark-join/datasource/"
//    val rddr = spark.sparkContext.textFile(path + "r.txt").toDF()
//    val rddl = spark.sparkContext.textFile(path + "l.txt").toDF()
//
//    rddl.join(rddr, rddr("value") === rddl("value"), "leftsemi").show()
//    val sc = spark.sparkContext
//    val s = sc.textFile("hdfs://172.20.9.30:9000/join-80/file00")
//    s.saveAsTextFile("alluxio://172.20.9.20:19998/join/file00")

    val sc = spark.sparkContext
    import spark.implicits

    val path = "alluxio://172.20.9.20:19998/join/file00"
//    val path = "hdfs://172.20.9.30:9000/join-80/file00"
    val tmp: RDD[String] = sc.textFile(path)

    print("Total row: " + tmp.count() + ", Time: " + (System.currentTimeMillis() - startTimeMillis) / 1000 + "; ")

  }
}