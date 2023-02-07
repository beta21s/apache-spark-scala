package com.truongtpa
package JoinCluster

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object showKQ {

  def main(args: Array[String]): Unit = {

    val startTimeMillis = System.currentTimeMillis()
    val appName = "showKQ"
    val spark: SparkSession = SparkSession.builder()
      .appName("")
      .master("local[*]")
      .getOrCreate()

    val filename = "scn4-no-ft-k8s.parquet"

    import spark.implicits._
    val rs = spark.read.parquet("hdfs://172.20.9.30:9000/result/" + filename)
    rs.show(false)
  }
}
