package com.truongtpa
package JoinCluster

import org.apache.spark.sql.SparkSession

object delKQ {

  def main(args: Array[String]): Unit = {

    val startTimeMillis = System.currentTimeMillis()
    val appName = "showKQ"
    val spark: SparkSession = SparkSession.builder()
      .appName("")
      .master("local[*]")
      .getOrCreate()

    val filename = "s3-cluster-scn1.parquet"

    // Delete ll content
    import spark.implicits._
    Seq("")
      .toDF()
      .write.mode("overwrite")
      .parquet("hdfs://172.20.9.30:9000/result/" + filename)

    spark.read.parquet("hdfs://172.20.9.30:9000/result/" + filename).show(false)
  }
}
