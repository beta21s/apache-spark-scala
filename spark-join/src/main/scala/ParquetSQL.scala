package com.truongtpa

import org.apache.spark.sql.SparkSession

object ParquetSQL {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("SparkByExamples.com")
      .master("local[*]")
      .getOrCreate()

    val table = spark.read.format("parquet").load("hdfs://master:9000/parquet/L/*")
    table.createOrReplaceTempView("table")
    // val q1  = """SELECT * FROM table where value = '50000000000000000000'"""
    val q1  = """SELECT * FROM table limit 5"""
    val result1 = spark.sql(q1)
    result1.show
  }
}
