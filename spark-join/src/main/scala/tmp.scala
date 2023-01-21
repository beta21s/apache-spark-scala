package com.truongtpa

import com.redislabs.provider.redis.toRedisContext
import org.apache.spark.sql.SparkSession
import com.redislabs.provider.redis._
import com.redislabs.provider.redis._
import org.apache.spark.rdd.RDD

object tmp {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .appName("tmp")
      .master("local[*]")
      .getOrCreate()

    import spark.sqlContext.implicits._
    val emp = Seq("1", "1", "2", "3")
    val empColumns = Seq(
      "value",
    )
    val empDF = emp.toDF(empColumns: _*)
    empDF.show(false)

    val dept = Seq("5", "6", "7", "8", "1", "1")
    val deptColumns = Seq("value")
    val deptDF = dept.toDF(deptColumns: _*)
    deptDF.show(false)

    empDF.join(deptDF, empDF("value") ===  deptDF("value"),"leftsemi").show(false)
  }
}