package com.truongtpa
package Ignite

import org.apache.ignite.spark.IgniteContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


object tmp {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("SparkByExamples.com")
      .master("local[*]")
      .getOrCreate()

    val MAVEN_HOME = "/home/fit/Downloads/apache-ignite-2.14.0-bin"
    spark.sparkContext.addJar(MAVEN_HOME + "/libs/ignite-core-2.14.0.jar")
    spark.sparkContext.addJar(MAVEN_HOME + "/libs/ignite-spring/ignite-spring-2.14.0.jar")
    spark.sparkContext.addJar(MAVEN_HOME + "/libs/ignite-spring/spring-beans-5.2.22.RELEASE.jar")
    spark.sparkContext.addJar(MAVEN_HOME + "/libs/ignite-spring/spring-core-5.2.22.RELEASE.jar")
    spark.sparkContext.addJar(MAVEN_HOME + "/libs/ignite-spring/spring-context-5.2.22.RELEASE.jar")
    spark.sparkContext.addJar(MAVEN_HOME + "/libs/ignite-spring/spring-expression-5.2.22.RELEASE.jar")
    spark.sparkContext.addJar(MAVEN_HOME + "/libs/cache-api-1.0.0.jar")
    spark.sparkContext.addJar(MAVEN_HOME + "/libs/ignite-indexing/h2-1.4.197.jar")

    val sc = spark.sparkContext
    val pathConfig = "/home/fit/Documents/apache-spark-scala/spark-join/config/example-cache.xml"
    val abc = new IgniteContext(sc, pathConfig)
  }
}