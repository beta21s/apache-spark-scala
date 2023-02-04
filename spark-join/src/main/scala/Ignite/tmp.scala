package com.truongtpa
package Ignite

//import org.apache.ignite.spark.{IgniteContext, IgniteRDD}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


object tmp {
  def main(args: Array[String]): Unit = {

//    val conf = new SparkConf()
//      .setMaster("local[*]")
//      .setAppName("RDDWriter")
//    val sc = new SparkContext(conf)
//    val ic = new IgniteContext(sc, "/home/fit/Documents/apache-spark-scala/spark-join/config/example-cache.xml")
//    val sharedRDD: IgniteRDD[Int, Int] = ic.fromCache("sharedRDD")
//    sharedRDD.savePairs(sc.parallelize(1 to 1000, 10).map(i => (i, i)))
//    ic.close(true)
//    sc.stop()

//
//
//
//        val spark: SparkSession = SparkSession.builder()
//          .appName("SparkByExamples.com")
//          .master("local[*]")
//          .getOrCreate()
//
//        val MAVEN_HOME = "/home/fit/Documents/apache-ignite-libs"
//        spark.sparkContext.addJar(MAVEN_HOME + "/libs/ignite-core-2.14.0.jar")
//        spark.sparkContext.addJar(MAVEN_HOME + "/libs/ignite-spring/ignite-spring-2.14.0.jar")
//        spark.sparkContext.addJar(MAVEN_HOME + "/libs/ignite-spring/spring-beans-5.2.22.RELEASE.jar")
//        spark.sparkContext.addJar(MAVEN_HOME + "/libs/ignite-spring/spring-core-5.2.22.RELEASE.jar")
//        spark.sparkContext.addJar(MAVEN_HOME + "/libs/ignite-spring/spring-context-5.2.22.RELEASE.jar")
//        spark.sparkContext.addJar(MAVEN_HOME + "/libs/ignite-spring/spring-expression-5.2.22.RELEASE.jar")
//        spark.sparkContext.addJar(MAVEN_HOME + "/libs/cache-api-1.0.0.jar")
//        spark.sparkContext.addJar(MAVEN_HOME + "/libs/ignite-indexing/h2-1.4.197.jar")
//
//        val sc = spark.sparkContext
//        val pathConfig = "/home/fit/Documents/apache-spark-scala/spark-join/config/example-cache.xml"
//        val abc = new IgniteContext(sc, pathConfig)
  }
}