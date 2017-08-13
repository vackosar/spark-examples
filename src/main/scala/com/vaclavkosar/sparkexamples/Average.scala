package com.vaclavkosar.sparkexamples

import org.apache.spark._
object Average {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Filter And Reduce").setMaster("local")
    val spark = new SparkContext(conf)
    val map = spark.parallelize(Array((1, 5), (1, 6), (2, 1), (2, 1)))
      .aggregateByKey((0, 0))((a, value) => (a._1 + value, a._2 + 1), (a, b) => (a._1 + b._1, a._2 + b._2))
      .mapValues[Double](a => a._1.asInstanceOf[Double] / a._2.asInstanceOf[Double])
      .collect()
      .foreach(println)
    spark.stop()
  }
}