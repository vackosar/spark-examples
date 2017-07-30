package com.vaclavkosar.sparkexamples

import org.apache.spark._
object FilterAndReduce {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Filter And Reduce")
    val spark = new SparkContext(conf)
    val count = spark.parallelize(1 until 1000, 2)
      .filter(n => n % 2 == 0)
      .reduce(_ + _)
    spark.stop()
  }
}