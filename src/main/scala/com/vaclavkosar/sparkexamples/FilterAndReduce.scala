package com.vaclavkosar.sparkexamples

import org.apache.spark._
object FilterAndReduce {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Filter And Reduce").setMaster("local")
    val spark = new SparkContext(conf)
    val count = spark.parallelize(1 until 10)
      .filter(n => n % 2 == 0)
      .reduce(_ + _)
    println(count)
    spark.stop()
  }
}