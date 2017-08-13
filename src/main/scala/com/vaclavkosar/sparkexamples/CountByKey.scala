package com.vaclavkosar.sparkexamples

import org.apache.spark._

object CountByKey {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Filter And Reduce").setMaster("local")
    val spark = new SparkContext(conf)
    val count = spark.parallelize(Seq(1, 1, 2, 3, 4))
      .map(i => (i, i))
      .countByKey()
    println(count)
    spark.stop()
  }
}