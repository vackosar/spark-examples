package com.vaclavkosar.sparkexamples

import org.apache.spark._
object Join {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName(getClass.getSimpleName).setMaster("local")
    val spark = new SparkContext(conf)
    val range1 = spark.parallelize(List((1, 1), (2, 2), (3, 3)))
    val range2 = spark.parallelize(List((1, 1), (2, 2), (3, 3)))

    range1.join(range2).collect().foreach(println(_))
    spark.stop()
  }
}