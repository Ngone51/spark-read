package org.apache.spark.examples.wuyi

import scala.math.random

import org.apache.spark.sql.SparkSession

/**
  */
object LearningExample {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .master("local")
      .appName("Spark Pi")
      .getOrCreate()
    val sc = spark.sparkContext
    val rdd1 = sc.parallelize(Array(("1", "Spark"), ("2", "Hadoop"), ("3", "scala"), ("4", "Java")), 2)
    val tmpRDD1 = rdd1.groupBy(e => if (e._1.toInt % 2 == 0) 1 else 2)
    val rdd2 = sc.parallelize(Array("10k", "20k", "30k"))
    val rdd3 = sc.parallelize(Array((4, "40k")))
    val tmpRDD2 = rdd2.map(x => ((x.charAt(0) - 48).toInt, x))
    val tmpRDD3 = tmpRDD2.union(rdd3)
    val finalRDD = tmpRDD1.join(tmpRDD3)
    println(finalRDD.collect().toMap)
    spark.stop()
  }
}
