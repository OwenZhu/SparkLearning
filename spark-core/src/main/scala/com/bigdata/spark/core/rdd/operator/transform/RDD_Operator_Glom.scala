package com.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_Operator_Glom {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    // List => Int
    // Int => Array
    val glomRDD: RDD[Array[Int]] = rdd.glom()

    glomRDD.collect().foreach(data => println(data.mkString(",")))

    sc.stop()

  }
}
