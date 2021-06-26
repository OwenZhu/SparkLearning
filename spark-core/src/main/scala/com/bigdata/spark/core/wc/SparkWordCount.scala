package com.bigdata.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object SparkWordCount {
  def main(args: Array[String]): Unit = {
    val sparkConfig = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConfig)

    val lines: RDD[String] = sc.textFile("data")
    val words: RDD[String] = lines.flatMap(_.split(" "))
    val wordsProcessed: RDD[(String, Int)] = words.map(
      word => (
        word.toLowerCase().replaceAll("[^A-Za-z0-9]", ""),
        1
      )
    )
    val wordGroup: RDD[(String, Int)] = wordsProcessed.reduceByKey(_+_)
    val wordGroupSorted = wordGroup.sortBy(_._2)

    wordGroupSorted.saveAsTextFile("output")
    sc.stop()
  }
}
