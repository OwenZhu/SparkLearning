package com.bigdata.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object SparkWordCount {
  def main(args: Array[String]): Unit = {
    val sparkConfig = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConfig)

    // Tokenization
    val lines: RDD[String] = sc.textFile("data")
    val words: RDD[String] = lines.flatMap(_.split(" "))

    // Group By Words
    val wordGroup: RDD[(String, Iterable[String])] = words.groupBy(word => word)

    // Transform to Word Count
    val wordToCount = wordGroup.map {
      case (word, list) => (word, list.size)
    }

    val array: Array[(String, Int)] = wordToCount.collect()
    array.foreach(println)

    sc.stop()
  }
}
