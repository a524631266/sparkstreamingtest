package com.home.zhangll.spark.transform

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
//import org.apache.spark.util.ThreadUtils

object SparkStreamingTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("aaa").setMaster("local[*]")
//    val sc = SparkSession.builder().config(conf).getOrCreate()

    val ssc = new StreamingContext(conf,Seconds(1))
//    第二种方式
//    val ssc = new StreamingContext(sc.sparkContext,Seconds(1))
    //
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("192.168.40.179", 9999)

    // words 这个DStream内部有一个dependence保存着父亲lines: Dstream的对象，而且会保留父亲节点的slideDuration, 且重写了compute方法
    val words: DStream[String] = lines.flatMap(_.split(" ")) // FlatMappedDStream 继承DStream抽象

    // pairs 这个DStream内部有一个dependence保存着父亲words Dstream的对象，而且会保留父亲节点的slideDuration, 且重写了compute方法
    val pairs: DStream[(String, Int)] = words.map(word => (word, 1)) //  MappedDStream 继承DStream抽象

    // wordCounts 这个DStream内部有一个dependence保存着父亲words Dstream的对象，而且会保留父亲节点的slideDuration, 且重写了compute方法
    val wordCounts: DStream[(String, Int)] = pairs.reduceByKey(_ + _) //  ShuffledDStream 继承DStream抽象

    wordCounts.print()


    ssc.start()
    ssc.awaitTermination()
  }
}
