package com.egridcloud.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Durations, StreamingContext}
/**
  * Created by LHX on 2018/3/7 上午 11:46.
  * 监听socket端口，统计单词个数
  */
object SparkStreamingSocket {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("sparkstreaming").setMaster("local[2]")
//    批次间隔5秒
    val streamingContext = new StreamingContext(conf, Durations.seconds(5))
    val lines: ReceiverInputDStream[String] = streamingContext.socketTextStream("localhost", 6666)
    val flatMap: DStream[String] = lines.flatMap(_.split(" "))
    val mapToPair: DStream[(String, Int)] = flatMap.map((_, 1))
    val reduceBykey: DStream[(String, Int)] = mapToPair.reduceByKey(_ + _)
    reduceBykey.foreachRDD((a, b) => println(s"count time:${b},${a.collect().toList}"))
    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
//不累加
//count time:1520407620000 ms,List((b,4), (a,1))
//count time:1520407625000 ms,List((c,1))
//count time:1520407630000 ms,List()