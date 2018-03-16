package com.egridcloud.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Durations, StreamingContext}

/**
  * Created by LHX on 2018/3/7 下午 2:10.
  * 监听socket端口，统计单词个数，
  * 使用UpdateState累加，使用checkpoint恢复历史数据
  */
object SparkStreamingSocket2 {
  def main(args: Array[String]): Unit = {
    val cpath ="C:/tmp/sparkstreamingsocket2"
    val getOrCreate: StreamingContext = StreamingContext.getOrCreate(cpath, () => {
      val conf = new SparkConf()
      conf.setAppName("sparkstreaming").setMaster("local[2]")
      val streamingContext = new StreamingContext(conf, Durations.seconds(5))
      //使用updateStateKey必须设置checkpoint
      streamingContext.checkpoint(cpath)
      val lines: ReceiverInputDStream[String] = streamingContext.socketTextStream("localhost", 6666)
      val flatMap: DStream[String] = lines.flatMap(_.split(" "))
      val mapToPair: DStream[(String, Int)] = flatMap.map((_, 1))
      val reduceByKey: DStream[(String, Int)] = mapToPair.reduceByKey(_ + _)
      val updateStateKey: DStream[(String, Int)] = reduceByKey.updateStateByKey((a: Seq[Int], b: Option[Int]) => {
        var total = 0
        for (i <- a) {
          total += i
        }
        val last: Int = if (b.isDefined) b.get else 0
        val now = total + last
        Some(now)
      })
      updateStateKey.foreachRDD((a, b) => println(s"count time:${b},${a.collect().toList}"))
      streamingContext
    })
    getOrCreate.start()
    getOrCreate.awaitTermination()
  }
}
//count time:1520406880000 ms,List((b,4), (,1), (a,3))
//count time:1520406885000 ms,List((b,4), (,1), (a,4), (c,2))
//count time:1520406900000 ms,List((b,4), (,1), (a,4), (c,2))
//count time:1520406905000 ms,List((b,4), (,1), (a,4), (c,2))
//count time:1520406910000 ms,List((b,4), (,1), (a,4), (c,2))