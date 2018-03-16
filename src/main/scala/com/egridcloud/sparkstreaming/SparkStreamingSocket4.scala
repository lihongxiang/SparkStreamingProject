package com.egridcloud.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
/**
  * Created by LHX on 2018/3/7 下午 6:06.
  * * 监听socket端口，统计单词个数，
  * 使用UpdateState累加
  * 增加window滑动窗口
  */
object SparkStreamingSocket4 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("sparkstreaming").setMaster("local[2]")
    //批次间隔5s
    val streamgingContext = new StreamingContext(conf,Durations.seconds(5))
    streamgingContext.checkpoint("C:/tmp/sparkstreamingsocket4")
    val lines: ReceiverInputDStream[String] = streamgingContext.socketTextStream("localhost",6666)
    val flatMap: DStream[String] = lines.flatMap(_.split(" "))
    val mapToPair: DStream[(String, Int)] = flatMap.map((_,1))
    //窗口间隔20s
    val reducerBykey: DStream[(String, Int)] = mapToPair.reduceByKey(_ + _).window(Durations.seconds(20),Durations.seconds(10))
    val updateStateByKey: DStream[(String, Int)] = reducerBykey.updateStateByKey((a: Seq[Int], b: Option[Int]) => {
      var total = 0
      for (i <- a) {
        total += i
      }
      val last: Int = if (b.isDefined) b.get else 0
      val now = total + last
      Some(now)
    })
    updateStateByKey.foreachRDD((a,b)=> println(s"count time:${b},${a.collect().toList}"))
    //是否触发job取决于Durations时间间隔
    streamgingContext.start()
    //等待程序结束
    streamgingContext.awaitTermination()
  }
}
//存在重复计数
//count time:1520407515000 ms,List((b,1), (a,5))
//count time:1520407525000 ms,List((b,4), (a,6))
//count time:1520407535000 ms,List((b,6), (a,6))
//count time:1520407545000 ms,List((b,6), (a,6))