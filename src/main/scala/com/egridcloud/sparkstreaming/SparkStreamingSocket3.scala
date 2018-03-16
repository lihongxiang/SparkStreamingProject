package com.egridcloud.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

/**
  * Created by LHX on 2018/3/7 下午 5:35.
  * * 监听socket端口，统计单词个数，
  * 使用UpdateState累加
  * 增加isUpdate，使用checkpoint
  */
class StreamingStatuValue(var value: Int, var isUpdate: Boolean = false) extends Serializable {
  override def toString: String = s"${value},${isUpdate}"
}
object SparkStreamingSocket3 {
  def main(args: Array[String]): Unit = {
    val checkPointPath = "C:/tmp/sparkstreamingsocket3"
    def func: StreamingContext = {
      val conf = new SparkConf().setAppName("sparkstreaming").setMaster("local[2]")
      val streamingContext = new StreamingContext(conf, Durations.seconds(5))
      streamingContext.checkpoint(checkPointPath)
      val lines: ReceiverInputDStream[String] = streamingContext.socketTextStream("localhost", 6666)
      val flatMap: DStream[String] = lines.flatMap(_.split(" "))
      val mapToPair: DStream[(String, StreamingStatuValue)] = flatMap.map((_, new StreamingStatuValue(1)))
      val reducerBykey: DStream[(String, StreamingStatuValue)] = mapToPair.reduceByKey((a, b) => new StreamingStatuValue(a.value + b.value))
      val updateStateByKey: DStream[(String, StreamingStatuValue)] = reducerBykey.updateStateByKey((a: Seq[StreamingStatuValue], b: Option[StreamingStatuValue]) => {
        var total = 0
        for (i <- a) {
          total += i.value
        }
        val last: StreamingStatuValue = if (b.isDefined) b.get else new StreamingStatuValue(0)
        if (a.size != 0) {
          last.isUpdate = true
        } else {
          last.isUpdate = false
        }
        val now = total + last.value
        last.value = now
        Some(last)
      })
      updateStateByKey.foreachRDD((a, b) => {
        val filter: RDD[(String, StreamingStatuValue)] = a.filter(_._2.isUpdate)
        println(s"count time:${b},all:${a.collect().toList},now:${filter.collect().toList}")
      })
      streamingContext
    }

    val orCreate: StreamingContext = StreamingContext.getOrCreate(checkPointPath, func _)
    orCreate.start()
    orCreate.awaitTermination()
  }
}
//count time:1520406650000 ms,all:List((b,3,true), (a,6,false)),now:List((b,3,true))
//count time:1520406655000 ms,all:List((b,3,false), (a,6,false)),now:List()
//count time:1520406660000 ms,all:List((b,3,false), (a,6,false)),now:List()