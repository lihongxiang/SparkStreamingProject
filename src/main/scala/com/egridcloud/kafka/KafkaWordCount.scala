package com.egridcloud.kafka

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf}

/**
  * Created by LHX on 2018/3/9 16:39.
  * 从kafka中读数据，Spark Streaming进行单词数量的计算
  */
object KafkaWordCount {
  val updateFunc = (iter: Iterator[(String, Seq[Int], Option[Int])]) => {
    iter.flatMap { case (x, y, z) => Some(y.sum + z.getOrElse(0)).map(i => (x, i)) }
  }
  def main(args: Array[String]): Unit = {
    val checkPointPath = "C:/tmp/checkPointPath/"
    //    val checkPointPath=args(0)
    val outputPath = "C:/tmp/sparkstreamingfile_save/"
    //    val outputPath=args(1)

    //参数用一个数组来接收：zookeeper集群、组、kafka的组、线程数量
    //svlhdpt02-pip.csvw.com:2181 g1 wordcount 1   要注意的是要创建line这个topic
    val Array(zkQuorum, group, topics, numThreads) = Array("svlhdpt02-pip.csvw.com:2181","group1","wordcount01","1")
    val sparkConf = new SparkConf().setAppName("KafkaWordCount")
    sparkConf.setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf,Seconds(10))
    ssc.checkpoint(checkPointPath)
    //"Array((alog-2016-04-16, 2), (alog-2016-04-17, 2), (alog-2016-04-18, 2))"
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    //保存到内存和磁盘，并且进行序列化
    val data: ReceiverInputDStream[(String, String)] =
      KafkaUtils.createStream(ssc, zkQuorum, group, topicMap, StorageLevel.MEMORY_AND_DISK_SER)
    //从kafka中写数据其实也是(key,value)形式的，这里的_._2就是value
    val words = data.map(_._2).flatMap(_.split(" "))
    val wordCounts = words.map((_, 1)).updateStateByKey(updateFunc, new HashPartitioner(ssc.sparkContext.defaultParallelism), true)
    wordCounts.foreachRDD((a,b)=> println(s"count time:${b},${a.collect().toList}"))
    wordCounts.saveAsTextFiles(outputPath)
    ssc.start()
    ssc.awaitTermination()
  }
}
//count time:1521168010000 ms,List((test_7,1), (test_5,1), (asd,1), (test_1,2), (test_3,2), (ok,1), (test_2,2), (test_6,1), (test_4,2))
//count time:1521168020000 ms,List((test_7,1), (test_5,1), (asd,1), (test_1,2), (test_3,2), (ok,1), (test_2,2), (test_6,1), (test_4,2))