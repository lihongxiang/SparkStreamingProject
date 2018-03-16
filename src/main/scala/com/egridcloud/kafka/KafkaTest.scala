package com.egridcloud.kafka

import java.util.Properties

import kafka.consumer._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import scala.collection.mutable.HashMap
/**
  * Created by LHX on 2018/3/9 上午 11:00.
  * 测试集群kafka发送、接收数据
  */
class KafkaTestConsumer(val topic: String) extends Thread {
  var consumer: ConsumerConnector = _
  def init: KafkaTestConsumer = {
    val pro = new Properties()
    pro.put("zookeeper.connect", "10.122.17.129:2181")
    pro.put("group.id", "group1")
    pro.put("zookeeper.session.timeout.ms", "60000")
    this.consumer = Consumer.create(new ConsumerConfig(pro))
    this
  }
  override def run(): Unit = {
    val topicConfig = new HashMap[String, Int]()
    topicConfig += topic -> 1
    val message: collection.Map[String, List[KafkaStream[Array[Byte], Array[Byte]]]] = consumer.createMessageStreams(topicConfig)
    val kafkaStream: KafkaStream[Array[Byte], Array[Byte]] = message.get(topic).get(0)
    //循环接收kafka数据 TODO 思考
    val iter: ConsumerIterator[Array[Byte], Array[Byte]] = kafkaStream.iterator()
    while (iter.hasNext()) {
      val bytes: Array[Byte] = iter.next().message()
      println(s"receives:${new String(bytes)}")
      Thread.sleep(1000)
    }
  }
}
// 伴生对象
object KafkaTestConsumer {
  def apply(topic: String): KafkaTestConsumer = new KafkaTestConsumer(topic).init
}

class KafkaTestProducer(val topic: String) extends Thread {
  var producer: KafkaProducer[String, String] = _

  def init: KafkaTestProducer = {
    val props = new Properties()
    props.put("bootstrap.servers", "10.122.17.129:9095")
    props.put("key.serializer", classOf[StringSerializer].getName)
    props.put("value.serializer", classOf[StringSerializer].getName)
    this.producer = new KafkaProducer[String, String](props)
    this
  }
  override def run(): Unit = {
    var num = 1
    while (true) {
      //要发送的消息
      val messageStr = new String(s"test_${num}")
      println(s"send:${messageStr}")
      producer.send(new ProducerRecord[String, String](this.topic, messageStr))
      num += 1
      if (num > 10) num = 0
      Thread.sleep(3000)
    }
  }
}
// 伴生对象
object KafkaTestProducer {
  def apply(topic: String): KafkaTestProducer = new KafkaTestProducer(topic).init
}

object KafkaTest {
  def main(args: Array[String]): Unit = {
    val consumer = KafkaTestConsumer("wordcount01")
    val producer = KafkaTestProducer("wordcount01")
    consumer.start()
    producer.start()
  }
}