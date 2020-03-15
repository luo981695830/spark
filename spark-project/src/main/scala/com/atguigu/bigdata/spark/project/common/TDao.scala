package com.atguigu.bigdata.spark.project.common

import com.atguigu.bigdata.spark.project.util.SparkStreamingEnv
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils

trait TDao {
  //读取kafka数据
  def readKafkaData():DStream[String]={
    val brokers = "alyhadoop102:9092,alyhadoop103:9092,alyhadoop104:9092"
    val topic = "adslog20"
    val group = "bigdata"
    val deserialization = "org.apache.kafka.common.serialization.StringDeserializer"
    val kafkaParams = Map(
      ConsumerConfig.GROUP_ID_CONFIG -> group,
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> deserialization,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> deserialization
    )

    val kafkaDS: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      SparkStreamingEnv.get,
      kafkaParams,
      Set(topic)
    )

    kafkaDS.map(_._2)
  }
  //读取socket数据
  def readSocketData():DStream[String]={
    null
  }

}
