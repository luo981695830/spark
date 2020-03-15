package sparkStreaming


import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Streaming_kafka_High1 {
  def main(args: Array[String]): Unit = {
    //创建SparkConf
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("kafka")
    //创建SparkStreaming入口对象 StreamingContext Seconds表示时间间隔
    val ssc: StreamingContext = new StreamingContext(conf,Seconds(3))

    //kafka参数
    val brokers = "alyhadoop102:9092,alyhadoop103:9092,alyhadoop104:9092"
    val topic = "first"
    val group = "bigdata"
    val deserialization = "org.apache.kafka.common.serialization.StringDeserializer"
    val kafkaParams = Map(
      ConsumerConfig.GROUP_ID_CONFIG -> group,
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> deserialization,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> deserialization
    )
    //使用KafkaUtils创建输入流对象
    val kafkaDS: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, Set(topic)
    )
    //创建DStream流式数据
    val kafkaMapDS: DStream[String] = kafkaDS.map(_._2)
    //打印数据
    kafkaMapDS.print()
    //启动采集器
    ssc.start()
    //让Driver等待采集器结束
    ssc.awaitTermination()
  }
}
