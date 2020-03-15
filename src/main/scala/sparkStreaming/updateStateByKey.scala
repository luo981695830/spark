package sparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object updateStateByKey {
  def main(args: Array[String]): Unit = {
    //创建SparkConf 设置最大线程数 app名称
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("updataStateByKey")
    //创建SparkStreaming 上下文对象
    val ssc: StreamingContext = new StreamingContext(conf,Seconds(3))
    //创建sparkContext对象 并设置检查点存储路径
    //TODO 有状态的数据的保存时放置在检查点中，所以需要设定检查点保存路径
    ssc.sparkContext.setCheckpointDir("cp")
    //创建socket数据流接收器 并设置接收的host port
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("alyhadoop102",9999)
    //对读取到的每行数据分割
    val wordsDS: DStream[String] = lines.flatMap(_.split(" "))
    //结构转换
    val mapDS: DStream[(String, Int)] = wordsDS.map((_,1))
    //
    val stateDS: DStream[(String, Long)] = mapDS.updateStateByKey(
      (seq: Seq[Int], buffer: Option[Long]) => {
        //读取缓冲区中的数据与当前读取的数据合并
        val res: Long = seq.sum + buffer.getOrElse(0L)
        //将每次更新的数据写进缓冲区
        Option(res)
      }
    )
    //打印结果
    stateDS.print
    //启动采集器
    ssc.start()
    //让driver等待采集器结束
    ssc.awaitTermination()
  }
}
