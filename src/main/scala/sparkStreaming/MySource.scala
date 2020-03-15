package sparkStreaming

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket
import java.nio.charset.StandardCharsets

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.receiver.Receiver


object MySource{
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SouceDIY")
    val ssc: StreamingContext = new StreamingContext(conf,Seconds(3))

    val lines: ReceiverInputDStream[String] = ssc.receiverStream(MySourceData("alyhadoop102",9999))

    val wordsDS: DStream[String] = lines.flatMap(_.split(" "))
    val countDS: DStream[(String, Int)] = wordsDS.map((_,1)).reduceByKey(_+_)

    countDS.print
    ssc.start()
    ssc.awaitTermination()
  }
}

object MySourceData {
  def apply(host: String, port: Int): MySourceData = new MySourceData(host, port)

}


class MySourceData(host:String,port:Int) extends Receiver[String](StorageLevel.MEMORY_ONLY){
  // 接收器启动的时候调用该方法
  //
  override def onStart(): Unit = {
    //启动一个新的线程来接收数据
    new Thread("socker Receiver"){
//      new Runnable {
        override def run(): Unit = {
          println("start——————")
          receive()
        }
//      }
    }.start()
  }

  //此方法用来接收数据
  def receive(): Unit = {
    val socket: Socket = new Socket(host,port)
    val reader: BufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream,StandardCharsets.UTF_8))
    var line:String = null
    while ((line =reader.readLine())!=null){
      println("receive——————")
      //将读到的数据发送给spark
      store(line)
    }
    //循环结束， 则关闭资源
    reader.close()
    socket.close()
  }

  override def onStop(): Unit = {

  }
}
