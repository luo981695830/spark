package sparkStreaming

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}


object StreamingWordCount {
  def main(args: Array[String]): Unit = {
    //TODO 创建上下文环境
    //sparkSteaming本地模式时不能使用单线程操纵 至少2个线程
    /*
    *  driver运行需要一个
    *  executor需要一个 运行采集器的
    *
    * */
    /*
    *   SparkStreaming底层原理，至少要有两条线程，一条线程用来分配给Receiver
    *   一条线程用来处理接收到的数据
    *
    * */
    /*
    *  如果只有一条线程，那么只会接收数据，不会处理数据
    * */
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("StreamingWordCount")
    val ssc: StreamingContext = new StreamingContext(conf,Seconds(3))
    //上下文环境对象可以根据实际的应用场景创建数据采集器
    //对数据进行处理
    //采集数据
//    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("alyhadoop102",9999)
    val receiverDS: ReceiverInputDStream[String] = ssc.receiverStream(new MyReceiver("alyhadoop102",9999))
    // 将采集数据进行扁平化操作
    val words: DStream[String] = receiverDS.flatMap(_.split(" "))
    //将扁平化后的数据进行结构的转换
    val wordToOne: DStream[(String, Int)] = words.map((_,1))
    //将转换结构后的数据进行聚合处理
    //尾递归
    //SparkStreaming计算是以一个采集周期为单位
    //无法将多个采集周期的数据进行聚合
    //称之无状态
    val count: DStream[(String, Int)] = wordToOne.reduceByKey(_+_)
    //将结果打印出来
    count.print
    //开始接受数据并计算
    //启动采集器  长期执行
    ssc.start()
    //让Driver等待采集器的结束
    ssc.awaitTermination()
  }
}

//自定义数据采集器
//1.继承Receiver

class MyReceiver(host:String,port:Int) extends Receiver[String](StorageLevel.MEMORY_ONLY) {
  private  var socket:Socket= _
  //接收数据方法
  def receive():Unit={
    socket=new Socket(host,port)
    val reader = new BufferedReader(
      new InputStreamReader(
        socket.getInputStream,
        "UTF-8"
      )
    )
    var line = ""
    //当receiver没有关闭，且reader读取到了数据则循环发送给spark
    while((line=reader.readLine())!= null){
      //发送给spark
        store(line)
    }
  }
  override def onStart(): Unit = {
    //启动一个新的线程来接收数据
    new Thread(
      new Runnable {
        override def run(): Unit = {
          receive()
        }
      }
    ).start()
  }

  override def onStop(): Unit = {
    if(socket != null){
      socket.close()
      socket= null
    }
  }
}
