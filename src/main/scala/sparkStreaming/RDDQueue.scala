package sparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object RDDQueue {
  def main(args: Array[String]): Unit = {
    //1、初始化Spark配置信息 创建SparkConf对象 设置app名称
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDDQueue")
    //2、初始化SparkStreamingContext  Seconds表示时间间隔
    val ssc: StreamingContext = new StreamingContext(conf,Seconds(3))
    val sc = ssc.sparkContext

    //3、创建一个可变队列
    val rddQueue: mutable.Queue[RDD[Int]] = mutable.Queue[RDD[Int]]()
    //4、创建 QueueInputDStream 输入数据流对象
    val inputStream: InputDStream[Int] = ssc.queueStream(rddQueue,false)
    /*
    *  abstract class InputDStream[T: ClassTag](_ssc: StreamingContext) extends DStream[T](_ssc)
    *  rddDS是InputDStream对象 继承了DStream
    *  reduce print都是DStream中方法
    * */
    //5、处理队列中的RDD数据
    val mappedStream = inputStream.map((_,1))
    val reducedStream: DStream[(Int, Int)] = mappedStream.reduceByKey(_+_)
    //6、打印结果
    reducedStream.print()
    //7、启动任务
    ssc.start

    //8、循环的方式向队列中添加RDD
    for(elem <- 1 to 5){
      rddQueue += ssc.sparkContext.makeRDD(1 to 300,10)
      Thread.sleep(2000)
    }
    //让driver等待收集器执行完毕
    ssc.awaitTermination()
  }
}
