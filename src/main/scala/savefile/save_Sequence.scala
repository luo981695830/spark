package savefile

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
* 文件类数据读取与保存：Sequence文件
* */

object save_Sequence {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[1]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)
    //3.创建Rdd
//    val dataRdd: RDD[(Int, Int)] = sc.makeRDD(Array((1,2),(3,4),(5,6)))
    //4.保存数据为SequenceFile
//    dataRdd.saveAsSequenceFile("output1")
    //5.读取SequenceFile文件
    sc.sequenceFile[Int,Int]("output1").collect().foreach(println)

    //关闭连接
    sc.stop()

  }
}
