package savefile

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
* 文件类数据读取与保存：text文件
* */
object save_text {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("save_text").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3.读取输入文件
    val inputRdd: RDD[String] = sc.textFile("input/1.txt")

    //4.保存数据
    inputRdd.saveAsTextFile("output")
    //关闭连接
    sc.stop()
  }
}
