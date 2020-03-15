package transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
*  groupby  分组
* */
object transformation_groupby {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("grouby").setMaster("local[*]")
    //2.创建SparkContext 该对象是提交SparkApp的入口
    val sc = new SparkContext(conf)
    //3.创建RDD
    val rdd: RDD[Int] = sc.makeRDD(1 to 4,2)
    //4.将每个分区的数据放到一个数组并收集到Driver端打印
    rdd.groupBy(_%2).collect().foreach(println)
    //5.创建RDD
    val rdd1: RDD[String] = sc.makeRDD(List("hello","hive","hadoop","spark","scala"))
    //6.按照首字母第一个单词相同分组
//    rdd1.groupBy(str=>str.substring(0,1)).collect().foreach(println)
    rdd1.groupBy(_.substring(0,1)).collect().foreach(println)

    //停止连接
    sc.stop()
  }
}
