package transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object transformation_flatMap {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("flatMap").setMaster("local[*]")
    //2.创建SparkContext并设置App名称
    val sc = new SparkContext(conf)
    //3.创建Rdd
    val listRdd: RDD[List[Int]] = sc.makeRDD(List(List(1,2),List(3,4),List(5,6),List(7)),2)
    //4.把所有子集合中数据取出放到一个大的集合中
    listRdd.flatMap(list=>list).collect().foreach(println)
    //5.关闭连接
    sc.stop()
  }
}
