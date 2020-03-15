package transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object transformation_zip {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf 并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("zip").setMaster("local[*]")
    //2.创建SparkContext，该对象是Spark app的入口
    val sc: SparkContext = new SparkContext(conf)
    //3.创建RDD
    val rdd1: RDD[Int] = sc.makeRDD(Array(1,2,3),3)
    //4.创建RDD
    val rdd2: RDD[String] = sc.makeRDD(Array("a","b","c"),3)
    //5.zip
    rdd1.zip(rdd2).collect().foreach(println)
    //6.关闭连接
    sc.stop()
  }
}
