package transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object transformation_union {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf 并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("distinct").setMaster("local[*]")
    //2.创建SparkContext，该对象是Spark app的入口
    val sc: SparkContext = new SparkContext(conf)
    //3.创建RDD
    val rdd1: RDD[Int] = sc.makeRDD(1 to 4)
    //4.创建RDD
    val rdd2: RDD[Int] = sc.makeRDD(4 to 8)
    //5.并集
    rdd1.union(rdd2).collect().foreach(println)
    //6.关闭连接
    sc.stop()
  }
}
