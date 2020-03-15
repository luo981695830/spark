package transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object transformation_map {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("map").setMaster("local[*]")
    //2.创建SparkContext，该对象是提交SparkApp入口
    val sc: SparkContext = new SparkContext(conf)
    //3.创建RDD
    val rdd: RDD[Int] = sc.makeRDD(1 to 4 ,2)
    //4.调用map方法，每个元素乘以2
    val mapRdd: RDD[Int] = rdd.map(_*2)
    //5.输出RDD中的数据
    mapRdd.collect().foreach(println)
    //6.关闭连接
    sc.stop()
  }
}
