package transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object transformation_mapPartitions {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("mappartition").setMaster("local[*]")
    //2.创建SparkContext 提交SparkApp入口
    val sc = new SparkContext(conf)
    //3.创建rdd
    val rdd: RDD[Int] = sc.makeRDD(1 to 4,2)
    //4.调用mapPartitions方法，每个元素乘以2
    val mpRdd: RDD[Int] = rdd.mapPartitions(_.map(_*2))
    //5.打印Rdd中的数据
    mpRdd.collect().foreach(println)
    //6.关闭连接
    sc.stop()
  }
}
