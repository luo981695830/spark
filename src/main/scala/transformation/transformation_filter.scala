package transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
*  filter 过滤
*
* */
object transformation_filter {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf 并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("filter").setMaster("local[*]")
    //2.创建SparkContext 该对象是Spark App入口
    val sc = new SparkContext(conf)
    //3.创建RDD
    val rdd: RDD[Int] = sc.makeRDD(Array(1,2,3,4),2)
    //4.过滤出符合条件的数据
    val filterRdd: RDD[Int] = rdd.filter(_%2==0)
    //5.将数据输出到控制台
    filterRdd.collect().foreach(println)
    //6.停止连接
    sc.stop()
  }
}
