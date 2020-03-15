package transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
*  distinct 去重
* */
object transformation_distinct {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf 并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("distinct").setMaster("local[*]")
    //2.创建SparkContext，该对象是Spark app的入口
    val sc: SparkContext = new SparkContext(conf)
    //3.创建Rdd
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,5,1,2,6,3))
    //4.去重后生成新的Rdd
    val distinctRdd: RDD[Int] = rdd.distinct()
    //5.输出
    distinctRdd.collect().foreach(println)
    //6.对RDD采用多个Task去重，提高并发度
    rdd.distinct(2).collect().foreach(println)
    //7.关闭连接
    sc.stop()
  }
}
