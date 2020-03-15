package transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object transformation_mapPartitionsWithIndex {
  def main(args: Array[String]): Unit = {
    //1.创建Sparkconf 并设置app名称
    val conf: SparkConf = new SparkConf().setAppName("mapPartitionsWithIndex").setMaster("local[*]")
    //2.创建SparkContext，该对象是提交SparkApp的入口
    val sc = new SparkContext(conf)
    //3.创建Rdd
    val rdd: RDD[Int] = sc.makeRDD(1 to 4 ,2)
    //4.调用mapPartitionsWithIndex方法
    //使每个元素跟所在分区好形成一个元组，组成一个新的RDD
    val indexRdd: RDD[(Int, Int)] = rdd.mapPartitionsWithIndex((index,items)=>{items.map((index,_))})
    //5.打印RDD中的数据
    indexRdd.collect().foreach(println)
    //6.关闭连接
    sc.stop()
  }
}
