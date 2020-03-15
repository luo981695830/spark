package transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
*   coalesce 重新分区
* */
object transformation_coalesce {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf 并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("distinct").setMaster("local[*]")
    //2.创建SparkContext，该对象是Spark app的入口
    val sc: SparkContext = new SparkContext(conf)
    //3.创建RDD
    val rdd: RDD[Int] = sc.makeRDD(Array(1,2,3,4),4)
    //4.缩减分区
    val coalesceRdd: RDD[Int] = rdd.coalesce(2)
//    //5.创建RDD
//    val rdd1: RDD[Int] = sc.makeRDD(Array(1,2,3,4,5,6),3)
//    //6.缩减分区
//    val coalesceRdd1: RDD[Int] = rdd.coalesce(2)
    //查看对应分区数据
    coalesceRdd.mapPartitionsWithIndex(
      (index,datas)=>{
        //打印每个分区数据，并带分区号
        datas.foreach(data=>{
          println(index+"=>"+data)
        })
        //返回分区的数据
        datas
      }
    )
    //关闭连接
    sc.stop()
  }
}
