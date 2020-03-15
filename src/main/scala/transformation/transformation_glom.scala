package transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
*  glom 分区转换数组
* */
object transformation_glom {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("glom").setMaster("local[*]")
    //2.创建SparkContext，该对象是提交SparkApp的入口
    val sc = new SparkContext(conf)
    //3.创建RDD
    val rdd: RDD[Int] = sc.makeRDD(1 to 4,2)
    //4.求出每个分区的最大值
    val maxRdd: RDD[Int] = rdd.glom().map(_.max)
    val glom_maxRdd: RDD[(Int, Int)] = maxRdd.mapPartitionsWithIndex((index,items)=>{items.map((index,_))})
    glom_maxRdd.collect().foreach(println)
    //5.输出所有分区的最大值的和
    println(maxRdd.collect().sum)
    /*
    *  输出数组中的每个元素
    * */
//    val glomRdd: RDD[Array[Int]] = rdd.glom()
//    glomRdd.collect().foreach(x=>{
//      for (elem <- x) {
//        println(elem)
//      }
//    })
    //停止连接
    sc.stop()
  }
}
