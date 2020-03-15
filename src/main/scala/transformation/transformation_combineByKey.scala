package transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object transformation_combineByKey {
  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3.1 创建第一个RDD
    val list: List[(String, Int)] = List(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98))
    val input: RDD[(String, Int)] = sc.makeRDD(list, 2)

    //3.2 将相同key对应的值相加，同时记录该key出现的次数，放入一个二元组
    val combineRdd: RDD[(String, (Int, Int))] = input.combineByKey(
      (_, 1),
      (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1),
      (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
    )

    //3.3 打印合并后的结果
    combineRdd.collect().foreach(println)

    //3.4 计算平均值
    combineRdd.map {
      case (key, value) => {
        (key, value._1 / value._2.toDouble)
      }
    }.collect().foreach(println)

    //4.关闭连接
    sc.stop()
  }

}
