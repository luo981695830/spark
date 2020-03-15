package sparksql

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
*   RDD方式实现求平均工资
* */
object Avg_RDD {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //创建RDD
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("zhangsan", 20), ("lisi", 30), ("wangw", 40)))

    //结构转换
    val mapRDD: RDD[(Int, Int)] = rdd.map {
      case (name, age) => {
        (age, 1)
      }
    }
    val res: (Int, Int) = mapRDD.reduce {
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2)
      }
    }
    //输出结果
    println(res._1/res._2)
    //关闭连接
    sc.stop()

  }

}
