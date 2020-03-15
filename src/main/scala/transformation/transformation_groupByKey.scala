package transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object transformation_groupByKey {
  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3具体业务逻辑
    //3.1 创建第一个RDD
    val rdd = sc.makeRDD(List(("a",1),("b",5),("a",5),("b",2)))

    //3.2 将相同key对应值聚合到一个Seq中
    val group: RDD[(String, Iterable[Int])] = rdd.groupByKey()

    //3.3 打印结果
    group.collect().foreach(println)

    //3.4 计算相同key对应值的相加结果
    group.map(t=>(t._1,t._2.sum)).collect().foreach(println)

    //4.关闭连接
    sc.stop()
  }


}
