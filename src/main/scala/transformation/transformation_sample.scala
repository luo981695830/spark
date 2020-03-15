package transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
*  sample采样
* */
object transformation_sample {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf 并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("sample").setMaster("local[*]")
    //2.创建SparkContext，该对象是Spark app的入口
    val sc: SparkContext = new SparkContext(conf)
    //3.创建Rdd
    val rdd: RDD[Int] = sc.makeRDD(1 to 10)
    //4.打印放回抽样结果
    rdd.sample(true,0.4,2).collect().foreach(println)
    //5.打印放回抽样
    rdd.sample(false,0.2,3).collect().foreach(println)
    //6.关闭连接
    sc.stop()
  }
}
