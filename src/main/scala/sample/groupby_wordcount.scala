package sample

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object groupby_wordcount {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("wordcont").setMaster("local[*]")
    //2.创建SparkContext，该对象是Spark App的入口
    val sc: SparkContext = new SparkContext(conf)
    //3.创建一个RDD
    val rdd: RDD[String] = sc.makeRDD(List("hello scala", "hello spark", "hello world"))
    //4.将字符串拆分成一个个单词
    val wordRdd: RDD[String] = rdd.flatMap(_.split("_"))
    //5.将单词进行结构转换 word=>(word,1)
    //    wordRdd.map(word=>(word,1))
    val wordToOneRdd: RDD[(String, Int)] = wordRdd.map((_, 1))
    //6.将转换结构后的数据分组
    val grouppRdd: RDD[(String, Iterable[(String, Int)])] = wordToOneRdd.groupBy(_._1)
    //7.将分组后的数据进行结构转换
    val wordToSum: RDD[(String, Int)] = grouppRdd.map {
      case (word, list) => (word, list.size)
    }
    //8.输出到控制台
    wordToSum.collect().foreach(println)
    //9.停止连接
    sc.stop()
  }
}
