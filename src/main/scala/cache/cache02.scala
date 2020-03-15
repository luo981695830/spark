package cache

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
*  采用reduceByKey 自带缓存
* */
object cache02 {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3.创建RDD 读取指定位置文件
    val lineRdd: RDD[String] = sc.textFile("input")
    //4.创建flatMap RDD
    val wordRdd: RDD[String] = lineRdd.flatMap(_.split(" "))
    //5.转换结构
    val wordToOneRdd: RDD[(String, Int)] = wordRdd.map {
      word => {
        println("**********")
        (word, 1)
      }
    }
    //采用reduceByKey 自带缓存
    val wordByKeyRdd: RDD[(String, Int)] = wordToOneRdd.reduceByKey(_+_)
    //cache 操作会增加血缘关系，不改变原有的血缘关系
    println(wordByKeyRdd.toDebugString)

    // 数据缓存
    //wordByKeyRdd.cache()

    //触发执行逻辑
    wordByKeyRdd.collect()
    println("-------")
    println(wordByKeyRdd.toDebugString)

    //再次出发执行逻辑
    wordByKeyRdd.collect()
    //关闭连接
    sc.stop()
  }
}
