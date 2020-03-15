//package cache
//
//import org.apache.spark.rdd.RDD
//import org.apache.spark.{SparkConf, SparkContext}
//
///*
//*  checkpoint 检查点
//* */
//object checkpoint01 {
//  def main(args: Array[String]): Unit = {
//    //1.创建SparkConf并设置App名称
//    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
//
//    //2.创建SparkContext，该对象是提交Spark App的入口
//    val sc: SparkContext = new SparkContext(conf)
//
//    //需要设置路径，否则抛异常:checkpoint directory has not been set in the SparkContext
//    sc.setCheckpointDir("./checkpoint1")
//
//    //3.创建RDD 读取指定位置文件 :hello atguigu atguigu
//    val lineRdd: RDD[String] = sc.textFile("input/1.txt")
//    //4.flatMap rdd
//    val wordRdd: RDD[String] = lineRdd.flatMap(_.split(" "))
//    //5.转换结构
//    val wordToOneRdd: RDD[(String, Long)] = wordRdd.map {
//      word => {
//        (word, System.currentTimeMillis())
//      }
//    }
//    wordRdd.aggregate()
//    //6.增加缓存，避免再重新跑一个job做checkpoint
////    wordToOneRdd.cache()
//    //7.数据检查点：针对wordToOneRdd做检查点计算
//    wordToOneRdd.checkpoint()
//    //8.触发执行逻辑   立即启动一个新的Job来专门做checkpoint运算
//    wordToOneRdd.collect().foreach(println)
//
//    //9.再次触发执行逻辑
//    wordToOneRdd.collect().foreach(println)
//    wordToOneRdd.collect().foreach(println)
//
//    //调用Thread.sleep 阻塞  这样就可以在网页端查看任务
//    Thread.sleep(1000000)
//    //关闭连接
//    sc.stop()
//
//  }
//}
