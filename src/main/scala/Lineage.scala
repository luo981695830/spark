import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
*  Lineage血缘
* */
object Lineage {
  def main(args: Array[String]): Unit = {
    //1.创建 sparkConf 设置app名称
    val conf: SparkConf = new SparkConf().setAppName("linegae").setMaster("local[*]")
    //2.创建SparkContext 该对象是Spark App入口
    val sc: SparkContext = new SparkContext(conf)
    //3.创建RDD
    val fileRdd: RDD[String] = sc.textFile("input/1.txt")
    println("----------------------")
    // 拆分元素
    val wordRdd: RDD[String] = fileRdd.flatMap(_.split(" "))
    println("----------------------")
    //转换结构
    val mapRdd: RDD[(String, Int)] = wordRdd.map((_,1))
    println("----------------------")
    //聚合
    val resRdd: RDD[(String, Int)] = mapRdd.reduceByKey(_+_)
    println(resRdd.toDebugString)
    //行动算子 提交job
    resRdd.collect()
    Thread.sleep(100000)
    //关闭连接
    sc.stop()
  }
}
