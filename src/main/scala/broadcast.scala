import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
*  广播变量
* */
object broadcast {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    /*
    *
    *   方式一  join
    ////可以实现聚合，但是会存在笛卡尔积以及shuffle  (a,(1,4)),(b,(2,5)),(c,(3,6)
    //val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))
    //val rdd2: RDD[(String, Int)] = sc.makeRDD(List(("a", 4), ("b", 5), ("c", 6)))
    //val newRDD: RDD[(String, (Int, Int))] = rdd1.join(rdd2)
    //newRDD.collect().foreach(println)
    *
    * */
    /*
    *   方式二: 广播变量
    * */
    //(a,(1,4)),(b,(2,5)),(c,(3,6)
    // 创建RDD
    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))
    val list: List[(String, Int)] = List(("a", 4), ("b", 5), ("c", 6))

    //创建一个广播变量
    val broadcastList: Broadcast[List[(String, Int)]] = sc.broadcast(list)
    //map结构转换
    val resRDD: RDD[(String, (Int, Int))] = rdd1.map {
      case (k1, v1) => {
        //声明临时变量，用来接收相同k的值
        var v2 = 0
        for ((k3, v3) <- list) {
          if (k1 == k3) {
            //如果k相同，则将值赋给临时变量v2
            v2 = v3
          }
        }
        //将每次匹配好的新元组返回
        (k1, (v1, v2))
      }
    }
    //调用collect行动算子返回的是数组类型
    val tuples: Array[(String, (Int, Int))] = resRDD.collect()
    //调用遍历函数，输出数组中的元素
    tuples.foreach(println)

    //关闭连接
    sc.stop()

  }
}
