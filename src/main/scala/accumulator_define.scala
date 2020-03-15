import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/*
*  自定义累加器，统计当前RDD中所有以“H”开头的单词个数
* */
object accumulator_define {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3.创建RDD
    val rdd: RDD[String] = sc.makeRDD(List("Hello", "Hello", "Hello", "Hello", "Hello", "Spark", "Spark"))
    //4.定义一个累加器变量
    val myAC: MyAccumulator = new MyAccumulator
    //5.注册累加器到SC
    sc.register(myAC)
    //6.使用累加器
    rdd.foreach{
      word=>{
        myAC.add(word)
      }
    }
    //7.输出结果
    println(myAC.value)
    //关闭连接
    sc.stop()
  }
}
/*
*   声明累加器
*   1.继承AccumulateV2，设定输入、输出泛型
*   2.重新方法
* */
class MyAccumulator extends AccumulatorV2[String,mutable.Map[String,Int]]{

  var map: mutable.Map[String, Int] = mutable.Map[String,Int]()
  
  //判断是否为初始状态
  override def isZero: Boolean = map.isEmpty
  //拷贝
  override def copy(): AccumulatorV2[String, mutable.Map[String, Int]] = {
    var newMC: MyAccumulator = new MyAccumulator
    newMC.map = this.map
    newMC
  }
  //重置
  override def reset(): Unit = map.clear()
  //增加
  override def add(inputStr:String): Unit = {
    if(inputStr.startsWith("H")){
      map(inputStr)=map.getOrElse(inputStr,0)+1
    }
  }
  //合并
  override def merge(other: AccumulatorV2[String,mutable.Map[String,Int]]): Unit = {
    //当前Executor的map
    var map1 = map
    //另一个需要合并的Executor=map
    var map2=other.value
    //将合并的结果赋值给map
    map = map1.foldLeft(map2){
      (mmpp,kv)=>{
        val word = kv._1
        val count = kv._2
        mmpp(word) = mmpp.getOrElse(word,0)+count
        mmpp
      }
    }
  }
  //获取值
  override def value:mutable.Map[String,Int] = map
}