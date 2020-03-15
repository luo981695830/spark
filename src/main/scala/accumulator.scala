import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/*
*  系统累加器
* */
object accumulator {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3.创建RDD
    val dataRdd: RDD[(String, Int)] = sc.makeRDD(List(("a",1),("a",2),("a",3),("a",4)))
    //4.打印单词出现的次数  reduceByKey执行shuffle
    dataRdd.reduceByKey(_+_).collect().foreach(println)

    var sum=0
    //foreach行动算子 运行在executor端
    dataRdd.foreach{
      case(a,count)=>{
        sum+=count
        println("sum="+sum)
      }
    }
    //输出分区数
    println(dataRdd.getNumPartitions)
    //6.打印在Driver端
    println(("a",sum))


    //使用累加器实现数据的聚合功能
    val sum1: LongAccumulator = sc.longAccumulator("sum1")

    dataRdd.foreach{
      case(a,count)=>{
        //使用累加器
        sum1.add(count)
      }
    }
    //获取累加器
    println(sum1.value)
    //关闭连接
    sc.stop()
  }
}
