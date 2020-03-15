package savefile

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.json.JSON.parseFull

/*
*  文件类数据读取与保存：Json文件
*  如果JSON文件中每一行就是一个JSON记录，
*  那么可以通过将JSON文件当做文本文件来读取，
*  然后利用相关的JSON库对每一条数据进行JSON解析
* */
object save_Json {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3.读取Json输入文件
    val jsonRdd: RDD[String] = sc.textFile("input/user.json")
    //4.导入解析Json所需的包并解析Json
    import scala.util.parsing.json.JSON
    val resultRdd: RDD[Option[Any]] = jsonRdd.map(JSON.parseFull)
    //5.输出结果
    resultRdd.collect().foreach(println)
    //关闭连接
    sc.stop()
  }
}
