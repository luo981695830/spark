package sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSql_Demo {
  def main(args: Array[String]): Unit = {
    //创建上下文环境配置对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("spark_sql_demo")
    //创建SparkSession对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    //RDD=>DataFrame=>DataSet 转换需要引入隐式转换规则，否则无法转换
    //spark不是包名，是上下文环境对象名
    import spark.implicits._

    //读取json文件，创建DataFrame{"username":"lisi","age":18}
    val df: DataFrame = spark.read.json("input/user.json")

    /*
    *  DataFrame数据显示的三种语法
    *   语法一：自带的
    *   语法二：SQL风格 前提需要创建视图
    *   语法三：DSL风格
    * */
    //DataFrame 数据显示
//    df.show()
//    +---+--------+
//    | 20|zhangsan|
//    | 18|    lisi|
//    | 16|  wangwu|
//    +---+--------+

    //SQL风格语法
//    df.createOrReplaceTempView("user")
//    spark.sql("select avg(age) from user").show

//    |avg(age)|
//    +--------+
//    |    18.0|
//    +--------+

    //DSL风格语法
//    df.select("username","age").show()
//      |zhangsan| 20|
//      |    lisi| 18|
//      |  wangwu| 16|
//      +--------+---+


  }
}
