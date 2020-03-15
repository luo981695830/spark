package sparksql

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/*
*  通过编程方式将RDD转换为DataFrame
* */
object RDD_To_DF {
  def main(args: Array[String]): Unit = {
    /*
    * 创建SparkSession
    *   SparkSession是创建DataFrame和执行SQL的入口
    *
    * */
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("Word Count")
      .getOrCreate()
    //使用SparkSession的方式创建sparkContext
    val sc: SparkContext = spark.sparkContext
    //创建RDD  元素为元组的数组
    val rdd: RDD[(String, Int)] = sc.parallelize(Array(("lisi", 10), ("zs", 20), ("zhiling", 40)))
    // 映射出来一个 RDD[Row], 因为 DataFrame其实就是 DataSet[Row]
    //结构转换，将数组中的元素转换为Row形式
    val rowRdd: RDD[Row] = rdd.map(x => Row(x._1, x._2))
    // 创建 StructType 类型  元数据信息
    val types = StructType(Array(StructField("name", StringType), StructField("age", IntegerType)))
    // 将元数据信息和数据组合在一起 封装成DataFrame
    val df: DataFrame = spark.createDataFrame(rowRdd, types)
    // 控制台显示
    df.show
  }
}
