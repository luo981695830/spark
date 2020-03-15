package sample

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
*   字段含义：时间戳        省份  城市  用户  广告
*   数据样例：1516609143867  6     7     64    16
*   需求：统计出每一个省份广告被点击次数的Top3
*   最后输出结果：(省份,(广告，次数))
*
*
* */

/*
*   问题 map转换结构时，如果匹配元组 必须加case?
*
* */

object Top3 {
  def main(args: Array[String]): Unit = {
    //创建sparkConf 设置app名称
    val conf: SparkConf = new SparkConf().setAppName("top3").setMaster("local[*]")
    //创建SparkContext spark入口
    val sc = new SparkContext(conf)
    //读取文件
    val rdd: RDD[String] = sc.textFile("agent.log")
    //提取 省份 广告数据 以元组的形式输出  (省份-广告，1)
    val mapRdd: RDD[(String, Int)] = rdd.map {
      line =>
        val datas: Array[String] = line.split(" ")
        (datas(1)+"-"+datas(4),1)
    }
    //按照相同的省份以及广告进行聚合 (省份-广告，3)
    val reduceRdd: RDD[(String, Int)] = mapRdd.reduceByKey(_+_)

    //二次结构转换 (省份,(广告，3))
    val mapRdd1: RDD[(String, (String, Int))] = reduceRdd.map {
      case (ad, count) => {
        val ad_countArray: Array[String] = ad.split("-")
        (ad_countArray(0), (ad_countArray(1), count))
      }
    }
    //按照省份分组
    val groupRdd: RDD[(String, Iterable[(String, Int)])] = mapRdd1.groupByKey()
    //排序
    val sortRdd: RDD[(String, List[(String, Int)])] = groupRdd.mapValues {
      values => {
        values.toList.sortWith {
          (left, right) => {
            left._2 > right._2
          }
        }.take(3)
      }
    }

    //输出到控制台
    sortRdd.collect().take(3).foreach(println)

    //关闭连接
    sc.stop()
  }
}
