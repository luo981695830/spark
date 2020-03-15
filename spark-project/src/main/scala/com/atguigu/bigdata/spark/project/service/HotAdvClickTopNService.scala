package com.atguigu.bigdata.spark.project.service

import java.text.SimpleDateFormat
import java.util.Date

import com.atguigu.bigdata.spark.project.bean
import com.atguigu.bigdata.spark.project.dao.HotAdvClickTopNDao
import com.atguigu.bigdata.spark.project.mock.RedisUtil
import org.apache.spark.streaming.dstream.DStream
import org.json4s.jackson.JsonMethods
import redis.clients.jedis.Jedis

class HotAdvClickTopNService {
    private val hotAdvClickTopNDao: HotAdvClickTopNDao = new HotAdvClickTopNDao

    /*
    *  逻辑
    *  TODO 每天每地区热门广告 Top3
    * */
    def getRankDatasAnylysis(num:Int)={
        val kafkaDS:DStream[String] = hotAdvClickTopNDao.readKafkaData()
        //1.将获取的数据进行结构转换
        val advDS: DStream[(String, Int)] = kafkaDS.map {
            line => {
                //对一条字符串数据切割 返回单个字段的数组
                val datas: Array[String] = line.split(",")
                //日期格式化
                val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
                //取出时间戳(格式化) 地区 广告 并拼接 日期_地区_广告
                // 转换结构 然后以(日期_地区_广告,1) 元组形式返回
                (sdf.format(new Date(datas(0).toLong)) + "_" + datas(1) + "_" + datas(4), 1)
            }
        }

        //2.使用有状态的操作完成数据的聚合处理
        val stateDS: DStream[(String, Long)] = advDS.updateStateByKey(
            (seq: Seq[Int], buffer: Option[Long]) => {
                //流式操作 获取一个时间周期中的数据并和之前的数据累加
                val sum: Long = seq.sum + buffer.getOrElse(0L)
                //存储
                //(日期_地区_广告,sum)
                Option(sum)
            }
        )

        //3.将聚合的结果进行结构的转换
        // 将转换结构后的数据根据时间进行分组
        val tsGroupDS: DStream[(String, Iterable[(String, Long)])] = stateDS.map {
            case (k, sum) => {
                val ks: Array[String] = k.split("_")
                //(日期_地区_广告,sum)->(日期,(地区_广告,sum))
                (ks(0), (ks(1) + "_" + ks(2), sum))
            }
            //以日期分组
        }.groupByKey()

        //4.将分组后的数据对区域进行分组处理  (日期,(地区,(广告,sum)))
        val resultDS: DStream[(String, Map[String, List[(String, Long)]])] = tsGroupDS.mapValues(//mapValues只对values处理  (地区_广告,sum)
            list => {
                val areaGroupTuple: Iterable[(String, (String, Long))] = list.map {
                    case (k, sum) => {
                        val ks: Array[String] = k.split("_")
                        //(地区_广告,sum)->(地区(广告,sum))
                        (ks(0), (ks(1), sum))
                    }
                }
                //groupBy 自定义分组规则  按照地区分组
                val areaGroupMap: Map[String, Iterable[(String, (String, Long))]] = areaGroupTuple.groupBy(_._1)

                //将分组后的数据(adv,sum)进行降序，去前N个
                areaGroupMap.mapValues(
                    mapList => {
                        //元组转集合
                        val advToCountList: List[(String, Long)] = mapList.map(_._2).toList
                        //集合排序 自定义排序规则
                        advToCountList.sortWith(
                            (left, right) => {
                                left._2 > right._2
                            }
                        ).take(num)
                    }
                )
            }
        )

        // 将结果数据进行遍历，保存到redis中
        //(日期,(地区,(广告,sum)))
//        resultDS.foreachRDD(//调用行动算子 获取数据
//            rdd=>{ //对生成的数据进行处理
//                rdd.foreach(
//                    data=>{
//                        val ts: String = data._1    //时间
//                        val data1: Map[String, List[(String, Long)]] = data._2 //(地区,(广告,sum))
//                        val client: Jedis = RedisUtil.getJedisClient
//                        data1.foreach(
//                            data11=>{
//                                import org.json4s.JsonDSL._
//                                //向Redis中保存数据应该都是字符串类型
//                                //List集合数据默认生成的字符串不是JSON格式
//                                // 需要引入特殊的依赖类库进行转换
//                                val adsCountJsonString: String = JsonMethods.compact(JsonMethods.render(data11._2))
//                                client.hset("area:ads:top3:"+ts,data11._1,adsCountJsonString)
//                            }
//                        )
//                    }
//                )
//            }
//        )
        //将排完序之后的数据返回
        tsGroupDS
    }
}
