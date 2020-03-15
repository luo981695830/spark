package com.atguigu.bigdata.spark.project.application

import com.atguigu.bigdata.spark.project.common.TApplication
import com.atguigu.bigdata.spark.project.controller.HotAdvClickTopNController
import com.atguigu.bigdata.spark.project.util.SparkStreamingEnv

/*
*  每天每地区热门广告 top3
* */

object HotAdvClickTopNApplication extends App with TApplication{
  // 启动应用程序  duration是采集的时间周期
  start("HotAdvClick",3){
    //业务逻辑
    val hotAdvClickTopNController: HotAdvClickTopNController = new HotAdvClickTopNController
    //num取前几
    hotAdvClickTopNController.getRankDatasAnylysis(3).print

//    SparkStreamingEnv.get().sparkContext.makeRDD(List(1,2,3,4)).collect()
  }stop{

  }
}
