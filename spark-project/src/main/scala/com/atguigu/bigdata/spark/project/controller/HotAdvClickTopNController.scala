package com.atguigu.bigdata.spark.project.controller

import com.atguigu.bigdata.spark.project.bean.HotAdvClick
import com.atguigu.bigdata.spark.project.service.HotAdvClickTopNService
import org.apache.spark.streaming.dstream.DStream

class HotAdvClickTopNController {
    private val hotAdvClickTopNService = new HotAdvClickTopNService

    def getRankDatasAnylysis(num:Int)={
        hotAdvClickTopNService.getRankDatasAnylysis(num)
    }
}
