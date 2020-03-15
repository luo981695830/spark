package com.atguigu.bigdata.spark.project.common

import com.atguigu.bigdata.spark.project.util.SparkStreamingEnv
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

trait TApplication {
    def start(appname:String="application",duration:Int=3)(op: =>Unit):TApplication={
      //1.spark环境
       SparkStreamingEnv.init(appname,duration)

      //2.处理
        try{
          op
        }catch {
          case e => e.printStackTrace()
        }
      //3.启动 & 等待
      SparkStreamingEnv.execute()
      this
    }
  def stop(op: =>Unit):Unit={
    //优雅的关闭

  }
}
