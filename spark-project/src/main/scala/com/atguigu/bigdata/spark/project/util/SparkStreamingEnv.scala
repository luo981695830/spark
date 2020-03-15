package com.atguigu.bigdata.spark.project.util

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreamingEnv {
  private val locals: ThreadLocal[StreamingContext] = new ThreadLocal[StreamingContext]

  def init(name:String,dur:Int)={
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(name)
    val ssc: StreamingContext = new StreamingContext(conf,Seconds(dur))
    ssc.sparkContext.setCheckpointDir("cp")
    locals.set(ssc)
  }
  def execute()={
    locals.get().start()
    locals.get().awaitTermination()
  }
  def get()={
      locals.get()
  }
}
