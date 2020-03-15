//package project
//
//import org.apache.spark.rdd.RDD
//import org.apache.spark.{SparkConf, SparkContext}
//
//import scala.collection.mutable.ListBuffer
//
///*
//*  需求2：Top10热门品类中每个品类的Top10活跃Session统计
//*  对于排名前10的品类，分别获取每个品类点击次数排名前10的sessionId
//*
//*   对于top10的品类，每一个都要获取对它点击次数排名前10的sessionId
//*   可以让我们看到，对某个用户群体最感兴趣的品类，
//*   各个品类最感兴趣最典型的用户的session的行为。
//*
//*
//* */
//object pro2 {
//  def main(args: Array[String]): Unit = {
//    //1.创建SparkConf并设置App名称
//    val conf: SparkConf = new SparkConf().setAppName("top10").setMaster("local[*]")
//
//    //2.创建SparkContext，该对象是提交Spark App的入口
//    val sc: SparkContext = new SparkContext(conf)
//
//    //3.需求一：top10热门品类
//    //获取原始数据
//    val dataRDD: RDD[String] = sc.textFile("input/user_visit_action.txt")
//
//    /*
//    *   sc.textFile是按行读的  每一行执行map里面的逻辑，
//    *   按“ ”分割 每一行返回包含两个String类型的数组
//    *   数组元素1是时间-用户id-sessionid-页面id-时间
//    *   数组元素2是时间-搜索关键字-点击品类id和产品id-下单品类id和产品id-支付品类ids和产品ids-城市id
//    *
//    */
//    //将原始数据进行转换
//    val actionRDD: RDD[UserVisitAction] = dataRDD.map(
//      data => {
//        //获取一行数据
//        val datas: Array[String] = data.split("_")
//        //将数据封装到UserVisitAction
//        UserVisitAction(
//          datas(0), //date  用户点击行为的日期
//          datas(1).toLong, //user_id 用户ID
//          datas(2), //session_id session的ID
//          datas(3).toLong, //page_id 某个页面的ID
//          datas(4), //action_time 动作的时间点
//          datas(5), //search_keyword 用户搜索的关键词
//          datas(6).toLong, //click_category_id 某一个商品品类的ID
//          datas(7).toLong, //click_product_id 某一个商品的ID
//          datas(8), //order_category_ids 一次订单中所有品类的ID集合
//          datas(9), //order_product_ids 一次订单中所有商品的ID集合
//          datas(10), //pay_category_ids 一次支付中所有品类的ID集合
//          datas(11), //pay_product_ids 一次支付中所有商品的ID集合
//          datas(12).toLong //city_id 城市ID
//        )
//      }
//    )
//    //测试actionRDD  输出3条数据
//    //    actionRDD.collect().take(3).foreach(println)
//    /*
//    *  经过map：
//    *        遍历原始数据集 -> 分割 -> 封装
//    *   actionRDD 是包含多个UserVisitAction的对象
//    * */
//    //再一次转换结构  取出品类 点击数 下单数 支付数 并封装为元组
//    val infoRDD: RDD[CategoryCountInfo] = actionRDD.flatMap(
//      //flatMap中传入匿名函数
//      // 对actionRDD中的元素逻辑遍历
//      action => {
//        //模式匹配  match关键字
//        action match {
//          //匹配样例类
//          case act: UserVisitAction => {
//            /*
//            * 对匹配到的元素多分支互斥判断
//            * */
//            if (act.click_category_id != -1) {
//              //点击
//              /*
//              *  判断是否是品类点击，如果是返回品类id 点击数1 下单数0 支付数0
//              *  点击每次只能点击一个  所以每一次的数据只有一个
//              * */
//              List(CategoryCountInfo(act.click_category_id.toString, 1, 0, 0))
//            } else if (act.order_category_ids != "null") {
//              //下单
//              /*
//              *  判断品类是否下单
//              *  一个订单可以包含多个品类
//              *  下单数据是多个品类id以,连接的字符串
//              *   先以，切割字符串 获取多个品类id的数组
//              *   然后遍历数组，将每次遍历的元素转换结构为元组添加到集合中
//              *   最后将集合返回
//              * */
//              val list: ListBuffer[CategoryCountInfo] = new ListBuffer[CategoryCountInfo]
//              val ids: Array[String] = act.order_category_ids.split(",")
//              for (id <- ids) {
//                list.append(CategoryCountInfo(id, 0, 1, 0))
//              }
//              list
//            } else if (act.pay_category_ids != "null") {
//              //支付
//              /*
//              *   判断品类是否支付
//              *   一次支付可以支付多个品类
//              *   支付数据是多个品类id以,连接的字符串
//              *   先以，切割字符串 获取多个品类id的数组
//              *   然后遍历数组，将每次遍历的元素转换结构为元组添加到集合中
//              *   最后将集合返回
//              * */
//              val list: ListBuffer[CategoryCountInfo] = new ListBuffer[CategoryCountInfo]
//              val ids: Array[String] = act.pay_category_ids.split(",")
//              for (id <- ids) {
//                list.append(CategoryCountInfo(id, 0, 0, 1))
//              }
//              list
//            } else {
//              Nil
//            }
//          }
//          case _ => Nil
//        }
//      }
//    )
//
//    //将相同的品类分成一组
//    val groupRDD: RDD[(String, Iterable[CategoryCountInfo])] = infoRDD.groupBy(_.categoryId)
//
//    //将分组后的数据进行聚合处理
//    //返回的数据结构为(品类id,(品类id,clickCount,OrderCount,PayCount))
//    val mapRDD: RDD[CategoryCountInfo] = groupRDD.mapValues(
//      datas => {
//        datas.reduce(
//          (info1, info2) => {
//            //            val orderCount = info1.orderCount + info2.orderCount
//            //            val clickCount = info1.clickCount + info2.clickCount
//            //            val payCount = info1.payCount + info2.payCount
//            info1.orderCount = info1.orderCount + info2.orderCount
//            info1.clickCount = info1.clickCount + info2.clickCount
//            info1.payCount = info1.payCount + info2.payCount
//            //计算之后的info1
//            info1
//
//            //            (info1.categoryId, orderCount, clickCount, payCount)
//          }
//        )
//      }
//    ).map(_._2)
//    /*
//    * (4,CategoryCountInfo(4,3890,3831,1271))
//    * (4,(4,3890,3831,1271))
//    * */
//    //将聚合后的数据排序  默认升序 ascending=false 倒序
//    val sortRDD: RDD[CategoryCountInfo] = mapRDD.sortBy(info=>(info.clickCount,info.orderCount,info.payCount),false)
//    //结构转换  解封对象 将对象的属性值转成成元组（品类id，点击数，下单数，支付数）
//    val tupleRDD: RDD[(String, Long, Long, Long)] = sortRDD.map(info=>(info.categoryId,info.clickCount,info.orderCount,info.payCount))
//    //取前10名
//    val takeRDD: Array[(String, Long, Long, Long)] = tupleRDD.take(10)
//
//    //(15,6120,1672,1259)
//
//    //得到top的品类的id
//    val categoryIdTop10: Array[String] = takeRDD.map(line=>line._1)
//
//    /*
//    *  筛选出只包含top10品类的id的用户行为
//    *  以top10中的品类id作为条件
//    *  对数据封装的对象的原始数据集过滤
//    *  过滤后得到的是包含top10的
//    *
//    * */
//    val filterActionRDD: RDD[UserVisitAction] = actionRDD.filter(
//      action => categoryIdTop10.contains(action.click_category_id.toString)
//    )
//    /*
//    *  结构转换
//    *  因为sessionId的排名是以点击次数作为排名
//    *  所以需要的数据是以(品类,sessionId)作为key 每一条数据的值为1
//    *  取出封装对象的属性  再次封装成元组((品类id,sessionId),1)
//    * */
//    val sessionRDD: RDD[((Long, String), Int)] = filterActionRDD.map(
//      action => ((action.click_category_id, action.session_id), 1)
//    )
//    //对点击次数求和 -> 数据结构 ((品类id,sessionId),count)
//    // 结构转换为(品类id,(sessionId,count)) 方便后面对session排序
//    val cate_session_countRDD: RDD[(Long, (String, Int))] = sessionRDD.reduceByKey(_ + _).map {
//      case ((category, session), count) => (category, (session, count))
//    }
//
//    /*
//    * 按照品类id进行分组
//    * */
//    val group_cate_session_RDD: RDD[(Long, Iterable[(String, Int)])] = cate_session_countRDD.groupByKey()
//
//    //排序
//    val categorySessionRDD: RDD[CategorySession] = group_cate_session_RDD.flatMap {
//      //模式匹配 将RDD中的每一个元素当作一个整体 遍历
//      case (cid, it) => {
//        /*
//        * it的数据结构是Iterable[(String, Int)])
//        * toList转换为集合
//        * sortBy()排序 以(String, Int)中的点击数排序 传入排序规则  这里用到柯里化
//        *  sortBy源码 def sortBy[B](f: A => B)(implicit ord: Ordering[B]): Repr = sorted(ord on f)
//        *  排序规则(Ordering.Int.reverse) 隐式转换
//        * */
//        val list: List[(String, Int)] = it.toList.sortBy(_._2)(Ordering.Int.reverse).take(10)
//        val result: List[CategorySession] = list.map {
//          case (sid, count) => CategorySession(cid.toString, sid, count)
//        }
//        result
//      }
//    }
//    categorySessionRDD.collect().foreach(println)
//
//
//    //关闭连接
//    sc.stop()
//
//  }
//}
//
///*
//*   定义CategorySession类，封装最终写入到数据库的数据
//* */
//
//case class CategorySession(
//                          categoryId:String, //品类ID
//                          sessionId:String,//会话ID
//                          clickCount:Long //点击次数
//                          )
//
////用户访问动作表
//case class UserVisitAction(
//                            date:String,//用户点击行为的日期
//                            user_id:Long,//用户的ID
//                            session_id:String,//session的ID
//                            page_id:Long,//某个页面的ID
//                            action_time:String,//动作的时间点
//                            search_keyword:String,//用户搜索的关键词
//                            click_category_id:Long,//某一个商品品类的ID
//                            click_product_id:Long,//某一个商品的ID
//                            order_category_ids:String,//一次订单中所有品类的ID集合
//                            order_product_ids:String,//一次订单中所有商品的ID集合
//                            pay_category_ids:String,//一次支付中所有品类的ID集合
//                            pay_product_ids:String,//一次支付中所有商品的ID集合
//                            city_id:Long//城市ID
//                          )
//
////输出结果表
//case class CategoryCountInfo(
//                              val categoryId:String,//品类id
//                              var  clickCount:Long,//点击次数
//                              var  orderCount:Long,//订单次数
//                              var  payCount:Long//支付次数
//                            )
