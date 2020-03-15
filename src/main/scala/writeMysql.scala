import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
* 往mysql写数据
* */

object writeMysql {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[2]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3.定义连接mysql的参数
    val driver="com.mysql.jdbc.Driver"
    val url="jdbc:mysql://localhost:3306/test"
    val username="root"
    val passwd = "root"
    //4.创建RDD
    val rdd: RDD[(String, String)] = sc.makeRDD(List(("a","11"),("b","22"),("c","33")))
   /*
    //5.注册驱动
    Class.forName(driver)
    //6.获取连接
    val conn: Connection = DriverManager.getConnection(url,username,passwd)
    //7.sql语句
    val sql :String="insert into users(username,password) values(?,?)"
    //8.获取数据库操作对象
    val ps: PreparedStatement = conn.prepareStatement(sql)
    //9.foreach算子 遍历rdd数据
    rdd.foreach{
      case(username,password)=>{
        //给参数赋值
        ps.setString(1,username)
        ps.setString(2,password)
        //执行sql语句
        ps.executeUpdate()
      }
    }
    //释放资源
    ps.close()
    conn.close()
    */
    rdd.foreachPartition{
      datas=>{
        //5.注册驱动
        Class.forName(driver)
        //6.获取连接
        val conn: Connection = DriverManager.getConnection(url,username,passwd)
        //7.sql语句
        val sql :String="insert into users(username,password) values(?,?)"
        //8.获取数据库操作对象
        val ps: PreparedStatement = conn.prepareStatement(sql)
        datas.foreach{
          case(username,password)=>{
            //给参数赋值
            ps.setString(1,username)
            ps.setString(2,password)
            //执行sql语句
            ps.executeUpdate()
          }
        }
        //释放资源
        ps.close()
        conn.close()
      }
    }
    //关闭连接
    sc.stop()
  }
}

