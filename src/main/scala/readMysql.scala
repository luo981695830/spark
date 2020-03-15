import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

/*
*  从mysql读取数据
* */
object readMysql {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3.定义连接mysql的参数
    val driver="com.mysql.jdbc.Driver"
    val url="jdbc:mysql://localhost:3306/test"
    val username="root"
    val passwd = "root"

    //4.创建jdbcRdd
    val sql:String = "select * from users where id>=? and id <= ?"
    //
    val resRdd: JdbcRDD[(Int, String, String)] = new JdbcRDD(
      sc,
      () => {
        //注册驱动
        Class.forName(driver)
        DriverManager.getConnection(url, username, passwd)
      },
      sql,
      1,//下限
      10,//上限
      2,//分区数
      rs => (rs.getInt(1), rs.getString(2), rs.getString(3))
    )
    resRdd.collect().foreach(println)
    //关闭连接
    sc.stop()
  }
}
