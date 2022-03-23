package cn.xiaoguangbiao.core

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author xiaoguangbiao
 * Desc 演示RDD的外部数据源
 */
object RDDDemo13_DataSource2{
  def main(args: Array[String]): Unit = {
    //TODO 0.env/创建环境
    val conf: SparkConf = new SparkConf().setAppName("spark").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")

    //TODO 1.source/加载数据/创建RDD
    //RDD[(姓名, 年龄)]
    val dataRDD: RDD[(String, Int)] = sc.makeRDD(List(("jack", 18), ("tom", 19), ("rose", 20)))

    //TODO 2.transformation
    //TODO 3.sink/输出
    //需求:将数据写入到MySQL,再从MySQL读出来
    /*
CREATE TABLE `t_student` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) DEFAULT NULL,
  `age` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
     */

    //写到MySQL
    //dataRDD.foreach()
    dataRDD.foreachPartition(iter=>{
      //开启连接--有几个分区就开启几次
      //加载驱动
      //Class.forName("com.mysql.jdbc.Driver")
      val conn: Connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata?characterEncoding=UTF-8","root","root")
      val sql:String = "INSERT INTO `t_student` (`id`, `name`, `age`) VALUES (NULL, ?, ?);"
      val ps: PreparedStatement = conn.prepareStatement(sql)
      iter.foreach(t=>{//t就表示每一条数据
        val name: String = t._1
        val age: Int = t._2
        ps.setString(1,name)
        ps.setInt(2,age)
        ps.addBatch()
        //ps.executeUpdate()
      })
      ps.executeBatch()
      //关闭连接
      if (conn != null) conn.close()
      if (ps != null) ps.close()
    })

    //从MySQL读取
    /*
    sc: SparkContext,
    getConnection: () => Connection, //获取连接对象的函数
    sql: String,//要执行的sql语句
    lowerBound: Long,//sql语句中的下界
    upperBound: Long,//sql语句中的上界
    numPartitions: Int,//分区数
    mapRow: (ResultSet) => T = JdbcRDD.resultSetToObjectArray _) //结果集处理函数
     */
    val  getConnection = () => DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata?characterEncoding=UTF-8","root","root")
    val sql:String = "select id,name,age from t_student where id >= ? and id <= ?"
    val mapRow: ResultSet => (Int, String, Int) = (r:ResultSet) =>{
      val id: Int = r.getInt("id")
      val name: String = r.getString("name")
      val age: Int = r.getInt("age")
      (id,name,age)
    }
    val studentTupleRDD: JdbcRDD[(Int, String, Int)] = new JdbcRDD[(Int,String,Int)](
      sc,
      getConnection,
      sql,
      4,
      6,
      1,
      mapRow
    )
    studentTupleRDD.foreach(println)
  }
}
