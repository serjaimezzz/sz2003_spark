package _03Others

import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

object _JDBCrddTest {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[2]").setAppName("test")
        val context = new SparkContext(conf)

        def main(args: Array[String]): Unit = {
            val sql = "select * from emp whter deptno >? and deptno<?"
            //jdbc连接驱动设置
            val jdbcurl = "jdbc:mysql://localhost/sz2003?useUnicode=true&characterEncoding=utf8"
            val user = "root"
            var pwd = "123456"

            //获取连接
            val conn = () => {
                Class.forName("com.mysql.jdbc.Driver").newInstance()
                DriverManager.getConnection(jdbcurl, user, pwd)
            }

            val showInfo = new JdbcRDD(context, conn, sql, 0, 10000, 1,
                res => {
                    val empno = res.getInt("empno")
                    val ename = res.getString("ename")
                    val mgr = res.getInt("mgr")
                    val hiredate = res.getDate("hiredate")
                    (empno, ename, mgr, hiredate)
                })

            showInfo.foreach(println)
            context.stop()
        }
    }
}
