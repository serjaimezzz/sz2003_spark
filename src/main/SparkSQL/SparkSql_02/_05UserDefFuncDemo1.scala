package SparkSql_02

import SparkSql_02._04SparkInnerFuncDemo.{df, session}
import org.apache.spark.sql.{DataFrame, SparkSession}

object _05UserDefFuncDemo1 {
    private val session: SparkSession = SparkSession.builder().master("local").appName("test").getOrCreate()
    val df: DataFrame = session.read.json("file:///D:\\JavaInThinking\\IDEA\\sz2003_spark\\input/emp.json")

    def main(args: Array[String]): Unit = {
        myFuncTest
        session.stop()
    }

    def myFuncTest: Unit ={
        /**
         * 自定义函数
         */
        //定义一个函数：
        val getLength = (str:String)=>str.length
        //注册函数：
        session.udf.register("getLength",getLength)


        //测试：
        df.createTempView("tmp")
        val sql =
            """
              |select ename,
              |getLength(ename)
              |from
              |tmp
              |""".stripMargin
        session.sql(sql).show()
    }
}
