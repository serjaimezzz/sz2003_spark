package SparkSql_02

import org.apache.spark.sql.{DataFrame, SparkSession}

object _07WindowFuncDemo {
    private val session: SparkSession = SparkSession.builder().master("local").appName("test").getOrCreate()
    import session.implicits._
    import org.apache.spark.sql.functions._
    val df: DataFrame = session.read.json("file:///D:\\JavaInThinking\\IDEA\\sz2003_spark\\input/emp.json")

    def main(args: Array[String]): Unit = {
        windowFuncTest
//        timeComponent
    }

    def windowFuncTest: Unit ={
        df.createTempView("tmp")

        val dfsql: DataFrame = session.sql(
            """
              |select distinct deptno,
              |count(1) over(partition by deptno sort by deptno)as count
              |from tmp
              |""".stripMargin
        )
        dfsql.show()
        session.stop()
    }

    def timeComponent: Unit ={
        val df1: DataFrame = df.select(
            year(to_date($"hiredate", "yyyy-MM-dd")).as("year"),
            month(to_date($"hiredate", "yyyy-MM-dd")).as("month"),
            dayofmonth(to_date($"hiredate", "yyyy-MM-dd")).as("day"))
                df1.show()
    }
}
