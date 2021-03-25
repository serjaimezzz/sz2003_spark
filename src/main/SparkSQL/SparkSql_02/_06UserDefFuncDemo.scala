package SparkSql_02

import org.apache.spark.sql.{DataFrame, SparkSession}

object _06UserDefFuncDemo {
    private val session: SparkSession = SparkSession.builder().master("local").appName("test").getOrCreate()
    val df: DataFrame = session.read.json("file:///D:\\JavaInThinking\\IDEA\\sz2003_spark\\input/emp.json")

    import org.apache.spark.sql.functions._
    import session.implicits._
    def main(args: Array[String]): Unit = {
        sqltest1
        session.udf.register("getLevel",getLevel _)
        session.stop()
    }

    def sqltest1: Unit ={
        df.createTempView("tmp")
        val sql =
            """
              |select ename,job,sal
              |case when
              |sal>3000 then 'A'
              |sal>1500 then 'B'
              |else 'C' end as level
              |from tmp
              |""".stripMargin

        session.sql(sql).show()
    }

    def getLevel(sal:Int): Unit ={
        if(sal>3000){"A"}
        else if(sal > 1500){"B"}
        else{"C"}
    }
}
