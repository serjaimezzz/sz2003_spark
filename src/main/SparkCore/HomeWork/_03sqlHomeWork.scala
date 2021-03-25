package HomeWork

import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object _03sqlHomeWork {
    private val session: SparkSession = SparkSession.builder().master("local").appName("test").getOrCreate()
    import session.implicits._
    import org.apache.spark.sql.functions._
    private val df1: DataFrame = session.read.json("file:///D:\\JavaInThinking\\IDEA\\sz2003_spark\\input/table1.json")
    private val df2: DataFrame = session.read.json("file:///D:\\JavaInThinking\\IDEA\\sz2003_spark\\input/table2.json")
    private val df: DataFrame = session.read.option("seq",",").option("header",true).csv("file:///D:\\JavaInThinking\\IDEA\\sz2003_spark\\input/work.csv")

    def main(args: Array[String]): Unit = {
//        work1
        work2
        session.stop()
    }

    def work1: Unit ={
//        df1.show()
//        df2.show()

        df1.createTempView("tmp1")
        df2.createTempView("tmp2")

        session.sql(
            """
              |select t1.A,t1.B,t2.de as `D+E`
              | from tmp1 as t1
              |join
              |(
              |select C,(D+E) as de
              |from tmp2
              |)t2
              |on t1.C=t2.C
              |""".stripMargin).show()
    }

    def work2: Unit ={
        //uv:独立访客:查看商品的独立访客
//        df.show()
        df.createTempView("tmp")

        session.sql(
            """
              |select A.keyword,A.UV
              |from
              |(
              |select keyword,count(user)over(distribute by keyword)`UV`
              |from tmp
              |)A
              |group by A.keyword,A.UV
              |""".stripMargin
        ).show()
    }
}
