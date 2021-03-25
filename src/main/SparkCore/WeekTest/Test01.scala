package WeekTest

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object Test01 {
    val conf = new SparkConf().setMaster("local").setAppName("test")
    val context = new SparkContext(conf)

    def main(args: Array[String]): Unit = {
        test1
    }

    def test1: Unit ={
        val session: SparkSession = SparkSession.builder().master("local").appName("test").getOrCreate()
        val dataFrame: DataFrame = session.read.json("file:///D:\\JavaInThinking\\IDEA\\sz2003_spark\\input/exam01.json")
        import session.implicits._
        import org.apache.spark.sql.functions._
//        dataFrame.show()

        dataFrame.createTempView("tmp")

        val dfsql1 = session.sql(
            """
              |select terminal,count(terminal)`count`
              |from tmp
              |where status=1
              |group by terminal
              |""".stripMargin)
//        dfsql1.show()
//        var a = 0
//        val frame: DataFrame = dataFrame.select($"terminal" as ("this"), lead("terminal", 1) over (Window.partitionBy("phoneNum").orderBy("date")) as ("next"))
//        frame.show()

        val dfsql2 = session.sql(
            """
              |
              |select province,count(status)`log_times`
              |from tmp
              |where status=1
              |group by province
              |order by log_times desc
              |limit 2
              |""".stripMargin)
//        dfsql2.show()

        val prop = new Properties()
        prop.setProperty("user","root")
        prop.setProperty("password","123456")
        dfsql1.write.mode(SaveMode.Overwrite).jdbc("localhost","exam01_1",prop)
        dfsql2.write.mode(SaveMode.Overwrite).jdbc("localhost","exam01_2",prop)
        session.stop()
    }
}
