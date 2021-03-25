package WeekTest

import org.apache.spark.sql.{DataFrame, SparkSession}


object WeekTest {

    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder().appName("test").master("local").getOrCreate()
        import spark.implicits._
        val dataFrame: DataFrame = spark.read.text("input/51job.txt")
//        dataFrame.show()

        dataFrame.createOrReplaceTempView("tmp")
        val frame: DataFrame = spark.sql(
            """
              |select
              |split(value,"\t")[0] as job,
              |split(value,"\t")[1] as company,
              |split(value,"\t")[2] as location,
              |split(split(split(value,"\t")[3],"/")[0],"-")[1] as max_sal,
              |split(split(value,"\t")[3],"/")[1] as sal_mode,
              |split(value,"\t")[4] as date
              |from tmp
              |where split(value,"\t")[3]!=''
              |""".stripMargin)
//        frame.show()
        frame.createTempView("51job")
        val frame1: DataFrame = spark.sql(
            """
              |select t.B/t.A as ratio
              |from
              |(
              |select
              |(
              |select count(sal_mode) as num
              |from 51job
              |where sal_mode="年"
              |)B
              |,
              |(
              |select count(sal_mode) as sum
              |from 51job
              |)A
              |)t
              |""".stripMargin)
            frame1.show()

        spark.sql(
            """
              |
              |select max_sal,
              |regexp_extract( `max_sal`, '([0-9]*[\\.]*[0-9]+)(万)',1) as a
              |from 51job
              |""".stripMargin).show()




    }
}
