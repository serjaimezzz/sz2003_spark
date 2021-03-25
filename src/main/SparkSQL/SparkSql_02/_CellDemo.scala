package SparkSql_02

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}

object _CellDemo {
    private val session: SparkSession = SparkSession.builder().master("local").appName("test").getOrCreate()
    import session.implicits._
    import org.apache.spark.sql.functions._
    def main(args: Array[String]): Unit = {
//        work1_sql
//        work1_df
//        work2_sql
//        work2_df
        work3
    }

    def work1_sql: Unit ={
        val df: DataFrame = session.read.option("sep", "\t").option("header", true).csv("file:///D:\\JavaInThinking\\IDEA\\sz2003_spark\\input\\product.csv")
        df.createTempView("product")
        val frame = session.sql(
            """
              |select *,sum(duration)over(partition by product_code sort by event_date asc
              |ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)as sumDuration
              |from product
              |""".stripMargin)
        frame.show()
    }

    /**
     * ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
     * rows between unbounded preceding and current row
     * 就是取从最开始到当前这一条数据，row_number()这个函数就是这样取的
     *
     * ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING
     * rows between 2 preceding and 2 following
     * 代表取前面两条和后面两条数据参与计算，比如计算前后五天内的移动平均就可以这样算.
     *
     * rowsBetween(-3,5)
     * 累计从计算当天，前3天到后5天的数据
     */
    def work1_df: Unit ={
        val df: DataFrame = session.read.option("sep", "\t").option("header", true).csv("file:///D:\\JavaInThinking\\IDEA\\sz2003_spark\\input\\product.csv")
        val frame: DataFrame = df.select($"product_code", $"event_date", $"duration",
            sum($"duration") over (Window.partitionBy("product_code").orderBy("event_date")).rowsBetween(Long.MinValue,0)as("sumDuration"))
        frame.show()
    }

    def work2_sql: Unit ={
        val df: DataFrame = session.read.option("sep", "\t").option("header", true).csv("file:///D:\\JavaInThinking\\IDEA\\sz2003_spark\\input\\product.csv")
        df.createTempView("product")

        /**
         * 分组汇总sumDuration再对所有的sumDuration求和
         */
        session.sql(
            """
              |select product_code,event_date,sum(duration) as sumDuration
              |from product
              |group by product_code,event_date with rollup
              |order by product_code,event_date
              |""".stripMargin).show()
    }

    def work2_df: Unit ={
        val df: DataFrame = session.read.option("sep", "\t").option("header", true).csv("file:///D:\\JavaInThinking\\IDEA\\sz2003_spark\\input\\product.csv")
        df.rollup($"product_code",$"event_date")    //使用指定的列为当前数据集创建一个多维汇总，以便我们对其进行聚合
          .agg(sum($"duration"))    //通过指定一系列聚合列来计算聚合。
          .orderBy($"product_code",$"event_date")
          .show()
    }

    def work3: Unit ={
        val df: DataFrame = session.read.option("sep", "\t").option("header", true).csv("file:///D:\\JavaInThinking\\IDEA\\sz2003_spark\\input\\product.csv")
        df.createTempView("product")
        val tmpSql = session.sql(
            """
              |select product_code,event_date,sum(duration)as sumDuration
              |from product
              |group by product_code,event_date
              |""".stripMargin)
        tmpSql.show()


        val frame: DataFrame = tmpSql.groupBy($"product_code").pivot("event_date").sum("sumDuration").na.fill(0)
        frame.show()
    }
}
