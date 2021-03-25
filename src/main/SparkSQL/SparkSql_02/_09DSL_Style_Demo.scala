package SparkSql_02

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object _09DSL_Style_Demo {
    private val session: SparkSession = SparkSession.builder().master("local").appName("test").getOrCreate()
    import session.implicits._
    import org.apache.spark.sql.functions._
    val df: DataFrame = session.read.json("file:///D:\\JavaInThinking\\IDEA\\sz2003_spark\\input/emp.json")

    def main(args: Array[String]): Unit = {
//        println(df.count())
        //
//        df.union(df).show()

//        df.toJSON.show(false)

//        val rows: Array[Row] = df.head(3)
//        rows.foreach(println)

        val value: Dataset[(String, Double)] = df.map(row => {
            val name = row.getAs[String]("ename")
            val sal: Double = row.getAs[Double]("sal")
            (name, sal)
        })
        value.show()
    }
}
