package SparkSql_02

import SparkSql_02._04SparkInnerFuncDemo.session
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object _Test {
    import org.apache.spark.sql._

    def main(args: Array[String]): Unit = {
        demo1
    }

    def demo1: Unit ={
        val conf: SparkConf = new SparkConf().setMaster("local").setAppName("test")
        val sc: SparkContext = new SparkContext(conf)
        val hiveContext: HiveContext = new HiveContext(sc)
        val df: DataFrame = hiveContext.jsonFile("file:///D:\\JavaInThinking\\IDEA\\sz2003_spark\\input/emp.json")
        df.registerTempTable("tmp")
        val df1: DataFrame = hiveContext.sql("""select ename from tmp""")
        val value: Dataset[String] = df1.map(row => row.getString(0))(Encoders.STRING)
        value.show()
    }
}
