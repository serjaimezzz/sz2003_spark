package SparkSql_02

import java.util.Properties

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object _02SqlFileSave {
    private val session: SparkSession = SparkSession.builder().master("local").appName("test").getOrCreate()

    def main(args: Array[String]): Unit = {
//        saveAsCsvFileTest
//        saveAsTextTest
        saveToMySql
    }

    def saveAsCsvFileTest: Unit ={
        val df: DataFrame = session.read.json("input/emp.json")

        /**
         * 调用DataFrameWriter的save方法:    保存路径不能提前存在
         * 默认保存格式为parquet
         * 可使用.format()指定保存为其他格式
         * 可以将表头写在第一行
         */
        df.write.format("csv")
          .option("header",true)
          .save("json_saveAs_csv")
        session.stop()
    }

    def saveAsTextTest: Unit ={
        val arr = Array("zhangsan","lisi","wangwu")
        val rdd: RDD[String] = session.sparkContext.parallelize(arr)
        import session.implicits._
        val df: DataFrame = rdd.toDF("name")
        df.write.format("text")
          .save("saveAsTextTest")
    }

    def saveToMySql: Unit ={
        val df: DataFrame = session.read.json("input/emp.json")

        val prop = new Properties()
        prop.setProperty("user","root")
        prop.setProperty("password","123456")

        /**
         * 可使用.mode()选择模式
         * SaveMode.Append      在原有的基础之上追加
         * SaveMode.Overwrite   覆盖(删除并重建)
         * SaveMode.ErrorIfExists   目录存在保存，默认的格式
         * SaveMode.Ignore  忽略，如果目录存在则忽略，不存在则创建
         */
        df.write.mode(SaveMode.Ignore).jdbc("jdbc:mysql://localhost:3306/sz2003_db","sparkTest",prop)
    }
}
