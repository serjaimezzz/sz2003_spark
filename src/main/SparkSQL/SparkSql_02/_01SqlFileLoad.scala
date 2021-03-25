package SparkSql_02

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

object _01SqlFileLoad {
    private val session: SparkSession = SparkSession.builder().master("local").appName("test").getOrCreate()
    def main(args: Array[String]): Unit = {
        /**
         *  正规加载文件的方式是load方法，load方式默认加载的是parquet格式的文件
         *  如果想要读取其他格式的文件需要.format()指定读取文件的格式
         */
//        val df: DataFrame = session.read.load("input/users.parquet")
//        jsonRead
//        csvRead
        jdbcRead

        /**
         * 也可以使用简化的方式来读取
         * session.read.json()
         * session.read.csv()
         * ...
         */
    }

    def jsonRead: Unit ={
        val df: DataFrame = session.read.format("json").load("file:///D:\\JavaInThinking\\IDEA\\sz2003_spark\\input/emp.json")
        df.show()
    }

    def csvRead: Unit ={
        /**
         * 加载csv文件时，默认使用逗号作为分隔符切分字段
         * 可以在.format()之后使用.option()指定分隔符
         * 可以.option()将第一行作为表头读取
         */
        val df1: DataFrame = session.read.format("csv")
          .option("sep", ";")
          .option("header", true)
          .load("input/ip-pprovince-count.csv")
        df1.show()
        df1.select("counts").filter("province='北京'  ").show()
    }

    def jdbcRead: Unit ={
        val properties = new Properties()
        properties.setProperty("user","root")
        properties.setProperty("password","123456")

        val df: DataFrame = session.read.jdbc("jdbc:mysql://localhost:3306/sz2003_db", "score", properties)
        df.show()
    }
}
