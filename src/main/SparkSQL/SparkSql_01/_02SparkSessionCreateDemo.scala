package SparkSql_01

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object _02SparkSessionCreateDemo {
    def main(args: Array[String]): Unit = {
//        a
        b
//        c
    }

    //第一种：使用SparkSession的builder构建器,多参数时可连续调用.config
    def a: Unit = {
        val session: SparkSession = SparkSession.builder()
          .master("local")
          .appName("test")
          .getOrCreate()
        val dataFrame: DataFrame = session.read.json("file:///D:\\JavaInThinking\\IDEA\\sz2003_spark\\input/emp.json")
        /**
         * show():This is an ActionOperator.
         * show():列出前20条记录
         * true，则超过20个字符的字符串将被截断，并且所有单元格都将右对齐;
         * false:不截断，左对齐
         */
        dataFrame.show()
        dataFrame.printSchema()
        session.stop()
    }

    //第二种：使用SparkConf绑定配置信息
    def b: Unit = {
        val conf: SparkConf = new SparkConf()
          .setMaster("local")
          .setAppName("test")
          .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

        val session: SparkSession = SparkSession.builder().config(conf).getOrCreate()
        val dframe: DataFrame = session.read.json("file:///D:\\JavaInThinking\\IDEA\\sz2003_spark\\input/emp.json")
        dframe.show()
        dframe.schema.printTreeString()
        session.stop()
    }

    //第三种：获取连接hive的SparkSession对象
    def c: Unit = {
        val session: SparkSession = SparkSession.builder()
          .master("local")
          .appName("test")
          .enableHiveSupport()
          .getOrCreate()

        session.table("sz2003.course").show()
        session.stop()
    }
}
