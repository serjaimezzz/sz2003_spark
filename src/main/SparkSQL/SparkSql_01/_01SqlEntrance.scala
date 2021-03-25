package SparkSql_01

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * spark2.0以前，有两个，分别是sqlContext和hiveContext
 */
object _01SqlEntrance {
    val conf = new SparkConf().setMaster("local").setAppName("test")
    val sc = new SparkContext(conf)
    def main(args: Array[String]): Unit = {
//        sqlContextTest
//        hiveContextTest
        sparkSessionTest
        sc.stop()
    }

    //第一种：获取一个sqlContext
    def sqlContextTest: Unit ={
        val sqlContext: SQLContext = new SQLContext(sc)
        val dataFrame: DataFrame = sqlContext.read.json("file:///D:\\JavaInThinking\\IDEA\\sz2003_spark\\input/emp.json")
        dataFrame.show(false)
    }

    //第二种：获取一个hiveContext
    def hiveContextTest: Unit ={
        val hiveContext: HiveContext = new HiveContext(sc)
        val dataFrame: DataFrame = hiveContext.table("sz2003.course")
        val frame: DataFrame = hiveContext.sql("select * from sz2003.course")
        dataFrame.show()
//        frame.show()

    }
    //第三种：使用spark2.0以后的SparkSession对象
    def sparkSessionTest: Unit ={
        val session: SparkSession = SparkSession.builder().config(conf).getOrCreate()
        val dataFrame: DataFrame = session.read.json("file:///D:\\JavaInThinking\\IDEA\\sz2003_spark\\input/emp.json")
        dataFrame.show()
    }
}
