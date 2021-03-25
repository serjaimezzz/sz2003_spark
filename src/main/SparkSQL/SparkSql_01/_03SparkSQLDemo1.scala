package SparkSql_01

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

object _03SparkSQLDemo1 {
    val session: SparkSession = SparkSession.builder().appName("test").master("local").getOrCreate()
    /**
     * 如果想要使用$,需要导入隐式转换
     * import session(Session对象名).implicits._
     * 建议：不管是否使用隐式转换，都在声明完SparkSession后导入此包
     */

    import session.implicits._

    //读取
    val dataFrame: DataFrame = session.read.json("file:///D:\\JavaInThinking\\IDEA\\sz2003_spark\\input/emp.json")


    def main(args: Array[String]): Unit = {
        //        dataFrame.show()
        //        selectTest
//        whereTest
        colTest
    }

    def selectTest: Unit = {
        /**
         * select :可以指定字段名称
         */
        val dataFrame1: DataFrame = dataFrame.select("empno", "ename", "job")
        dataFrame1.show()
        //        dataFrame.select(new Column("empno"),new Column("ename"),new Column("job")).show()
        //        dataFrame.select($"empno").show()
    }

    def whereTest: Unit = {
        /**
         * where算子:
         */
        dataFrame.select("empno", "ename", "job", "sal").where('sal > 1500).show()
    }

    def colTest {
        /**
         * 字段可以进行计算
         */
        dataFrame.select($"empno", $"ename", ($"sal"+1000)).show()
    }
}
