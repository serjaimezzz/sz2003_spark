package SparkSql_01

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object _08DF2Others {
    val session: SparkSession = SparkSession.builder().master("local").appName("test").getOrCreate()
    val dataFrame: DataFrame = session.read.json("file:///D:\\JavaInThinking\\IDEA\\sz2003_spark\\input/emp.json")

    def main(args: Array[String]): Unit = {
        DF_RDD
    }

    def DF_RDD: Unit ={

        val rdd1: RDD[Row] = dataFrame.rdd
        rdd1.foreach(println)

        for(i <- rdd1){
            val l: Long = i.getLong(1)
            println(l)
        }
    }

    def DF_Ds: Unit ={
        /**
         * Dataset的实质就是升级版的DataFrame
         * 可以调用其他算子转换成 ds
         * 如:as、checkPoint、filter、map、coalesce、select、where、orderBy...
         * 无 toDs()方法
         */
        dataFrame.as("ds")
    }
}
