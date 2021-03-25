package SparkSql_01

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object _09Ds2others {
    def main(args: Array[String]): Unit = {
//        Ds_RDD
        Ds_DF
    }

    def Ds_RDD: Unit ={
        val session: SparkSession = SparkSession.builder().master("local").appName("test").getOrCreate()
        import session.implicits._
        val girls = List(Girl(1,"aaa","f",21),Girl(2,"sss","f",24))
        val datas: Dataset[Girl] = session.createDataset[Girl](girls)
//        datas.show()

        val rdd1: RDD[Girl] = datas.rdd
        rdd1.foreach(println)

        session.stop()
    }

    def Ds_DF: Unit ={
        val session: SparkSession = SparkSession.builder().master("local").appName("test").getOrCreate()
        import session.implicits._
        val girls = List(Girl(1,"aaa","f",21),Girl(2,"sss","f",24))
        val datas: Dataset[Girl] = session.createDataset[Girl](girls)
        //        datas.show()

        val df: DataFrame = datas.toDF()
        df.show()

        session.stop()
    }
}
