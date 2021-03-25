package SparkSql_01

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

object _07RDD2Others {
    val session: SparkSession = SparkSession.builder().master("local").appName("test").getOrCreate()
    import session.implicits._

    def main(args: Array[String]): Unit = {
//        RDD_DF
        RDD_Ds
    }

    def RDD_DF: Unit ={

        //获取一个RDD对象
        val datas: RDD[Row] = session.sparkContext.parallelize(List(Row(1, "zed", "m",23), Row(2, "ali", "f",24)))
        val schema = StructType( List(
            StructField("id",DataTypes.IntegerType,false),
            StructField("name",DataTypes.StringType,false),
            StructField("gender",DataTypes.StringType,false),
            StructField("age",DataTypes.IntegerType,false)  ))

        /**
         *  两种方法:
         *  1.session.createFrame(rdd,schema)
         *  2.rdd.toDF()
         */

        val dataFrame: DataFrame = session.createDataFrame(datas, schema)
        dataFrame.show()

        val df: DataFrame = dataFrame.toDF()
        df.show()

    }

    def RDD_Ds: Unit ={
        val datas: RDD[Cat] = session.sparkContext.parallelize(List(
            Cat(1, "sasa", 1),
            Cat(2, "dd", 2),
            Cat(3, "xx", 4),
            Cat(4, "pp", 3)
        ))
        /**
         *  两种方法:
         *  1.session.createDataset(rdd)
         *  2.rdd.toDS()
         */
        val ds1: Dataset[Cat] = session.createDataset(datas)
        ds1.show()

        val ds2: Dataset[Cat] = datas.toDS()
        ds2.show()

        session.stop()
    }

}

case class Cat(id:Int,name:String,age:Int)