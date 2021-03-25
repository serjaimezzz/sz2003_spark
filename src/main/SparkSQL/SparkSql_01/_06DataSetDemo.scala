package SparkSql_01

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

/**
 * Dataset就是DataFrame的升级版
 *  创建方法:
 *      session.createDataset(List|Seq|Array)
 */
object _06DataSetDemo {
    def main(args: Array[String]): Unit = {
        val session: SparkSession = SparkSession.builder().master("local").appName("test").getOrCreate()//获取一个RDD对象
        import session.implicits._
        val girls = List(Girl(1,"aaa","f",21),Girl(2,"sss","f",24))
        val ds: Dataset[Girl] = session.createDataset[Girl](girls)
        ds.show()
        ds.printSchema()
        session.stop()
    }
}

//必须使用样例类
case class Girl(id:Int,name:String,gender:String,age:Int)
