package SparkSql_01

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * 生成DataFrame的三种方式：
 * 1.session.read.json
 * 2.javaBean+反射：session.createDataFrame(students,classOf[Student])
 * 3.动态编程：session.createDataFrame(Rowdata,schema)
 */
object _05DataFrameDemo1 {
    def main(args: Array[String]): Unit = {
//        javaBeanTest
        structTest
    }

    /**
     * 方法2：使用JavaBean+反射机制,获取DataFrame对象
     */
    def javaBeanTest: Unit = {
        val session: SparkSession = SparkSession.builder().master("local").appName("test").getOrCreate()
        import session.implicits._

        //描述一个学生集合对象
        val students: List[Student] = List(
            new Student(1,"zhangsan","m",23),
            new Student(2,"lisi","m",24),
            new Student(3,"wangwu","m",24),
            new Student(4,"zhaoyun","m",25))

        import scala.collection.JavaConversions._
        //调用createDataFrame方法
        val dataFrame: DataFrame = session.createDataFrame(students, classOf[Student])
        dataFrame.show()
        session.stop()
    }

    /**
     *  方法3：使用动态编程
     *  Row:代表的是二维表中的一行记录，相当于一个Java对象
     *  StructType:是该二维表的元数据信息；是structuralField的集合
     *  StructField:是该二维表中某一个字段的元数据信息
     */
    def structTest: Unit ={
        val session: SparkSession = SparkSession.builder().master("local").appName("test").getOrCreate()
        //获取一个RDD对象
        val datas: RDD[Row] = session.sparkContext.parallelize(List(Row(1, "zed", "m",23), Row(2, "ali", "f",24)))
        val schema = StructType( List(
            StructField("id",DataTypes.IntegerType,false),
            StructField("name",DataTypes.StringType,false),
            StructField("gender",DataTypes.StringType,false),
            StructField("age",DataTypes.IntegerType,false)))
        val dataFrame: DataFrame = session.createDataFrame(datas, schema)
        dataFrame.show()
    }
}
