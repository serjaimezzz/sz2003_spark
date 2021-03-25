package _04StructureStreaming

import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{DataTypes, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object _01Read_HDFS {
    private val session: SparkSession = SparkSession.builder().master("local[*]").appName("test").getOrCreate()
    import session.implicits._
    import org.apache.spark.sql.functions._

    def main(args: Array[String]): Unit = {
        session.sparkContext.setLogLevel("ERROR")

        //为读取到的数据维护一个元数据schema
        val schema = new StructType()
          .add("item_id",DataTypes.IntegerType)
          .add("annotations",DataTypes.StringType)
          .add("img_name",DataTypes.StringType)

        val frame: DataFrame = session.readStream
          .schema(schema)   //设置表头的元数据
          .json("hdfs://master/SparkStreaming")

        frame.writeStream.outputMode(OutputMode.Update())
          .format("console")
          .start()
          .awaitTermination()
    }
}
