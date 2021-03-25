package _04StructureStreaming

import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 从kafka上拿到数据进行处理后，再上传到kafka
 */
object _05Sink_Kafka {
    private val session: SparkSession = SparkSession.builder().master("local[*]").appName("test").getOrCreate()
    import session.implicits._
    import org.apache.spark.sql.functions._

    def main(args: Array[String]): Unit = {
        val frame: DataFrame = session.readStream.format("kafka")
          .option("kafka.bootstrap.servers","master:9092,slave1:9092,slave2:9092")
          .option("subscribe","pet")
          .load()

        val frame1 = frame.selectExpr("cast(value as string)").select("value").as[String]
        val frame2 = frame1.map(
            x=>{
                val arr: Array[String] = x.split("::")
                (arr(0).toInt,arr(1),arr(2))
            })
          .filter(_._3.contains("Comedy"))
          .as[(Int,String,String)]
          .toDF("id","name","info")
          .map(row=>{
              ""+row.getAs("id")+row.getAs("name")+row.getAs("info")
          })

        frame2.writeStream
//            .format("console")
          .format("kafka")
          .outputMode(OutputMode.Append())
          .option("kafka.bootstrap.servers","master:9092,slave1:9092,slave2:9092")
          .option("topic","streaming")
          .option("checkpointLocation","checkpoint")
          .start()
          .awaitTermination()
    }
}
