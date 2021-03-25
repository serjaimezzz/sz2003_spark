package _04StructureStreaming

import org.apache.spark.sql.{DataFrame, SparkSession}

object _04Sink_HDFS {
    private val session: SparkSession = SparkSession.builder().master("local[*]").appName("test").getOrCreate()
    import session.implicits._
    import org.apache.spark.sql.functions._

    def main(args: Array[String]): Unit = {
        val frame: DataFrame = session.readStream.format("kafka")
          .option("kafka.bootstrap.servers","master:9092,slave1:9092,slave2:9092")
          .option("subscribe","pet")
          .load()

        val frame1 = frame.selectExpr("cast(value as string)").select("value").as[String]

        session.sparkContext.setLogLevel("ERROR")

        frame1.writeStream.format("text")
//          .option("path","output")//存储路径:本地
          .option("path","hdfs://master/StreamingSink")
          .option("checkpointLocation","checkpoint")
          .start()
          .awaitTermination()
    }
}
