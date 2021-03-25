package _04StructureStreaming

import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, SparkSession}

object _02Read_Kafka {
    private val session: SparkSession = SparkSession.builder().master("local[*]").appName("test").getOrCreate()
    import session.implicits._
    session.sparkContext.setLogLevel("ERROR")

    def main(args: Array[String]): Unit = {
        val frame: DataFrame = session.readStream.format("kafka")
          .option("kafka.bootstrap.servers","master:9092,slave1:9092,slave2:9092")
          .option("startingOffsets","latest")
          .option("subscribe","pet")
          .load()

        val df: DataFrame = frame.selectExpr("cast(value as string)","cast(partition as string)",
            "cast(offset as string)")
            .select($"value",$"partition",$"offset")

        df.writeStream.outputMode(OutputMode.Update())
          .format("console")
          .start()
          .awaitTermination()

    }
}
