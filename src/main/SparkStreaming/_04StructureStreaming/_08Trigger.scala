package _04StructureStreaming

import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}
/**
 * trigger函数：
 *   sparkStreaming是一个准实时的计算框架，微批处理
 *   structuredStreaming是一个实时的计算框架，但是底层使用的sparksql的api，
 *   并且是sparkStreaming的进化版，比微批处理更快，也有微小的时间段，最快可以达到 `100ms` 左右的端到端延迟。
 *   而使用trigger函数可以做到1ms的端到端延迟。
 */
object _08Trigger {
    private val session: SparkSession = SparkSession.builder().master("local[*]").appName("test").getOrCreate()
    import session.implicits._

    def main(args: Array[String]): Unit = {
        session.sparkContext.setLogLevel("ERROR")
        val frame: DataFrame = session.readStream.format("kafka")
          .option("kafka.bootstrap.servers","master:9092,slave1:9092,slave2:9092")
          .option("subscribe","pet")
          .load()

        val frame1 = frame.selectExpr("cast(value as String)")

        frame1.writeStream
          .format("console")
          .trigger(Trigger.ProcessingTime(0))
          .start()
          .awaitTermination()
    }
}
