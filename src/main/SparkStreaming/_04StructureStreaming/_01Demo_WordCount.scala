package _04StructureStreaming

import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{Dataset, KeyValueGroupedDataset, SparkSession}

/**
 * wordcount
 * master：nc -lk port发送数据
 * 读取数据打印到console
 */
object _01Demo_WordCount {
    private val session: SparkSession = SparkSession.builder().master("local[*]").appName("test").getOrCreate()
    import session.implicits._
    private val ds: Dataset[String] = session.readStream
      .format("socket")
      .option("host", "master")
      .option("port", "10000")
      .load().as[String]


    def main(args: Array[String]): Unit = {
        val value: KeyValueGroupedDataset[String, (String, Int)] = ds.flatMap(_.split(" ")).map((_, 1)).groupByKey(_._1)
        val result: Dataset[(String, Long)] = value.count()

        /**
         * StructedStreaming流资源的查询必须执行writeStream.start()
         * OutputMode.Complete():全局的数据流进行汇总，此模式一定要在聚合时才能应用
         * OutputMode.APPEND()：只会将新数据追加到接收器中，不能用于带有聚合的查询
         * OutputMode.UPDATE()：只会将更新的数据添加到接收器中，如果没有聚合操作，相当于APPEND
         */
        result.writeStream
          .outputMode(OutputMode.Complete)
          .format("console")
          .start()
          .awaitTermination()//防止没有数据时停止程序
    }
}
