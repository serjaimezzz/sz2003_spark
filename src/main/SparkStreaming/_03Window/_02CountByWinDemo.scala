package _03Window

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
/**
 * 计算最近30秒的热搜词的top3
 *     java
 *     c++
 *     bigdata
 *     python
 *     .....
 */
object _02CountByWinDemo {
    private val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("zk")
    // Duration是最小时间单元
    val duration = Seconds(10)
    val sc: StreamingContext = new StreamingContext(conf, duration)
    sc.checkpoint("file:///D:\\JavaInThinking\\IDEA\\sz2003_spark\\streamingCache")
    val prop: Properties = new Properties()
    prop.load(_02CountByWinDemo.getClass.getClassLoader.getResourceAsStream("consumer.properties"))
    val ds: ReceiverInputDStream[String] = sc.socketTextStream("master", 10000)

    def main(args: Array[String]): Unit = {
        ds.map((_, 1))
          .reduceByKeyAndWindow( (x: Int, y: Int) => {x + y}, duration * 3, Seconds(10) )
          .transform(rdd => {
              val tuples= rdd.sortBy(_._2, false).take(3)
              sc.sparkContext.makeRDD(tuples)
          })
          .print()

        sc.start()
        sc.awaitTermination()
    }
}
