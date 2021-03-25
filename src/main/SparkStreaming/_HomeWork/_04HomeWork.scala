package _HomeWork

import java.util.Properties

import _03Window._02CountByWinDemo
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object _04HomeWork{
    private val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("zk")
    // Duration是最小时间单元
    val duration = Seconds(10)
    val sc: StreamingContext = new StreamingContext(conf, duration)
    sc.checkpoint("file:///D:\\JavaInThinking\\IDEA\\sz2003_spark\\streamingCache")
    val prop: Properties = new Properties()
    prop.load(_02CountByWinDemo.getClass.getClassLoader.getResourceAsStream("consumer.properties"))

    def main(args: Array[String]): Unit = {

    }
}
