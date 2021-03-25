package _01ReadFile

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Durations, StreamingContext}

/**
 * SparkStreaming是一个准实时计算框架
 * 使用其接受nc(netcat)发送过来的数据，然后进行单词统计
 */
object _01SparkStreamingDemo {
    /**
     * 获取配置对象：
     * 本地模式至少需要两个线程。一个负责接收，一个负责计算。否则拿不到数据。
     */
    private val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("test")
    private val sc: StreamingContext = new StreamingContext(conf, Durations.seconds(10))

    def main(args: Array[String]): Unit = {
        //读取
        val ds: ReceiverInputDStream[String] = sc.socketTextStream("master", 7777)
        val value: DStream[(String, Int)] = ds.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
        value.print()

        //启动计算程序
        sc.start()
        //等待终止
        sc.awaitTermination()
    }
    //nc -lk master 7777
}
