package _03Window

import java.util.Properties

import _02OxDemo._08UpdateStateByKey.conf
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.concurrent.duration.Duration

object _01WindowDemo1 {
    private val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("zk")
    // Duration是最小时间单元
    val duration = Seconds(5)
    private val ssc: StreamingContext = new StreamingContext(conf, duration)
    val prop: Properties = new Properties()
    prop.load(_01WindowDemo1.getClass.getClassLoader.getResourceAsStream("consumer.properties"))

    def main(args: Array[String]): Unit = {
        val ds: ReceiverInputDStream[String] = ssc.socketTextStream("master", 10000)
        /**
         *  window(windowDuration: Duration, slideDuration: Duration)
         *          窗口宽度（必须是此DStream的批处理间隔的倍数），
         *     slide滑动频率（必须是此DStream的批处理间隔的倍数）;默认值为配置中预设的duration
         */
        val value: DStream[(String, Int)] = ds.map((_, 1)).window(duration * 3,duration)
        value.reduceByKey(_ + _).print()

        ssc.start()
        ssc.awaitTermination()
    }
}
