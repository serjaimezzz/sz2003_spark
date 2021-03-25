package _03Window

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

object _03WindowDemo {
    private val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("zk")
    private val sc: StreamingContext = new StreamingContext(conf, Seconds(10))
    sc.checkpoint("file:///D:\\JavaInThinking\\IDEA\\sz2003_spark\\streamingCache")
    val prop: Properties = new Properties()
    prop.load(_03WindowDemo.getClass.getClassLoader.getResourceAsStream("consumer.properties"))
    val ds: ReceiverInputDStream[String] = sc.socketTextStream("master", 10000)

    def main(args: Array[String]): Unit = {
//        val1    //计算滑动窗口内的记录总条数
//        val2    //根据value来统计（k，v）相同的数量
        val3    //窗口内的数据进行聚合。
        sc.start()
        sc.awaitTermination()
    }
    def val1: Unit ={
        ds.countByWindow(Seconds(30), Seconds(10))
          .print()
    }

    def val2: Unit ={
        //（k，v（随机数））
        ds.map((_, (Math.random() * 10).toInt))
          .countByValueAndWindow(Seconds(30), Seconds(10))
          .print()
    }

    def val3: Unit ={
        ds.reduceByWindow(_ + _, Seconds(30), Seconds(10))//字符串拼接
        .print()
    }
}
