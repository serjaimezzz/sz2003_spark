package _02OxDemo

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object _05foreachDemo {
    private val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("zk")
    private val sc: StreamingContext = new StreamingContext(conf, Seconds(5))
    val prop: Properties = new Properties()
    prop.load(_05foreachDemo.getClass.getClassLoader.getResourceAsStream("consumer.properties"))

    def main(args: Array[String]): Unit = {
        val ds: ReceiverInputDStream[String] = sc.socketTextStream("master", 10087)

        /**
         * foreachRDD
         * 1.首先是一个输出算子
         * 2.在driver端执行
         * 3.将dStream处理的micro-batch数据转成RDD,就可以调用RDD的相关算子进行计算
         */
        ds.foreachRDD(rdd=>{
            rdd.map((_,1)).foreach(println)
        })

        sc.start()
        sc.awaitTermination()
    }
}
