package _02OxDemo

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 测试：
 * 使用nc -lk port 发送数据
 * main方法读取数据并进行处理
 */
object _04OperatorDemo {
    private val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("zk")
    private val sc: StreamingContext = new StreamingContext(conf, Seconds(10))
    val prop: Properties = new Properties()
    prop.load(_04OperatorDemo.getClass.getClassLoader.getResourceAsStream("consumer.properties"))
    val ds: ReceiverInputDStream[String] = sc.socketTextStream("master", 10087)

    def main(args: Array[String]): Unit = {
//        mapTest
//        wordCountTest
        joinTest

        sc.start()
        sc.awaitTermination()
    }

    def mapTest: Unit ={
        ds.map((_,1)).print()
    }

    def wordCountTest: Unit ={
        //只对同一批次(微批次)的数据进行累加
        ds.flatMap(_.split(" ")).filter(_.length>3).map((_,1)).reduceByKey(_ + _).print(100)
    }

    def joinTest: Unit ={
        val d1: DStream[(String, Int)] = ds.map((_, 1))
        val d2: DStream[(String, Int)] = ds.map((_, 1)).reduceByKey(_ + _)
        d1.join(d2).print()
    }
}
