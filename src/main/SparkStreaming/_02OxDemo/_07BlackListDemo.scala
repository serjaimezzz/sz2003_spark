package _02OxDemo

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 过滤黑名单
 */
object _07BlackListDemo {
    private val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("zk")
    private val sc: StreamingContext = new StreamingContext(conf, Seconds(5))
    val prop: Properties = new Properties()
    prop.load(_07BlackListDemo.getClass.getClassLoader.getResourceAsStream("consumer.properties"))

    def main(args: Array[String]): Unit = {
        //黑名单
        val list: List[(String, Boolean)] = List(("aaa", true), ("bbb", true))
        val blackList: RDD[(String, Boolean)] = sc.sparkContext.makeRDD(list)

        //读取黑名单
        val ds: ReceiverInputDStream[String] = sc.socketTextStream("master", 10087)
        val ds1: DStream[(String, (Int, Option[Boolean]))] = ds.map((_, 1)).transform((x, time) => {
            val rdd1: RDD[(String, (Int, Option[Boolean]))] = x.leftOuterJoin(blackList)
            val rdd2: RDD[(String, (Int, Option[Boolean]))] = rdd1.filter(x => {
                if (x._2._2.getOrElse(false)) {
                    false
                } else {
                    true
                }
            })
            rdd2
        })
//        val result: DStream[(String, (Int, Option[Boolean]))] = rdd2
        ds1.print()

        sc.start()
        sc.awaitTermination()
    }
}
