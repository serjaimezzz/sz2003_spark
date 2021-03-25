package _02OxDemo

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object _06OnlineBlacklistFilterOps {
    private val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("bl")
    private val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))
    val prop: Properties = new Properties()
    prop.load(_06OnlineBlacklistFilterOps.getClass.getClassLoader.getResourceAsStream("consumer.properties"))

    def main(args: Array[String]): Unit = {
        val blackListRDD: RDD[(String, Boolean)] = ssc.sparkContext.parallelize(List(("1.1.1.1", true),("2.2.2.2",true)))
        val lines: ReceiverInputDStream[String] = ssc.socketTextStream("master", 10087)
        //dataTemplate:     11.1.1.1##2020.12.10 11:10:10##Get
        /**
         * 如果数据格式输入错误，会引发StringIndexOutOfBoundsException
         */
        val ip2otherDS: DStream[(String, String)] = lines.map(x => {
            val index: Int = x.indexOf("##")    //返回指定的子字符串首次出现时该字符串内的索引
            val ip: String = x.substring(0, index)//ip = 11.1.1.1
            val other: String = x.substring(index + 2)//其他信息：2020.12.10 11:10:10##Get
            (ip, other)
        })

        val value: DStream[(String, String)] = ip2otherDS.transform(
            rdd => {
            val join: RDD[(String, (String, Option[Boolean]))] = rdd.leftOuterJoin(blackListRDD)
                join.foreach(println)

//          左表:(11.1.1.1,2020.12.10 11:10:10##Get)
//          右表:(1.1.1.1,true)
//          左外连接：(11.1.1.1,(2020.12.10 11:10:10##Get,None))
//          左外连接：(1.1.1.1,(2020.12.10 11:10:10##Get,Some(true)))
            join.filter {   //不符合条件就过滤掉
                case (ip, (left, right)) => {   //匹配上，就返回{}中的值
                    !right.isDefined    //相当于isEmpty,如果是None,则返回true
                }
            }//过滤掉黑名单中列表的元组._2有任意类型值的记录
              .map { case (ip, (left, right)) => {
                  (ip, left)
              }
              }
        })
        value.print()

        ssc.start()
        ssc.awaitTermination()
    }
}
