package _02OxDemo

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}


/**
 *  SparkStreaming与Kafka的整合，用于消费Kafka里的信息
 *  使用的整合包是0-10.  使用里面的Direct直连方式。    而0-8里除了direct还有一个reciver方法
 */
object _01AutoCommitOffset {
    private val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("test")
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    private val sc: StreamingContext = new StreamingContext(conf, Durations.seconds(5))

    def main(args: Array[String]): Unit = {
        //设置消费者的属性信息
        val params = Map[String,String] (
            "bootstrap.servers" -> "master:9092,slave1:9092,slave2:9092",
            "group.id" -> "connTest",
            "auto.offset.reset" -> "earliest",
            "key.deserializer" -> "org.apache.kafka.common.serialization.IntegerDeserializer",
            "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
            "enable.auto.commit"->"true"    //让SparkStreaming自动维护offset
        )

        //使用整合包里的工具，调用直连方法
        val ds: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
            sc,
            LocationStrategies.PreferConsistent, //指定消费策略，SS会为Kafka主题中的每一个分区分配一个算子
            ConsumerStrategies.Subscribe[String, String](Array("pet").toSet, params))
        //打印数据
        ds.map(_.value()).print()
        //启动
        sc.start()
        sc.awaitTermination()
    }
}
