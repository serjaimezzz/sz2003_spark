package _02OxDemo

import java.util
import java.util.Properties

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.{Jedis, JedisPool}

/**
 * 使用redis kv数据库来维护kafka
 *  1.从Redis中获取要读取的消息的开始offset
 *  2.通过offset获取数据，进行处理
 *  3.将读取到的最新的消息的offset更新到Redis
 *
 *  redis存储offset数据的设计思路：
 *
 *          groupName   pet#0   12
 *                      pet#1   10
 *                      pet#2   12
 *
 *  测试方法：
 *  1.启动Kafka、启动main方法
 *  2.Kafka发送数据，main方法读取数据并打印结果
 */
object _03RedisOffsetDemo {
    //获取配置对象
    private val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("redis")
    private val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))
    val prop: Properties = new Properties()
    prop.load(_03RedisOffsetDemo.getClass.getClassLoader.getResourceAsStream("consumer.properties"))

    def main(args: Array[String]): Unit = {
        val topics: Array[String] = Array("pet")

        //设置消费者属性信息
        val params: Map[String, String] = Map[String,String](
            "bootstrap.servers"->prop.getProperty("bootstrap.servers"),
            "group.id"->prop.getProperty("group.id"),
            "key.deserializer" -> prop.getProperty("key.deserializer"),
            "value.deserializer" -> prop.getProperty("value.deserializer"),
            "enable.auto.commit" -> "false"
        )

        //先获取jedis对象，连接redis,获取偏移量
        val redisUtils = new RedisUtils
        val jedis: Jedis = redisUtils.getJedis()
        //从Redis中获取偏移量
        val offsets: Map[TopicPartition, Long] = redisUtils.getOffset(jedis,prop)
        var dStream: InputDStream[ConsumerRecord[String, String]] = null

        if(offsets.size>0){
            dStream = KafkaUtils.createDirectStream(
                ssc,
                LocationStrategies.PreferConsistent,
                ConsumerStrategies.Subscribe[String,String](topics, params, offsets))
        }else{
            dStream = KafkaUtils.createDirectStream(
                ssc,
                LocationStrategies.PreferConsistent,
                ConsumerStrategies.Subscribe[String,String](topics, params))
        }

        dStream.foreachRDD(rdd=>{
            rdd.foreach(println)
            val ranges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
            //更新偏移量
            redisUtils.updateOffset(prop.getProperty("group.id"),ranges,jedis)
        })

        ssc.start()
        ssc.awaitTermination()
    }
}

/**
 * 自定义工具类，提供获取jedis客户端、获取offsets偏移量，以及更新方法
 */
class RedisUtils{
    def getJedis():Jedis = {
        val config: GenericObjectPoolConfig = new GenericObjectPoolConfig
        //设置最大连接数
        config.setMaxTotal(10)  //最大连接数
        config.setMaxIdle(5)    //最大空闲线程数
        config.setMinIdle(0)    //最小空闲数
        val pool: JedisPool = new JedisPool(config,"master", 6379)
        val jedis: Jedis = pool.getResource
        //设置密码
        jedis.auth("123456")
        jedis
    }

    def getOffset(jedis: Jedis,prop: Properties): Map[TopicPartition, Long] = {
        //先创建一个空的map集合
        var offsets:Map[TopicPartition,Long] = Map()
        //从Redis中通过组名作为key后获取对应的所有field和value
        val kvs: util.Map[String, String] = jedis.hgetAll(prop.getProperty("group.id"))
        import scala.collection.JavaConversions._
        for (kv <- kvs){
            val arr: Array[String] = kv._1.split("#")
            val topic = arr(0)
            val partition = arr(1).toInt
            offsets += (new TopicPartition(topic,partition) -> kv._2.toLong)
        }
        offsets
    }

    def updateOffset(groupName: String, ranges: Array[OffsetRange], jedis: Jedis): Unit = {
        for (x <- ranges){
            jedis.hset(groupName,x.topic+"#"+x.partition,x.untilOffset.toString)
        }
    }
}
