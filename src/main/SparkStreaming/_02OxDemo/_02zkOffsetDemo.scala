package _02OxDemo

import java.util
import java.util.Properties

import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 使用zookeeper来手动维护sparkStreaming消费kafka消息的offset
 * 1.从zookeeper中获取偏移量
 * 2.处理数据
 * 3.将最新的offset维护到zookeeper
 */

object _02zkOffsetDemo {
    //获取配置对象
    private val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("zk")
    private val sc: StreamingContext = new StreamingContext(conf, Seconds(5))
    val prop: Properties = new Properties()
    prop.load(_02zkOffsetDemo.getClass.getClassLoader.getResourceAsStream("consumer.properties"))

    /**
     * 方法的入口
     * @param args
     */
    def main(args: Array[String]): Unit = {

        val topics: Array[String] = Array("pet")

        val params = Map[String,String](
            "bootstrap.servers"->prop.getProperty("bootstrap.servers"),
            "group.id"->prop.getProperty("group.id"),
            "key.deserializer" -> prop.getProperty("key.deserializer"),
            "value.deserializer" -> prop.getProperty("value.deserializer"),
            "enable.auto.commit" -> "false"
        )

        //需要连接zookeeper,获取消费者消费的offsets
        val zkUtils = new ZookeeperUtils(prop.getProperty("zookeeper.servers"))
        val offsets: Map[TopicPartition,Long] = zkUtils.getOffsets(prop.getProperty("group.id"), topics)

        var ds: InputDStream[ConsumerRecord[String,String]] = null
        //如果获取的offsets的长度大于0，说明不是第一次消费
        if (offsets.size > 0){
            ds = KafkaUtils.createDirectStream(
                sc,
                LocationStrategies.PreferConsistent,
                ConsumerStrategies.Subscribe[String,String](topics,params,offsets))
            println("这个主题不是第一次消费")
        }else {
            ds = KafkaUtils.createDirectStream(
                sc,
                LocationStrategies.PreferConsistent,
                ConsumerStrategies.Subscribe[String, String](topics, params))
            println("这个主题是第一次消费")
        }

        //遍历kafka的每一条消息
        ds.foreachRDD(rdd => {
            rdd.foreach(x => {
                x.offset()
                x.partition()
                println(x.partition() + "分区：" +x.offset()+ "\t value:"+x.value())
            })
            //获取消费信息的最后一个offset
            val offset: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
            //更新到zookeeper
            zkUtils.updateOffset(prop.getProperty("group.id"),offset)
        })

        sc.start()
        sc.awaitTermination()
    }

}

/**
 * 自定义一个zookeeper的工具类，用来连接zookeeper和获取zookeeper里的offset，
 * 以及更新offset的方法
 * @param zkServersInfo
 */
class ZookeeperUtils(zkServersInfo: String){
    //路径：/kafka-2020/offsets/groupName/topicName/partitionNum/
    //基础路径：
    val base_zk_path = "/kafka-2020/offsets"

    //连接zookeeper
    val zkCli = {
        val zkClient = CuratorFrameworkFactory.builder().connectString(zkServersInfo)
          .retryPolicy(new ExponentialBackoffRetry(1000, 3)).build()
        zkClient.start()//启动客户端，保持与zookeeper的连接
        zkClient
    }


    //检查zookeeper上的路径是否存在，如果不存在就创建
    def checkPathExist(path: String) = {
        if (zkCli.checkExists().forPath(path) == null){
            zkCli.create().creatingParentsIfNeeded().forPath(path)
        }
    }

    /**
     * Exception in thread "main" java.lang.IllegalArgumentException: Path must start with / character
     *
     */

    //定义一个方法，来获取zookeeper上的偏移量
    def getOffsets(groupName: String, topics: Array[String]): Map[TopicPartition, Long] =
        {
            //存储获取到的offsets
            var offsets: Map[TopicPartition, Long] = Map()
            //遍历主题的集合
            for(topic <- topics){
                val path = s"${base_zk_path}/${groupName}/${topic}" // /kafka-2020/offsets/"group.id"/Array("pet")
                //检查路径是否存在
                checkPathExist(path)
                //获取当前路径下的子节点，也就是当前主题下所有分区的znode
                println(path)
                val parts: util.List[String] = zkCli.getChildren.forPath(path)
                //从每一个分区中获取offset
                import scala.collection.JavaConversions._
                for(partition <- parts){
                    //从znode中获取存储的数据，也就是offset
                    val bytes: Array[Byte] = zkCli.getData.forPath(partition)
                    //将字节数组转成Long类型的offset
                    val offset: Long = new String(bytes).toLong
                    //将对应分区的offset添加到map中
                    offsets += (new TopicPartition(topic,partition.toInt) -> offset)
                }
            }
            offsets
        }

    /**
     * 更新偏移量
     * @param groupName
     * @param offset
     */
    def updateOffset(groupName: String, offset: Array[OffsetRange]): Unit = {
        for (x <- offset){
            val path = s"${base_zk_path}/${groupName}/${x.topic}/${x.partition}"
            checkPathExist(path)
            zkCli.setData().forPath(path,x.untilOffset.toString.getBytes())
        }
    }
}
