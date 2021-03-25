package _02OxDemo

import java.util.Properties

import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object _08UpdateStateByKey {
    private val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("zk")
    private val sc: StreamingContext = new StreamingContext(conf, Seconds(5))
    //  override val mustCheckpoint = true 状态需要保存
    sc.checkpoint("file:///D:\\JavaInThinking\\IDEA\\sz2003_spark\\streamingCheckPoint")
    val prop: Properties = new Properties()
    prop.load(_08UpdateStateByKey.getClass.getClassLoader.getResourceAsStream("consumer.properties"))

    def main(args: Array[String]): Unit = {
        val ds: ReceiverInputDStream[String] = sc.socketTextStream("master", 10087)
        //定义一个函数
        /**
         * 第一个参数K 表示key相同的K
         * 第二个参数 Seq[V] 表示这一批次的同一个key的v的集合
         * 第三个参数 Option[S] 表示上一批次的这个key的v的值
         * 返回值是一个迭代器，返回的是这个key与这一批次的值与上一批次值的累加之和
         */
        val func = (iter:Iterator[(String,Seq[Int],Option[Int])])=>{
            iter.map(x=>{
                (x._1,x._2.sum+x._3.getOrElse(0))
            })
        }

        /**
         * * @param updateFunc 状态更新
         * * @param partitioner 用于控制新dstream中每个rdd的分区
         * * @param rememberPartitioner 是否记住生成的RDD中的分区程序对象。
         * (hello,1)...
         * (hello,1+1+...)
         *
         */
        val result: DStream[(String, Int)] = ds.map((_, 1))
          .updateStateByKey(
            func,
            new HashPartitioner(sc.sparkContext.defaultMinPartitions),
            true
          )
        result.print()

        sc.start()
        sc.awaitTermination()
    }
}
