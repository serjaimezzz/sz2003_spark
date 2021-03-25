package _03Window

import java.util.Properties

import org.apache.spark.sql.SparkSession
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.ReceiverInputDStream

object _04Sql_Streaming {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("test")
        conf.set("spark.driver.allowMultipleContexts","true")
        val sc: SparkContext = new SparkContext(conf)
        //过滤日志的信息等级
        sc.setLogLevel("ERROR")
        //注意：如果是sql与streaming的整合，需要调用StreamingContext(sparkContext:SparkContext,duration:Duration)
        //否则会出现多次创建SparkContext的情况
        val ssc: StreamingContext = new StreamingContext(sc, Seconds(10))
        ssc.checkpoint("file:///D:\\JavaInThinking\\IDEA\\sz2003_spark\\streamingCache")

        val dStream: ReceiverInputDStream[String] = ssc.socketTextStream("master", 10087)
        val session: SparkSession = SparkSession.builder().config(conf).getOrCreate()
        import session.implicits._

        val ds1 = dStream.map(x => {
            val arr: Array[String] = x.split(" ")
            (arr(1), arr(2).toInt)
        })


        val ds2 = ds1.updateStateByKey(
            //使用updateStateByKey进行累加
            (iter: Iterator[(String, Seq[Int], Option[Int])]) => {
                iter.map(x => {
                    (x._1, x._2.sum + x._3.getOrElse(0))
                })
            },
            new HashPartitioner(sc.defaultMinPartitions),
            true
        )

        //使用SparkSQL进行排名，取前三
        val ds3 = ds2.transform(x => {
            val df = x.toDF("key", "num")
            df.createOrReplaceTempView("tmp")
            val sql =
                """
                  |select t.key,t.num,t.rk
                  |from
                  |(
                  |select key,num,
                  |rank()over(sort by num desc) rk
                  |from tmp
                  |) t
                  |where t.rk<4
                  |""".stripMargin
            session.sql(sql).rdd
        })
        ds3.print()
        ssc.start()
        ssc.awaitTermination()
    }
}
