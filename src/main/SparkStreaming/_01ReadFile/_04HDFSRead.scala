package _01ReadFile

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Durations, StreamingContext}

object _04HDFSRead {
    private val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("test")
    private val sc: StreamingContext = new StreamingContext(conf, Durations.seconds(5))

    def main(args: Array[String]): Unit = {
        val ds: DStream[String] = sc.textFileStream("hdfs://master:8020/streaming/")
        ds.print()

        sc.start()
        sc.awaitTermination()
    }
}
