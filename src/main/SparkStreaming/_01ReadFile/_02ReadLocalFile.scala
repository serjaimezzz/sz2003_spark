package _01ReadFile

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Durations, StreamingContext}

/**
 * 处理本地文件，只能是IO流产生的新文件,并且只能读一次
 * 追加进入该文件时读取不到
 */
object _02ReadLocalFile {
    private val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("test")
    private val sc: StreamingContext = new StreamingContext(conf, Durations.seconds(5))

    def main(args: Array[String]): Unit = {
        val ds: DStream[String] = sc.textFileStream("file:///output")
        ds.print(10)

        sc.start()

        sc.awaitTerminationOrTimeout(10000)
    }
}
