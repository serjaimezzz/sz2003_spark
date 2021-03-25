package _01GetRDD

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object JobSubmit {
    def main(args: Array[String]): Unit = {
        val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
        val sc: SparkContext = new SparkContext(config)

        val file: RDD[String] = sc.textFile(args(0))

        file.flatMap(_.split(" "))
          .map((_,1))
          .reduceByKey(_ + _)
          .saveAsTextFile(args(1))

        sc.stop()


    }
}
