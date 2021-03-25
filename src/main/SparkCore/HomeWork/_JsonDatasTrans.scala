package HomeWork

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.json.JSON

object _JsonDatasTrans {
    private val conf: SparkConf = new SparkConf().setMaster("local").setAppName("test")
    private val context: SparkContext = new SparkContext(conf)

    def main(args: Array[String]): Unit = {
        val datas: RDD[String] = context.textFile("input/rating.json")
//        datas.foreach(println)
//        datas.mapPartitions()

    }
}

//case class Movie()