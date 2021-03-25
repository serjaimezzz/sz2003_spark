package _01GetRDD

import java.io

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 获取RDD对象的方式:
 * 1.通过读取外部文件(本地文件,HDFS文件)来获取RDD;只能读纯文本文件。
 * 2.调用makeRDD()或者parallelize()从集合中创建
 * 3.其他RDD调用算子转换而来
 */
object _01GetRDD {
    //获取配置对象
    val conf = new SparkConf().setAppName("test").setMaster("local")
    //获取上下文对象
    val sc = new SparkContext(conf)

    def main(args: Array[String]): Unit = {
//        a
//        b
//        c

    }

    def a: Unit ={
        //第一种:读取外部文件获取RDD
        val textFileRDD: RDD[String] = sc.textFile("file:///D:\\JavaInThinking\\IDEA\\sz2003_spark\\input/wordcount")
        textFileRDD.foreach(println)
    }

    def b: Unit ={
        //第二种:从集合中创建,调用makeRDD;如果未指定分片,使用默认值:2
        val rdd1: RDD[Int] = sc.makeRDD(Array(1, 3, 5, 7, 9), 3)
        //RDD类型为Array中元素的数据类型
        val rdd2: RDD[List[String]] = sc.parallelize(List(List("z","r","d","a"), List("a","d")),2)
        rdd1.foreach(println)
        rdd2.foreach(println)
    }

    def c: Unit ={
        //RDD类型为Array中元素的数据类型
        val rdd1: RDD[List[String]] = sc.parallelize(List(List("z","r","d","a"), List("a","d")),2)
        //第三种:从其他RDD转换而来
        val rdd2: RDD[Char] = rdd1.flatMap(_.mkString.toList)
        rdd2.foreach(println)
    }
}
