package _03Others

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable

object _PartitionerDemo1 {
    private val conf: SparkConf = new SparkConf().setMaster("local").setAppName("test")
    private val context = new SparkContext(conf)

    def main(args: Array[String]): Unit = {
        subjectPartitionerTest
        context.stop()
    }

    def subjectPartitionerTest: Unit ={
        val datas: RDD[String] = context.textFile("input/subjects.txt")
        val tuples= datas.map(line => {
            val fields = line.split("[\\s]+")
            //取出URL
            val str = fields(1)
            //获取地址的host，主域名
            val url  = new URL(str)
            val host = url.getHost
            (host,1)
        })

        //将相同的URL进行聚合，得到各个学科的访问量
        val sumed: RDD[(String, Int)] = tuples.reduceByKey(_ + _).cache()

        //将所有学科取出来，然后去重，并返回Scala对象
        val subjects: Array[String] = sumed.keys.distinct().collect()

        //创建自定义分区器对象 并分区
        val value: RDD[(String, Int)] = sumed.partitionBy(new SubjectPartitioner(subjects))
        value.foreach(println)
    }
}


/**
 * 自定义分区器需要继承Partitioner并实现对应方法
 * @param subjectArr 学科数组
 */
class SubjectPartitioner(subjectArr:Array[String]) extends Partitioner{
    //创建一个map集合用来存学科和分区号
    val subject = new mutable.HashMap[String,Int]()
    //定义一个计数器，用来生成分区号
    var i = 0
    for (elem <- subjectArr) {
        subject += (elem -> i) // 存学科和分区
        i += 1 // 分区自增
    }

    override def numPartitions: Int = subjectArr.size

    override def getPartition(key: Any): Int = subject.getOrElse(key.toString,0)
}