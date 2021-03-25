package _03Others

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, RangePartitioner, SparkConf, SparkContext}

object _06PartitionerDemo {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("test")
    private val context = new SparkContext(conf)

    def main(args: Array[String]): Unit = {
//        partByHashPartTest
//        partByRangePartTest
        partByMypartitionerTest
    }

    def partByHashPartTest: Unit = {
        /**
         * 指定分区所用的分区器
         */
        val rdd = context.parallelize(Array((1, "a"), (2, "b"), (3, "d"), (14, "k"), (21, "JB"), (1, "rose")), 4)
        val partRDD = rdd.partitionBy(new org.apache.spark.HashPartitioner(2))
        val array: Array[Array[(Int, String)]] = partRDD.glom().collect()
        for (elem <- array) {
            println(elem.mkString(","))
        }

        val value = partRDD.mapPartitionsWithIndex { case (num, datas) => {
            datas.map(_ + ":" + num)
        }
        }
        value.foreach(println)

        val myPartRDD: RDD[(Int, String)] = rdd.partitionBy(new MyPartitioner(3))

    }

    def partByRangePartTest: Unit = {
        /**
         * 指定分区所用的分区器
         */
        val rdd = context.parallelize(Array((1, "a"), (2, "b"), (3, "d"), (14, "k"), (21, "JB"), (1, "rose")), 4)
        val partRDD = rdd.partitionBy(new RangePartitioner(2,rdd))
        val array: Array[Array[(Int, String)]] = partRDD.glom().collect()
        for (elem <- array) {
            println(elem.mkString(","))
        }

        val value = partRDD.mapPartitionsWithIndex { case (num, datas) => {
            datas.map(_ + ":" + num)
        }
        }
        value.foreach(println)
    }


    def partByMypartitionerTest: Unit = {
        /**
         * 指定分区所用的分区器
         */
        val rdd = context.parallelize(Array(("Z",-1),("a",1),("-",1)))
        val mypartRDD = rdd.partitionBy(new MyPartitioner(3))
        val value = mypartRDD.mapPartitionsWithIndex {
            case (num, datas) => {
                datas.map(_ + ":" + num)
            }
        }
        value.foreach(println)
    }

    def _08CustomePartitioner: Unit ={
        val datas: RDD[String] = context.textFile("input/WordCount")
        val words: RDD[String] = datas.flatMap(_.split(" "))
        val kvs: RDD[(String, Int)] = words.map((_, 1))
        val value: RDD[(String, Int)] = kvs.reduceByKey(_ + _)
        val result: RDD[(String, Int)] = value.sortBy(x => x, false)
        //如果想要将数据按照自定义分区器保存，那么就需要放在行动算子之前
        val value1: RDD[(String, Int)] = result.partitionBy(new MyPartitioner(3))
        value1.foreach(println)
    }
}

/**
 * 自定义分区器
 * 需求：单词频率统计
 * aA-nN在0分区,其他字母开头的在1分区，剩下的字符开头的在2分区
 */
class MyPartitioner(partitionerNum:Int) extends Partitioner{
    //需要给分区器的数量赋值
    override def numPartitions: Int = {
        partitionerNum
    }

    override def getPartition(key: Any): Int = {
        val str = key.toString
        val first = str.substring(0, 1)
        if(first.matches("[A-Na-n]")){
            0
        }else if (first.matches("[o-zO-Z]")){
            1
        }else 2
    }
}