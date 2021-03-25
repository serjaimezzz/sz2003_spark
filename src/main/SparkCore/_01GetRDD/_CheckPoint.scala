package _01GetRDD

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 检查点机制：是血缘关系的存储机制
 */

object _CheckPoint {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("test").setMaster("local[3]")
        val sc: SparkContext = new SparkContext(conf)
        //指定检查点目录
        sc.setCheckpointDir("file:///D:\\A_1000feng\\Spark/checkpoint")
        val rdd: RDD[Int] = sc.parallelize(1 to 9)
        /**
         * 将此RDD标记为检查点。它将被保存到由`sc.setCheckpointDir`设置的检查点目录内的文件中，
         * 并且所有对其父RDD的引用都将被删除。在此RDD上执行任何作业之前，必须先调用此函数。
         * 强烈建议将此RDD保留在内存中，否则将其保存在文件中将需要重新计算。
         */
        val result: RDD[Int] = rdd.sortBy(x => x, false)
        result.cache() //使用默认存储级别（MEMORY_ONLY）缓存该RDD。
        result.persist()
        result.checkpoint()
        result.foreach(println)
    }
}
