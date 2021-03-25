package _02Operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 行动操作返回的是其他的数据类型
 */
object ActionOperator {
    val conf = new SparkConf().setAppName("ReflectionDemo").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)

    def main(args: Array[String]): Unit = {
        //        collectTest
        //        countTest()
        //        firstTest()
                takeTest()
        //        countByValueTest
        //        countByKeyTest
//                reduceTest
//                takeSampleTest
//        takeOrderedTest
    }

    def takeOrderedTest: Unit = {
        /**
         * 返回由指定隐式排序[T]定义的RDD中的第一个k(最小的)元素，并维护排序。这与[[top]]正好相反。
         */
        val rdd1 = sc.parallelize(Array(9, 91, 2, 4, 6, 7, 8, 9), 2)
        val array: Array[Int] = rdd1.takeOrdered(3)
        array.foreach(println)
    }

    def takeSampleTest: Unit = {
        val rdd1 = sc.parallelize(1 to 9)
        /**
         * 在数组中返回此RDD的固定大小的采样子集
         *
         * withReplacement:是否通过替换取样
         * num:返回样本的大小
         * seed:随机数生成器的种子
         */
        val array: Array[Int] = rdd1.takeSample(true, 4, 0)
        array.foreach(println)
    }

    def reduceTest: Unit = {
        val rdd1: RDD[Int] = sc.parallelize(Array(1, 2, 4, 9), 3)
        //        val result1: Int = rdd1.reduce(_ + _)
        /**
         * 规约
         */
        val result2: Int = rdd1.reduce((x: Int, y: Int) => {
            y - x
        })
        println(result2)
    }

    def collectTest(): Unit = {
        /**
         * Return an array that contains all of the elements in this RDD
         * 返回一个数组包含RDD中的所有元素。
         * 先局部收集各个分区中的元素，再汇总。
         */
        val rdd1 = sc.parallelize(Array(1, 2, 3, 4, 5, 6, 7, 8, 9))
        val array: Array[Int] = rdd1.map(_ * 2).collect()
        array.foreach(println)
    }

    def countTest(): Unit = {
        val rdd1 = sc.parallelize(Array(1, 2, 3, 4, 5, 6, 7, 8, 9), 4)
        /**
         * 返回RDD中的元素数量。
         * 先计算所有分区的元素个数,再将各个分区的元素数进行汇总
         */
        val num: Long = rdd1.count()
        println(num)
    }

    def firstTest(): Unit = {
        val rdd1 = sc.parallelize(List(List(2, 3, 1), List(5, 6)), 4)
        //返回RDD中第一个分区的第一个元素
        val ele = rdd1.first()
        println(ele)
    }

    def takeTest(): Unit = {
        val rdd1 = sc.parallelize(Array(5, 2, 3, 4, 5, 6, 7, 8, 9), 2)
        /**
         * 取RDD的前n个元素。它首先扫描一个分区，分区是按照顺序排列的
         * * 然后使用该分区的结果来估计需要满足的其他分区的数量的限制。
         */
        val array: Array[Int] = rdd1.take(3)
        println(array.mkString(","))
    }

    def countByValueTest: Unit = {
        /**
         * 通过value统计元素的重复次数
         * 将此RDD中每个唯一值的计数作为(value、count)对的本地映射返回。
         */
        val rdd1 = sc.parallelize(Array(5, 2, 3, 4, 5, 6, 7, 8, 9), 2)
        val countByV: collection.Map[Int, Long] = rdd1.countByValue()
        countByV.foreach(println)
    }

    def countByKeyTest: Unit = {
        /**
         * 统计key的重复次数
         */
        val rdd1 = sc.parallelize(List((1001, "zhangsan"), (1002, "lisi"), (1003, "wangwu"), (1004, "zhaoyun"), (1004, "zzz")))
        val countByK: collection.Map[Int, Long] = rdd1.countByKey()
        countByK.foreach(println)

    }
}
