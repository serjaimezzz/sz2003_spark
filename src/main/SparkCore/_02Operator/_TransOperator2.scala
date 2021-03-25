package _02Operator

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object _TransOperator2 {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("test")
    val context = new SparkContext(conf)
    def main(args: Array[String]): Unit = {
//        unionTest
//        intersectionTest()
        joinTest()
//        zipTest()
    }

    def unionTest: Unit ={
        /**
         * 返回这个RDD和另一个RDD的f `并集 `。
         * 任何相同的元素将出现多次(使用' .distinct() '消除它们)。
         */
        val value1: RDD[Int] = context.parallelize(Array(1, 2, 3, 4, 5))
        val value2: RDD[Int] = context.parallelize(Array(1, 3, 5, 7, 9))
        //union:相同的元素出现多次
        val unionRDD: RDD[Int] = value1.union(value2)
        unionRDD.foreach(print)//1234513579
        val subRDD = value1.subtract(value2)
        subRDD.foreach(print)//24
    }

    def intersectionTest(){
        /**
         * 交集
         */
        val rdd1: RDD[Int] = context.parallelize(Array(1,2,3,4,5,6),3)
        val rdd2: RDD[Int] = context.makeRDD(List(1, 3, 5, 7, 9), 3)
        //intersection
        val value1 = rdd1.intersection(rdd2) //默认两个分区;返回和另一个RDD的交集,不包含重复元素
        val value2 = rdd1.intersection(rdd2,3)//指定分区数,跨集群执行散列分区
        //        val value3 = value intersection(value0, HashPartitioner)
        //        value1.foreach(println)
        value2.foreach(println)//31 5
    }

    def joinTest(): Unit ={
        val rdd1 = context.parallelize(List((1001, "zhangsan"), (1002, "lisi"), (1003, "wangwu"),(1004, "zhaoyun")))
        val rdd2 = context.parallelize(List((1001, "深圳"), (1002,null), (1003, "北京"),(1006,"长春")))
        //内连接
        val joined: RDD[(Int, (String, String))] = rdd1.join(rdd2)
        //左外
        val leftjoinRDD = rdd1.leftOuterJoin(rdd2)
        println("左外连接：")
        leftjoinRDD.foreach(println)
        //右外
        val rjoin = rdd1.rightOuterJoin(rdd2)
        println("右外连接：")
        rjoin.foreach(println)
    }

    def zipTest(): Unit ={
        val rdd1 = context.parallelize(Array(1, 2, 3))
        val rdd2 = context.parallelize(Array("a", "b", "c"))

        /**
         * 两个RDD做拉链，长度必须统一
         * scala拉不上就不管了,而spark会报错
         */
        //两个RDD做拉链
        rdd1.zip(rdd2).foreach(print)//(1,a)(2,b)(3,c)
        //与自己的下标做拉链
        rdd1.zipWithIndex().foreach(print)//(1,0)(2,1)(3,2)
    }
}
