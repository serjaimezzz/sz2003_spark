package _02Operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object _PairRDDOp {
    val conf = new SparkConf().setMaster("local").setAppName("test")
    val sc = new SparkContext(conf)

    def main(args: Array[String]): Unit = {
//        fold
//        foldByKeyTest
//        aggByKeyTest
//        aggregateByKeyTest
//        aggregateTest
//        coGroupedRDDTest
    }

    def fold: Unit = {
        /**
         * x = (((0+1)+2+...+9)+1)
         */
        val datas: RDD[Int] = sc.parallelize(1 to 9)
        val result: Int = datas.fold(0)(_ + _)
        println(result)
    }

    def foldByKeyTest: Unit = {
        /**
         * 当分区内的处理和分区间的处理相同时,效果和aggByKey相同
         */
        val datas = sc.parallelize(List(("q", 1), ("w", 3), ("a", 4), ("w", 2), ("q", 5), ("a", 2)), 2)
        val value: RDD[(String, Int)] = datas.foldByKey(0)(_ + _)
        value.foreach(println)
    }

    def aggByKeyTest: Unit = {
        /**
         * 各分区内独立处理数据，再在分区间处理各个分区的数据
         * aggregateByKey[U: ClassTag](zeroValue: U)...:ClassTag运行时通过反射可以得到U的泛型
         */
        val datas = sc.parallelize(List(("q", 1), ("w", 3), ("a", 4), ("w", 2), ("q", 5), ("a", 2)), 2)
        //        val glomRDD = datas.glom().foreach(_.foreach(println))
        //取出分区内各组key的valueMax相加得到valueMaxSum,分区间的key相同的valueMaxSum再相加
        val value: RDD[(String, Int)] = datas.aggregateByKey(0)(math.max(_, _), _ + _)
        value.foreach(println)
    }


    def aggregateByKeyTest: Unit = {
        /**
         * 先分区,将各个分区内key相同的value累加到各个key的累加器上,
         * 再将各个分区的valueSum累加到一起(没有加到累加器上)
         * 带有一个初始值的形参，有一个分区内的函数形参，还有一个分区间的规约函数形参
         */
        val rdd: RDD[(String, Int)] = sc.parallelize(Array(("a", 1), ("b", 2), ("c", 3), ("a", 1), ("a", 1)), 2)
        val value: RDD[(String, Int)] = rdd.aggregateByKey(1)(_ + _, _ + _)
        value.foreach(println)
    }

    def aggregateTest: Unit = {
        /**
         * 带一个初始值,聚合所有分区内的元素
         * 先将每一个分区的元素累加到初始值上,然后将各个分区器的和累加到初始值上
         */
        val rdd1: RDD[Int] = sc.parallelize(Array(1, 2, 3, 4, 5), 2)
        val str: Int = rdd1.aggregate(5)(_ + _, _ + _)
        //  5+  (  (5+1+2)   +   (5+3+4+5)  )
        println(str)
    }


    def coGroupedRDDTest: Unit = {
        /**
         * 只能作用在K-V形式的RDD上,和另一个K-V形式的RDD按照Key来分组
         * 根据key来分组,将两组key相同的RDD的value用两组compactBuffer()聚合
         */
        val player: RDD[(Int, String)] = sc.parallelize(List((1, "rose"), (2, "kyrie"), (3, "ad")), 2)
        val team = sc.parallelize(List((1, "Chicago"), (2, "NewYork"), (3, "Los"), (1, "Bull")), 2)
        val value: RDD[(Int, (Iterable[String], Iterable[String]))] = player.cogroup(team)
        value.foreach(println)
    }


}
