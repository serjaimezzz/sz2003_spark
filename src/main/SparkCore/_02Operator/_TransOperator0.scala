package _02Operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * local模式下,如果不指定内核数，默认使用1;
 *      - local
 *      - local[num],使用num内核
 *      - local[*]:单台机器处理器总内核数
 * cluster模式:默认使用所有worker的总内核数
 *
 * 所有RDD算子的计算功能全部都由executor执行,如 _*2
 * 算子的逻辑封装在Driver中执行
 * Driver决定计算发送到哪一个executor。
 */
object _TransOperator0 { //
    private val conf: SparkConf = new SparkConf().setMaster("local").setAppName("test")
    val sc = new SparkContext(conf)

    def main(args: Array[String]): Unit = {
//        mapTest
//        mapValueTest
//        mapPartitionsTest
//        mapPartitionsWithIndexTest
//        flatMapTest()
//        filterTest
    }

    /**
     * 筛选出符合条件的结果(true则保留,否则过滤掉)
     */
    def filterTest: Unit = {
        val datas: RDD[Int] = sc.makeRDD(1 to 10)
        //        println(datas)  //ParallelCollectionRDD[0] at makeRDD at _01TransformationOperator.scala:35
        val result: RDD[Int] = datas.filter(_ % 2 == 0)
        result.foreach(println)
    }


    def mapTest: Unit = {
        val rdd: RDD[Int] = sc.parallelize(1 to 10)
        //对每一个元素进行操作：_ * 2
        val mapRDD: RDD[Int] = rdd.map(_ * 2)
        mapRDD.foreach(println)
    }

    def mapValueTest: Unit = {
        /**
         * 通过映射函数传递键-值对RDD中的每个值，而无需更改键； 这也保留了原始RDD的分区。
         */
        val rdd1: RDD[(Int, String)] = sc.parallelize(List((1, "rose"), (2, "kyrie"), (3, "dw")), 3)
        val value: RDD[(Int, Int)] = rdd1.mapValues(_.length)
        //        value.foreach(println)
        val value1 = rdd1.mapValues(x => {
            (x, x.length > 4)
        })
        value1.foreach(println)
    }

    def mapPartitionsTest: Unit = {
        /**
         * 针对每一个分区进行遍历,形参是一个迭代器,指向一个分区
         * 返回一个新的迭代器
         * mapPartitions可以对一个RDD中所有的分区进行遍历
         * 算子只考虑分区,n个分区循环n遍
         * 效率更快,但是可能会出现内存溢出OOM
         * map函数会被调用n(元素数量)次,而mapPartitions只会被调用m(分区数量)次
         */

        val rdd: RDD[Int] = sc.parallelize(Array(1, 2, 3, 4, 0, 8, 5, 9), 3)

        val mp1 = rdd.mapPartitions(iter => iter.filter(_ > 2)) //迭代器
        //        mp1.foreach(println)

        val mp2: RDD[Int] = rdd.mapPartitions(_.toIterator)
        //        mp2.foreach(println)

        //将该函数发布到各个分区,减少了发送到执行器的交互次数
        val mp3 = rdd.mapPartitions(it => {
            it.map(x => x)
        })
        //        mp3.foreach(println)
    }

    def mapPartitionsWithIndexTest: Unit = {
        /**
         * 获取分区号,遍历每一个分区内的所有元素
         * part:ele
         * 关注的是哪一个分区
         */
        val rdd1 = sc.parallelize(Array(1, 2, 3, 4, 0, 8, 5, 9), 2)
        //查看一个分区有哪些元素
        val value: RDD[String] = rdd1.mapPartitionsWithIndex((part: Int, it: Iterator[Int]) => {
            it.map("分区" + part + ":" + _)
        })
        value.foreach(println)

        //查看元素在哪一个分区
        val value1 = rdd1.mapPartitionsWithIndex {
            case (num, datas) => {
                datas.map(_ + "分区号:" + num)
            }
        }
        value1.foreach(println)
    }

    def flatMapTest() {
        /**
         * 返回一个新的RDD，首先对RDD的所有元素应用一个函数，然后将结果扁平化。
         */
        val value: RDD[Int] = sc.parallelize(1 to 9)
        val flatRDD: RDD[Int] = value.flatMap(_ :: Nil)
        //        flatRDD.foreach(print)

        val rdd2 = sc.makeRDD(Array(List(1, 2), List(3, 4)))
        val flatMapRDD = rdd2.flatMap(x => x)
        flatMapRDD.foreach(println)
        //        flatMapRDD.collect.foreach(println)
    }


}
