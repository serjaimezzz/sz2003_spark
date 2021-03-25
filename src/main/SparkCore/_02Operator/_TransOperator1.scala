package _02Operator

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object _TransOperator1 {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("test")
    val sc = new SparkContext(conf)
    def main(args: Array[String]): Unit = {
//        sampleTest
//        glomTest
//        coalesceRDD   //优化版的repartition
//        coalesceTest
//        repartitionTest
        testDistinct
    }

    def glomTest: Unit = {
        /**
         * 返回通过将每个分区内的所有元素合并到数组中而创建的RDD。
         */
        val rdd1 = sc.parallelize(1 to 18, 4)
        val value: RDD[Array[Int]] = rdd1.glom()
        //                value.foreach(_.foreach(println))         //遍历方式1
        //        value.foreach(x => x.foreach(println))    //遍历方式2
        for (elem <- value.collect()) { //遍历方式3
            //            println(elem.max)
            println(elem.mkString(","))
        }
    }

    def sampleTest: Unit ={
        val rdd1 = sc.parallelize(1 to 10)

        /**
         * 随机从RDD中按照比例取样
         * withReplacement:取样后是否放回,true表示放回;    src:元素是否可以被多次采样(在采样后替换)
         * fraction:取样的比例，决定打分的标准,1是全部抽取出来,0是全不出来
         * seed:随机种子；指定随机数生成器种子(第一个生成的随机数)。
         * 带随机种子的是真随机，下次随机的结果和上次一样；不带随机种子的是伪随机
         */
        val sampleRDD = rdd1.sample(false, 0.3, 1)
        sampleRDD.collect().foreach(println)
    }



    /**
     * 返回缩小为numPartitions分区的新RDD。
     */
    def coalesceRDD: Unit ={
        val listRDD = sc.makeRDD(1 to 16, 4)
        println("重新分区前:"+listRDD.partitions.size)   //4
        val coalesceRDD = listRDD.coalesce(3)
        println("重新分区后:"+coalesceRDD.partitions.size)//3
    }

    def coalesceTest: Unit ={
        /**
         * 重新分区，可以指定是否使用shuffle,默认不使用shuffle
         * 使用shuffle指的是将原RDD的所有分区的数据重新打乱分区
         * 不使用shuffle时,指定的分区数量不能大于原来的RDD数量
         */
        val rdd1: RDD[String] = sc.parallelize(Array("a", "b", "c", "b", "a", "e"),3)
        val str1 = rdd1.aggregate("|")(_+_,_+_)
        println("重新分区前:"+str1)
        //使用coalesce重新分区
        val newRDD = rdd1.coalesce(4,true)
        val value = newRDD.mapPartitionsWithIndex((part:Int, it:Iterator[String]) => it.map(part+":"+_))
        value.foreach(println)
        val str2 = newRDD.aggregate("|")(_+_,_+_)
        println("重新分区后:"+str2)
    }

    def repartitionTest: Unit ={
        /**
         * 底层调用coalesce(numPartitions,true),底层默认调用shuffle
         */
        val rdd1: RDD[String] = sc.parallelize(Array("a", "b", "c", "b", "a", "e"),2)
        val str1 = rdd1.aggregate("|")(_+_,_+_)
        println("重新分区前:"+str1)
        val newRDD = rdd1.repartition(3)
        val value = newRDD.mapPartitionsWithIndex((part: Int, it: Iterator[String]) => {
            it.map(part + ":" + _)
        })
        value.foreach(println)
        val str2 = newRDD.aggregate("|")(_+_,_+_)
        println("重新分区后:"+str2)

        val glomRDD = newRDD.glom().collect()
        for (elem <- glomRDD) {
            println(elem.mkString(","))
        }
    }


    /**
     * 底层调用了reduceByKey
     */
    def testDistinct={
        val rdd2 = sc.makeRDD(Array("1","2","3","4","1","2","3"),2)
        //查看如何分区的
        // rdd2.mapPartitionsWithIndex(myfunc).foreach(println)
        /**
         * distinct的底层流程
         *
         * 1      1 null
         * 2      2 null
         * 3      3 null                   1 null            1 null
         *                                 1 null            3 null        1  3
         *                                 3 null
         *                                 3 null
         *
         * 4      4 null
         * 1      1 null                 2 null            2 null        2  4
         * 2      2 null                 2 null            4 null
         * 3      3 null                 4 null
         *
         */
        rdd2.distinct(2).foreach(println)
    }


}
