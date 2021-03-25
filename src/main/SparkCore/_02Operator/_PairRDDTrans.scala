package _02Operator


import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object _PairRDDTrans {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("test")
    val context = new SparkContext(conf)

    def main(args: Array[String]): Unit = {
//        groupByTest
//        groupbyTest1
//        sortByTest
        reduceByKeyTest()
//        combineByKeyTest0
//        combineByKeyTest1
    }

    def groupByTest: Unit = {
        /**
         * 将元素按照指定规则进行分组,
         * 分组后的数据形成了KV对元组(K-V),K表示分组的key,Value表示分组的数据集合
         */
        val rdd1: RDD[Int] = context.parallelize(1 to 16, 2)
        val value: RDD[(Boolean, Iterable[Int])] = rdd1.groupBy(_ % 2 == 0)
        value.foreach(println)
        val tuples: Array[(Boolean, Iterable[Int])] = value.collect()
        for (elem <- tuples) {
            println(elem._1 + ":" + elem._2.toList)
        }

        /**
         * 结果:
         * (false,CompactBuffer(1, 3, 5, 7, 9, 11, 13, 15))
         * (true,CompactBuffer(2, 4, 6, 8, 10, 12, 14, 16))
         *
         * false:List(1, 3, 5, 7, 9, 11, 13, 15)
         * true:List(2, 4, 6, 8, 10, 12, 14, 16)
         */
    }

    def groupbyTest1: Unit = {
        val datas = context.parallelize(Array("one", "two", "three", "one", "two", "two", "two"))
        val tuples: RDD[(String, Iterable[Int])] = datas.map(word => (word, 1)).groupByKey()
        tuples.foreach(println)
        //(two,CompactBuffer(1, 1, 1, 1))
        //(one,CompactBuffer(1, 1))
        //(three,CompactBuffer(1))
        val result: RDD[(String, Int)] = tuples.map(x => (x._1, x._2.sum))
        result.foreach(println)
        //(two,4)
        //(one,2)
        //(three,1)
    }

    def groupByKeyTest(): Unit = {
        val tuple: Map[String, Int] = Map(("c", 3), ("b", 2), ("a", 1), ("c", 3), ("d", 3), ("e", 3))
        val seq: RDD[(String, Int)] = context.parallelize(tuple.toSeq)

        /**
         * 将RDD中每个key的值分组为单个序列。
         * 对生成的RDD进行哈希分区，将其划分为“numPartitions”分区。
         * 每个组内的元素的顺序是不能保证的，甚至可能在每次评估RDD结果时都有所不同。
         */
        //        seq.groupByKey(3).foreach(println)//每个组内的元素的顺序是不能保证的，甚至可能在每次评估RDD结果时都有所不同。


        /**
         * 只能作用在K-V形式的RDD上
         * 根据key来分组,相同K的value组合到一起,形成迭代器
         * 底层也会创建shuffleRDD
         */
        val player: RDD[(Int, String)] = context.parallelize(List((1, "rose"), (2, "kyrie"), (3, "ad"), (0, "lillard"), (0, "WB"), (23, "LBJ"), (23, "MJ")), 2)
        val value: RDD[(Int, Iterable[String])] = player.groupByKey(2)
        value.foreach(println)

    }

    def sortByTest: Unit = {
        /**
         * 对RDD的元素进行全局排序
         * numPartitions:可指定分区      分区越多-->Task越多-->并行度越高-->性能越快（shuffle除外）
         */
        val rdd1: RDD[Int] = context.parallelize(1 to 16, 2)
        val sortByRDD = rdd1.sortBy(x => x, false, 5)
        sortByRDD.foreach(println)
    }

    def sortByKeyTest(): Unit = {
        val tuple: Map[String, Int] = Map(("c", 3), ("b", 2), ("a", 1), ("c", 3), ("d", 3), ("e", 3))
        val seq: RDD[(String, Int)] = context.parallelize(tuple.toSeq)
        /**
         * 按key对RDD进行排序，以便每个分区都包含一个已排序的元素范围。
         * 在生成的RDD上调用' collect '或' save '将返回或输出一个有序的记录列表
         * (在' save '情况下，它们将被写入文件系统中的多个' part-X '文件中，按键的顺序)。
         */
        val value8: RDD[(String, Int)] = seq.sortByKey(true, 3)
        //        value8.foreach(println)

        /**
         * 只能作用在K-V形式的RDD,对Key进行排序
         * 底层创建了shuffleRDD(宽依赖)
         */
        val player: RDD[(Int, String)] = context.parallelize(List((1, "rose"), (2, "kyrie"), (3, "ad"), (0, "lillard")), 2)
        player.sortByKey(false, 1).foreach(println)
    }


    def combineByKeyTest: Unit = {
        /**
         * 通过Key来合并value
         * 第一个参数:value的初始值,也就是累加器
         * 第二个参数:函数,作用于分区内,将每一个元素累加到初始值上
         * 第三个参数:函数,作用与分区间,将不同分区的结果累加到同一个Key上
         * 底层也创建了shuffleRDD,涉及到宽依赖,触发一个stage
         */
        val player: RDD[(Int, String)] = context.parallelize(List((1, "rose"), (2, "kyrie"), (3, "ad"), (0, "lillard"), (0, "WB"), (23, "LBJ"), (23, "MJ")), 2)
        //        player.foreach(println)
        val function = player.combineByKey(
            x => x,
            (x: String, y: String) => {
                x + y
            },
            (x: String, y: String) => {
                x + y
            }
        )
        function.foreach(println)
    }

    def combineByKeyTest0: Unit = {
        /**
         * 通过Key来合并value
         * 第一个参数:value的初始值,也就是累加器
         * 第二个参数:函数,作用于分区内,将每一个元素累加到初始值上
         * 第三个参数:函数,作用与分区间,将不同分区的结果累加到同一个Key上
         * 底层也创建了shuffleRDD,涉及到宽依赖,触发一个stage
         *
         */
        val player: RDD[(Int, String)] = context.parallelize(List((1, "rose"), (2, "kyrie"), (3, "ad"), (0, "lillard"), (0, "WB"), (23, "LBJ"), (23, "MJ")), 2)
        //        player.foreach(println)
        val function = player.combineByKey(
            x => x,
            (x: String, y: String) => {
                x + y
            },
            (x: String, y: String) => {
                x + y
            }
        )
        function.foreach(println)
    }

    def combineByKeyTest1: Unit = {
        val datas: RDD[(String, Int)] = context.parallelize(Array(("a", 10), ("a", 90), ("b", 10), ("a", 20), ("b", 90), ("a", 32)), 2)
        //        datas.foreach(println)
        val combine: RDD[(String, (Int, Int))] = datas.combineByKey(
            (_, 1), //初始值value变成元组
            (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1), //分区内
            (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc2._2 + acc2._2) //分区间
        )

        //        val tuples: Array[(String, (Int, Int))] = combine.collect()
        //        tuples.foreach(println)

        val result: RDD[(String, Double)] = combine.map {
            case (key, value) => (key, (value._1 / value._2).toDouble)
        }
        result.foreach(println)
    }

    def reduceByKeyTest(): Unit = {
        /**
         * 相同Key的value进行规约
         */
        val tuple: Map[String, Int] = Map(("c", 3), ("b", 2), ("a", 1), ("c", 3), ("d", 3), ("e", 3))
        val seq: RDD[(String, Int)] = context.makeRDD(tuple.toSeq)
        /**
         * 使用关联和可交换(associative and commutative)的reduce函数合并每个键的值。
         * 在将结果发送到一个reducer之前，它也会在每个mapper上执行局部合并，类似于MapReduce中的“合并器”。
         * 输出将使用numPartitions分区进行哈希分区。
         */
        val value7 = seq.reduceByKey(_ + _, 3)
        //        value7.foreach(println)

        val rdd1: RDD[(Int, String)] = context.parallelize(List((1, "rose"), (2, "kyrie"), (3, "ad"), (0, "lillard"), (0, "WB"), (23, "LBJ"), (23, "MJ")), 2)
        val value: RDD[(Int, String)] = rdd1.reduceByKey((x: String, y) => {
            x + y
        }) //x:value.y:value
        value.foreach(println)

    }
}
