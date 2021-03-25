package _03Others

import org.apache.spark.{SparkConf, SparkContext}

/**
 * spark内置的两个分区器：
 * 1.HashPartitioner
 * 2.RangePartitioner
 */
object _Partition {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[2]").setAppName("test")
        val context = new SparkContext(conf)

        //1.从内存中创建makeRDD,底层实现就是parallelize
        /**
         * def makeRDD[T: ClassTag](
         * seq: Seq[T],numSlices: Int = defaultParallelism): RDD[T] = withScope {
         * //                           默认并行度,分区数
         * parallelize(seq, numSlices)
         * }
         */
        val listRDD = context.makeRDD(List(1, 2, 3, 4),2)//
        //2.从内存中创建parallelizeRDD
        val arrRDD = context.parallelize(Array(1, 2, 3, 4),3)//
        //3.从外部存储中创建,可以读取当前文件路径或HDFS上的文件路径
        //默认从文件中读取的数据类型都是字符串类型
        /**
         * minPartitions:
         * 分区数量,不赋值时，会使用默认值，机器的线程数量和2进行比较取最小值
         *
         * local模式不指定线程数量，线程数量就是1
         * cluster模式不指定线程数量时，默认使用的是CPU的内核数。如果所有worker的总核数>2,则使用2
         * 当指定值时，分区数量一定是指定值的数量
         */
        val fileRDD = context.textFile("input",2)
        //最小分区数取决于Hadoop读取文件时的分片规则 5/2得出每个分区的元素个数 2 2 1
        //计算分区和分区内放哪些元素是两种规则

//        listRDD.saveAsTextFile("listOutput")
//        arrRDD.saveAsTextFile("arrOutput")
        fileRDD.saveAsTextFile("fileOutput")
    }
}
