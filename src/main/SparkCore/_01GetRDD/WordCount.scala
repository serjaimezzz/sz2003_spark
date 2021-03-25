package _01GetRDD

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 编写spark程序，使用的都是RDD(弹性分布式数据集)
 * 来完成一个WordCount小程序
 *
 * RDD算子分两大类：
 *      - 转换算子 transform
 *      - 行动算子 Action
 *      (打标记,最后一起运算)
 */
object WordCount {
    def main(args: Array[String]): Unit = {

        //1.设定Spark计算框架的运行(部署)环境: local模式，并创建SparkConf对象
        val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

        //2.创建上下文对象
        val sc: SparkContext = new SparkContext(config)

        //3.读取文件,将文件内容一行一行的读取出来
        //        args(0)="in/WordCount"
        val file: RDD[String] = sc.textFile("input/WordCount")

        //4.将一行一行的数据分解成一个一个的单词:按空格切分并压平
        val lines: RDD[String] = file.flatMap(_.split(" "))

        //5.为了统计方便,将单词数据进行结构的转换:构成元组
        val kvs: RDD[(String, Int)] = lines.map((_, 1))

        //6.对转换结构后的数据进行分组聚合
        val wordSum: RDD[(String, Int)] = kvs.reduceByKey(_ + _)    //相当于x+=y第一个_存参x

        //7.将统计结果采集后打印到控制台
        val result: Array[(String, Int)] = wordSum.collect()
        //        println(result)//[Lscala.Tuple2;@5aaaa446
        result.foreach(println)

        //7.采集结果排序并输出到第二个参数arg(1)所给的路径
        //        val value: RDD[(String, Int)] = wordSum.sortBy((_._2,false))
        //        value.saveAsTextFile(args(1))
        //        println(sc)
    }
}
