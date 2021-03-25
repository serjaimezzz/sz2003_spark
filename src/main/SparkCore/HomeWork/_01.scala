package HomeWork

import java.io.File

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

object _01 {
    private val conf: SparkConf = new SparkConf().setMaster("local").setAppName("test")
    private val context = new SparkContext(conf)
    def main(args: Array[String]): Unit = {
//        work1
//        work2
//        work3
        work4
    }

    def work1: Unit ={
        val rdd: RDD[(String, Int)] = context.makeRDD(List(("spark", 2), ("hadoop", 6), ("hadoop", 4), ("spark", 6)))

        val newRDD = rdd.map(x=>{
            val bookname = x._1
            val sales = x._2
            (bookname,(sales,1))
        })

        val totalRDD = newRDD.reduceByKey((x, y) => {
            val totalsales = x._1 + y._1
            val totaldays = x._2 + y._2
            (totalsales, totaldays)
        })

        val resultRDD = totalRDD.map(x => {
            (x._1, x._2._1 / x._2._2)
        })
        resultRDD.foreach(println)

    }

    def work10: Unit ={
        val datas: RDD[(String, Int)] = context.makeRDD(List(("spark", 2), ("hadoop", 6), ("hadoop", 4), ("spark", 6)))
//        datas.aggregate()
    }


    def work2: Unit ={
        val rdd = context.textFile("D:\\A_1000feng\\Spark\\day04\\README.md")
//        rdd.foreach(println)
        //遍历不是字母的符号-->字符数组
        val wordsOrg = rdd.flatMap(_.split(Array(',', ' ', '.', '#', '/','<','>',':','[',']','(',')','-','"','+','`')))
//        words.foreach(println)
        /**
         * stopwords过滤：
         */
        var stopWords:Iterator[String] = null
        val stopWordsFile = new File("D:\\A_1000feng\\Spark\\day04\\stopwords.txt")
        if(stopWordsFile.exists()){
            stopWords = Source.fromFile(stopWordsFile).getLines()
        }
        val stopWordsList = stopWords.toList

        val words = wordsOrg.filter(!_.isEmpty).filter(!stopWordsList.contains(_))
//        words.foreach(println)

        val wordsTuple = words.map((_,1)).reduceByKey(_ + _).map(x=>{(x._2,x._1)}).sortByKey(false).take(50)
        wordsTuple.foreach(println)
    }

    def work3: Unit ={
        val rdd = context.textFile("D:\\A_1000feng\\Spark\\day04\\DataSource\\Advert.log")

        val records = rdd.map(lines => {
            val fields: Array[String] = lines.split('\t')
            val timestamp = fields(0)
            val province = fields(1)
            val city = fields(2)
            val userid = fields(3)
            val adid = fields(4)
            ((province, adid),1)
        }).reduceByKey((x,y)=>x+y)//((Jiangsu,8),2151) 每个省份每种广告的总数

        val result = records.map(x => (x._2, x._1)).sortByKey(false).map(x => (x._2._1, (x._2._2, x._1))).groupByKey().mapValues(_.take(3))
        result.foreach(println)
        context.stop()
    }

    def work4: Unit ={
        val fileRDD = context.textFile("D:\\A_1000feng\\Spark\\day04\\DataSource\\Advert.log")


    }

}
