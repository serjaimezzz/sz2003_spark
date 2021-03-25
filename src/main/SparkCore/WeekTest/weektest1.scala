package WeekTest

import java.io.File

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

object weektest1 {
    val conf = new SparkConf().setMaster("local").setAppName("test")
    val context = new SparkContext(conf)

    def main(args: Array[String]): Unit = {
//        test1
//        test2
//        test3
        test4
        context.stop()
    }

    def test1: Unit = {
        val rdd = context.textFile("input/test.txt", 2)
        val records = rdd.map(lines => {
            val fields = lines.split("\t")
            val id = fields(0)
            val time = fields(1)
            val url = fields(2)
            (url, 1)
        })
        //        records.foreach(println)
        val resultRDD = records.reduceByKey((x, y) => {
            x + y
        }).sortByKey().take(3).foreach(println)
    }

    def test2: Unit = {
        val datas = context.textFile("input/wordscount")
//        datas.foreach(println)
        val wordsOrg = datas.flatMap(_.split(Array(',', ' ', '.', '#', '/', '<', '>', ':', '[', ']', '(', ')', '-', '"', '+', '`')))

        val words = wordsOrg.filter(!_.isEmpty)
//        words.foreach(println)

        val sparkTimes = words.filter(_.equals("Spark")).map((_,1)).reduceByKey(_+_)
        sparkTimes.foreach(println)

        val wordsTuple = words.map((_, 1)).reduceByKey(_ + _).map(x => {
            (x._2, x._1)
        }).sortByKey(false).take(1)
        wordsTuple.foreach(println)
    }

    def test3: Unit ={
        val datas = context.textFile("input/peopleinfo.txt")
        val records = datas.map(lines=>{
            val fields = lines.split("    ")
            val id = fields(0)
            val gender = fields(1)
            val height = fields(2)
            (gender,height)
        })

//        records.foreach(println)
        //总人数
        val totals = records.map(x=>{
            (x._1,1)
        }).reduceByKey(_+_)

//        records.map((x)=>(x._2,x._1)).sortByKey().take(1).foreach(println)
//        records.map((x)=>(x._2,x._1)).sortByKey(false).take(1).foreach(println)
        //身高信息
        val hInfo = records.groupByKey().mapValues(x => {
            (x.max, x.min)
        })

        val result = totals.join(hInfo)
        result.foreach(println)
    }

    def test4: Unit ={
        val datas = context.textFile("input/stock.csv")
        val records = datas.map(lines=>{
            val fields = lines.split(",")
            val sid = fields(0)
            val time = fields(1)
            val price = fields(2)
            (sid,(time,price))
        })

//        records.groupByKey().mapValues((x)=>{(x.max,x.min)}).foreach(println)

//        val result= records.groupByKey().mapValues(x=>{
//            ((x.maxBy(x=>x._2)), (x.minBy(x=>x._2)))
//        })
//        result.foreach(println)


    }

}

















