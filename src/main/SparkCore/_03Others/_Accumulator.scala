package _03Others

import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable


/**
 * IDEA中的local模式,底层使用的是spark-submit提交方式
 * 在local端开启了driver时，内置有一个worker
 */
object _01Accumulator {
    private val conf: SparkConf = new SparkConf().setMaster("local").setAppName("")
    private val context = new SparkContext(conf)

    def main(args: Array[String]): Unit = {
        Acc
    }

    def Acc: Unit ={
        val datas = context.makeRDD(1 to 10,2)
        //求数组的和
        var sum = 0 //sum是driver里面的变量
//        datas.foreach(x=>{
//            sum+=x      //算子在worker端的executor才执行，并没有将结果累加到driver端的sum
//        })

        datas.collect().foreach(x=>{
            sum+=x
        })
        println(sum)//0;    打印的是driver上的变量sum
    }
}

object _02Accumulator{
    private val conf: SparkConf = new SparkConf().setMaster("local").setAppName("")
    private val context = new SparkContext(conf)

    def main(args: Array[String]): Unit = {
        val datas: RDD[Int] = context.parallelize(1 to 10, 2)
        //创建累加器对象
        val myAcc = new LongAccumulator
        //在SparkContext注册累加器
        context.register(myAcc,"sum")

        datas.foreach(x=>{  //算子是在worker里的executor里执行的
            myAcc.add(x)    //调用累加器的add方法，进行累加值
        })
        println(myAcc.value)
    }
}

