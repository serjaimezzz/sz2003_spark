package _03Others

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable


object _03CustomerAcc{
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("test").setMaster("local")
        val sc = new SparkContext(conf)
        val arr: RDD[Int] = sc.parallelize(1 to 10,2)
        //创建自定义的累加器对象
        val sumAcc = new SumAccumulator
        //注册累加器对象
        sc.register(sumAcc,"sum")
//        arr.foreach(println)
        arr.foreach(sumAcc.add(_))

        //打印累加器的值
        println(sumAcc.value)
    }
}

/**
 * 自定义累加器,用于实现求和逻辑
 * 1.extends AccV2
 * 2.规定泛型,[input,ouput]
 * 3.
 */
class SumAccumulator extends AccumulatorV2[Long,Long]{
    //定义一个成员变量,用于存储累加后的结果
    var sum:Long = 0
    //判断累加器的值是否为空,true为空
    override def isZero: Boolean = {sum == 0}

    /**
     * 复制累加器对象到worker上，也就是创建一个新的累加器
     * @return
     */
    override def copy(): AccumulatorV2[Long, Long] = {
        var other = new SumAccumulator
        //将当前累加器的对象的值赋值到新的累加器上
        other.sum = this.sum
        other
    }

    /**
     * 重置累加器
     */
    override def reset(): Unit = {
        sum = 0
    }

    /**
     * 将要累加的数据累加到累加器的值上
     * @param v
     */
    override def add(v: Long): Unit = {
        sum += v
    }

    /**
     * 用于两两合并累加器的值，指的是this和参数对象，累加到this上
     * @param other
     */
    override def merge(other: AccumulatorV2[Long, Long]): Unit = {
        sum+=other.value
    }

    /**
     * 获取值
     * @return
     */
    override def value: Long = sum
}


