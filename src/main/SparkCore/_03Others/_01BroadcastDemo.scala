package _03Others

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}

object _01BroadcastDemo {
    val conf = new SparkConf().setMaster("local").setAppName("test")
    val context = new SparkContext(conf)

    def main(args: Array[String]): Unit = {
        BroadcastDemo
//        noBroadcast
        context.stop()
    }

    def BroadcastDemo: Unit ={
        //定义要查询的敏感单词的字典
        val list = List("static","class","val","var","public")
        //定义将要传输的普通的局部变量，包装成广播变量
        val listBroadcast: Broadcast[List[String]] =context.broadcast(list)
        //读一个文件,然后查看是否有敏感单词
        val datas = context.textFile("file:///D:\\JavaInThinking\\IDEA\\sz2003_spark\\input")
        val words = datas.flatMap(_.split(" "))
        //判断是否包含:用listBroadcast.value获取广播变量的值
        /**
         * 在算子中使用广播变量的值。        语法：广播变量名.value
         *  因为算子 用的是广播变量名.value这个语法，因此知道是广播变量，所以会去blockManager里获取
         *  如果没有，再去driver或者就近的executor里获取
         */
        val rs = words.filter(listBroadcast.value.contains(_)).distinct()
        rs.foreach(println)
    }
    /**
     * 没有广播变量
     */
    def noBroadcast: Unit = {
        //定义一个要查询的敏感单词的字典,假如10M
        val list = List("static","class","val","var","public")
        //读一个文件,然后查看是否有敏感单词
        val datas = context.textFile("file:///D:\\JavaInThinking\\IDEA\\sz2003_spark\\input")
        val words = datas.flatMap(_.split(" "))
        //判断是否包含
        /**
         *  filter是在worker端执行的，每一个task中都会调用一次，
         *  因此list这个集合，就会传输很多次。
         *  有多少个task，就传输多少次，非常占网络IO。性能很低
         */
        val rs = words.filter(list.contains(_)).distinct()
        rs.foreach(println)
    }
}
