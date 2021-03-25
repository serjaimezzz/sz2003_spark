package _03Others

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


case class Girl(faceValue:Int,height:Int){}
object Girl {
    //隐式类转换
    implicit val a =new Ordering[Girl](){
        override def compare(x: Girl, y: Girl) = {
            //颜值降序
            var rs = y.faceValue - x.faceValue
            if (rs == 0){
                //身高升序
                rs = x.height - y.height
            }
            rs
        }
    }
}

object _CustomSortDemo{
    private val conf = new SparkConf().setMaster("local").setAppName("test")
    private val context = new SparkContext(conf)
    def main(args: Array[String]): Unit = {
        val arr = Array(Girl(8,166),Girl(8,160),Girl(10,170))
        val datas: RDD[Girl] = context.makeRDD(arr)
        import Girl._
        datas.sortBy(x=>x).foreach(println)
    }
}