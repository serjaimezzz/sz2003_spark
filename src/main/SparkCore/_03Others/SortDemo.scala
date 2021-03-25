package _03Others

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SortDemo01 {
    //使用sortBy或sortByKey算子
    private val conf = new SparkConf().setMaster("local").setAppName("test")
    private val context = new SparkContext(conf)
    def main(args: Array[String]): Unit = {
        val stu = List(("zhangsan",23),("lisi",22),("ww",25))
        val list = context.parallelize(stu)
        //按照年龄降序
        list.sortBy(x=>{x._2},false)
        //按照默认的key进行排序:字典序
        list.sortByKey(false).foreach(println)
    }
}

object SortDemo02{
    private val conf = new SparkConf().setMaster("local").setAppName("test")
    private val context = new SparkContext(conf)
    def main(args: Array[String]): Unit = {
        val stu = Array(Students(1,"zs",25),Students(2,"ls",23),Students(3,"ww",25))
        val datas: RDD[Students] = context.parallelize(stu)
        val value: RDD[Students] = datas.sortBy(x => x)
        value.foreach(println)
    }
}

case class Students(sid:Int,name:String,age:Int) extends Ordered[Students]{

    override def compare(that: Students): Int = {
        //年龄升序
        var rs = this.age - that.age
        if (rs == 0){
            //学号降序
            rs = that.sid - this.sid
        }
        rs
    }
}