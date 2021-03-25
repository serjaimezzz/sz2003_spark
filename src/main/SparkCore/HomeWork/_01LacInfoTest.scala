package HomeWork

import org.apache.spark.{SparkConf, SparkContext}

object _01LacInfoTest {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local").setAppName("test")
        val sc = new SparkContext(conf)

        val lines = sc.textFile("")

        val usetInfo = lines.map(line=>{
            //解析每一行
            val arr = line.split(",")
            //获取手机号
            val phoneNum = arr(0)
            //获取时间
            val time = arr(1).toLong
            //获取状态信息
            val status = arr(3)
            //获取基站ID
            val lacid = arr(2)

            //为了方便计算求和，将连接时的时间转为负数，将时间转换如下
            val newTime = if(status .equals("1")) -time else time

            //设置返回值
            ((phoneNum,time),lacid)
        })
        //第三步：将用户的同一个基站的时间进行累加计算
        val sumed = usetInfo.reduceByKey(_ + _)
//        sumed.foreach(println)

        //第四步：加载基站信息
        val laclines = sc.textFile("")
        //第五步：解析lines，封装成基站对象
        val lacInfo= laclines.map(line=>{
            val arr = line.split(",")
            val lacid = arr(0)
            val x = arr(1)
            val y = arr(2)
            //封装成元组
            (lacid,(x,y))
        })

        //第六步：为了和基站信息做join运算，需要将userInfo重新封装
        val newUser = sumed.map(user=>{
            val lacid = user._1._2
            val phone = user._1._1
            val totalTime = user._2
            (lacid,(phone,totalTime))
        })

        //第七步：join
//        val joined = newUser.leftOuterJoin(lacInfo)

        //第八步：
//        joined
    }
}
