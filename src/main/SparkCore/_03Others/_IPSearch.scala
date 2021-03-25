package _03Others

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object _IPSearch {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("ipsearch").setMaster("local")
        val sc = new SparkContext(conf)

        //2.获取ip的字典数据
        val ipinfo = sc.textFile("input/ip.txt")
        //3.拆分数据
        val splitedIP: RDD[(String, String, String)] = ipinfo.map(line => {
            //数据是以|分割的
            val fields = line.split("\\|")
            //获取IP的起始和结束范围 取具体转换过后的值
            val startIP = fields(2)//起始
            val endIP = fields(3) //结束
            val shengfen = fields(6) // IP对应的省份
            (startIP, endIP, shengfen)
        })
        /**
         * 对于经常用到变量值,在分布式计算中,多个节点task一定会多次请求该变量
         * 在请求过程中一定会产生大量的网络IO,因此会影响一定的计算性能
         * 在这种情况下,可以使将该变量用于广播变量的方式广播到相对应的Executor端
         * 以后再使用该变量时就可以直接冲本机获取该值计算即可,可以提高计算数据
         */
        //在使用广播变量之前,需要将广播变量数据获取
        val arrIPInfo: Array[(String, String, String)] = splitedIP.collect
        //在包装成广播变量
        val broadcastIPInfo: Broadcast[Array[(String, String, String)]] = sc.broadcast(arrIPInfo)

        //获取用户数据
        val userInfo = sc.textFile("data/http.log")
        //切分用户数据并查找该用户属于哪个省份
        val shengfen: RDD[(String, Int)] = userInfo.map(line => {
            //数据是以 | 分隔的
            val fields = line.split("\\|")
            //获取用户ip地址 125.125.124.2
            val ip = fields(1)
            //将ip转成Long,目的是方便二分查找
            val ipToLong = ip2Long(ip)
            //获取广播变量中的数据
            val arrInfo: Array[(String, String, String)] = broadcastIPInfo.value
            //查找当前用ip的位置
            //线性查找(遍历数据注意对比)
            //二分查找(必须排序)
            //i值的获取有两种形式:
            //1.得到正确的下标,可以放心的去取值
            //2.得到了-1 没有找到
            //最好加一个判断若是 -1 写一句话 或是 直接结束
            val i: Int = binarySearch(arrInfo, ipToLong)
            val shengfen = arrInfo(i)._3
            (shengfen, 1)
        })
        //统计区域访问量
        val sumed = shengfen.reduceByKey(_ + _)
        //        //算子可以用来对数据库存储数据
        //        sumed.foreachPartition(data2MySql)
        //        println("数据库存储成功")
        //输出结果
        println(sumed.collect.toList)
        sc.stop()
    }

    /**
     * 把ip转换为long类型 直接给 125.125.124.2
     * @param ip
     * @return
     */
    def ip2Long(ip:String):Long = {
        val fragments = ip.split("[.]")
        var ipNum = 0L
        for( i <- 0 until fragments.length){
            //| 按位或 只要对应的二个二进位有一个为1时，结果位就为1 <<左位移
            ipNum = fragments(i).toLong | ipNum << 8L
        }
        ipNum
    }

    /**
     * 通过二分查找来查询IP对应的索引
     */
    def binarySearch(arr: Array[(String, String, String)], ip: Long): Int = {
        //开始和结束值
        var start = 0
        var end = arr.length - 1
        while (start <= end) {
            //求中间值
            val middle = (start + end) / 2
            //arr(middle)获取数据中的元组\
            //元组存储着ip开始 ip结束 省份
            //因为需要判断时候在ip的范围之内.,所以需要取出元组中的值
            //若这个条件满足就说明已经找到了ip
            if ((ip >= arr(middle)._1.toLong) && (ip <= arr(middle)._2.toLong)) {
                return middle
            }
            if (ip < arr(middle)._1.toLong) {
                end = middle - 1
            }
            if(ip>arr(middle)._2.toLong){
                start = middle + 1
            }
        }
        -1
    }
}


