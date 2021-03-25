package _03Others

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable


class WordCountAcc extends AccumulatorV2[String,mutable.HashMap[String,Int]]{
    //成员变量的维护
    var map = new mutable.HashMap[String, Int]()

    override def isZero: Boolean = {map.isEmpty}

    /**
     * 复制的是累加器而不是成员变量
     * 但是心的累加器应该是this这个累加器的成员变量对象的值
     * @return
     */
    override def copy(): AccumulatorV2[String, mutable.HashMap[String, Int]] = {
        val newAcc = new WordCountAcc
        newAcc.map = this.map
        newAcc
    }

    override def reset(): Unit = {map.clear()}

    override def add(v: String): Unit = {
        map.get(v) match {
            case None => map.put(v,1)
            case Some(x) => map.put(v,x+1)
            /**
             * map模式匹配只有两种结果，要么是None，要么是Some(value)
             */
        }
    }

    override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Int]]): Unit = {
        /**
         * 两个累加器
         */
        for (elem <- other.value) {
            //表示的是other里的每一个单词的KV对
            //查看this的map中是否有other里的这个单词
            map.get(elem._1)match {
                case Some(x) => { map.put(elem._1,x+elem._2)}
                case None =>map.put(elem._1,elem._2)
            }
        }
    }

    override def value: mutable.HashMap[String, Int] = {
        map
    }
}