package SparkSql_02

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * wordcount出现数据倾斜的解决方案：
 * 局部聚合+全局
 */
object _08DataSkewDemo {
    private val session: SparkSession = SparkSession.builder().master("local").appName("test").getOrCreate()
    import session.implicits._
    def main(args: Array[String]): Unit = {
        val list = List("a a a a a a a a a a a a a a a a b b b c c c a a a a aa a a a","a a a a a b b a c a")
        val df: DataFrame = list.toDF("line")

        df.createTempView("tmp")
//        wordcountTest1
//        randSqlTest
//        partSql
//        fullSql
        session.stop()
    }

    def wordcountTest1: Unit ={
        val sql =
            """
              |select word,count(1)
              |from
              |(
              |select explode(split(line," "))as word
              |from tmp
              |)A
              |group by word
              |""".stripMargin
        session.sql(sql).show()
    }

    def randSqlTest: Unit ={
        val randSql=
            """
              |select A.word,concat( floor(rand()*4),"_",A.word ) as prefix_word
              |from
              |(
              |select explode(split(line," ")) as word
              |from tmp
              |)A
              |
              |""".stripMargin
        session.sql(randSql).show()
    }

    def partSql: Unit ={
        //将同一个key加上前缀，产生多个不同的key，
        val partSql=
            """
              |select B.prefix_word,count(1) as prefix_count
              |from
              |(
              |select A.word,concat( floor(rand()*4),"_",A.word ) as prefix_word
              |from
              |(
              |select explode(split(line," ")) as word
              |from tmp
              |)A
              |)B
              |group by B.prefix_word
              |""".stripMargin
        session.sql(partSql).show()
    }

    def fullSql: Unit ={
        //select substr(prefix_word,instr(prefix_word,"_")+1) as fword,sum(prefix_count)
        val fullSql=
            """
              |select substr(prefix_word,instr(prefix_word,"_")+1 )as fword,sum(prefix_count)
              |from
              |(
              |select B.prefix_word,count(1) as prefix_count
              |from
              |(
              |select A.word,concat( floor(rand()*4),"_",A.word ) as prefix_word
              |from
              |(
              |select explode(split(line," ")) as word
              |from tmp
              |)A
              |)B
              |group by B.prefix_word
              |)C
              |group by fword
              |""".stripMargin
        session.sql(fullSql).show()
    }
}
