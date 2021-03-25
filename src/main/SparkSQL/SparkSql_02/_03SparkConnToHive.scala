package SparkSql_02

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * SparkSQL连接Hive,读写数据
 *
 * 执行语句时，改变了hive-metastore的VERSION，导致了在master上hive连接报错
 * Caused by: MetaException(message:
 * Hive Schema version 2.1.0 does not match metastore's schema version 1.2.0 Metastore is not upgraded or corrupt)
 * 在MySQL中：
 * update VERSION set SCHEMA_VERSION='2.1.1' where  VER_ID=1;
 */
object _03SparkConnToHive {
    private val session: SparkSession = SparkSession.builder()
      .master("local").appName("test")
      .config("hive.exec.dynamic.partition.mode","nonstrict")
      .enableHiveSupport().getOrCreate()

    def main(args: Array[String]): Unit = {
//        tableRead
//        queryTest
        sqlQueryTest
//        writeDFtoHive
    }

    def tableRead: Unit ={
        val df: DataFrame = session.table("sz2003.emp")
        df.show()
        session.stop()
    }

    def queryTest: Unit ={
        val df: DataFrame = session.table("sz2003.emp")
        val df1: DataFrame = df.groupBy("deptno").avg("sal")
        val df2: DataFrame = df.groupBy("deptno").max("sal")

        df.groupBy("deptno").agg("sal"->"max","sal"->"min").show()
    }

    def sqlQueryTest: Unit ={
        val df: DataFrame = session.table("sz2003.emp")
        df.createTempView("tmp")
        val sql =
            """
              |select deptno,
              |max(sal)
              |from tmp
              |group by deptno
              |order by deptno
              |""".stripMargin
        session.sql(sql).show()
        session.stop()
    }

    /**
     * 将DataFrame保存到hive的表中
     */
    def writeDFtoHive: Unit ={

        val df: DataFrame = session.table("sz2003.emp")
        df.write.mode(SaveMode.Append)
          .insertInto("sz2003.emp2")
        session.stop()
    }
}
