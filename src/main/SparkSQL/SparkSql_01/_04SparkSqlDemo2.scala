package SparkSql_01

import org.apache.spark.sql.{Column, DataFrame, SparkSession}


object _04SparkSqlDemo2 {
    def main(args: Array[String]): Unit = {
//        DSLTest
        SqlTest
    }

    def DSLTest: Unit ={
        val session: SparkSession = SparkSession.builder().master("local").appName("test").getOrCreate()
        import session.implicits._
        /**
         * DSL Style:
         *      domain-specific language
         */
        val dataFrame: DataFrame = session.read.json("input/rating.json")
        dataFrame.where("uid=1").select($"movie",$"uid",$"rate")
          //          .select("uid")
          //          .select(new Column("rate"))

          .where("rate<6")
          .orderBy($"rate".desc)
          .show()
        //orderBy默认升序
    }

    def SqlTest: Unit ={
        /**
         * 需要为结果集dataFrame维护一张表
         */
        val session: SparkSession = SparkSession.builder().master("local").appName("test").getOrCreate()
        val dataFrame: DataFrame = session.read.json("file:///D:\\JavaInThinking\\IDEA\\sz2003_spark\\input/emp.json")

        /**
         * 全局临时视图是跨会话的。它的生存期是Spark应用程序的生存期，即应用程序终止时它将自动删除。
         * 它与系统保留的数据库global_temp相关联，我们必须使用限定名称来引用全局temp视图，
         * 例如SELECT * FROM global_temp.view1。
         *
         *  global:全局,整个spark程序中都可以访问到
         *          否则,仅当前任务可访问
         *  replace:如果存在就替换,不存在也会创建
         *  无replace:如果存在就报错,不存在就创建
         */
        dataFrame.createGlobalTempView("emp")
//        dataFrame.createOrReplaceGlobalTempView("emp")
        val sql1 = "select * from global_temp.emp"
        session.sql(sql1.stripMargin).show()
        session.newSession().sql(
            """
              |select * from global_temp.emp
              |where sal >= 1500
              |""".stripMargin)
          .show()

        dataFrame.createOrReplaceTempView("emp")
//        dataFrame.createTempView("emp")
        val sql2 = "select * from emp where sal < 1500"
        session.sql(sql2.stripMargin).show()
    }
}
