package SparkSql_02

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object _HomeWork {
    private val session: SparkSession = SparkSession.builder().master("local").appName("test").getOrCreate()
    import session.implicits._
    import org.apache.spark.sql.functions._
    def main(args: Array[String]): Unit = {
//        work1
//        work7_1
        work7_2
//        work8_1
//        work8_2
    }

    //1.查询每只基金在指定时间段内（20120301~20120315）赎回手续费最高的销售机构代码
    def work1: Unit ={
        val df: DataFrame = session.read.option("sep",",").option("header",true).csv("file:///D:\\JavaInThinking\\IDEA\\sz2003_spark\\input/jsz95.csv")
//        df.show()
        df.createTempView("tmp")
        session.sql(
            """
              |
              |""".stripMargin
        ).show()
        session.stop()
    }

    def work7_1: Unit ={
        val df: DataFrame = session.read.json("file:///D:\\JavaInThinking\\IDEA\\sz2003_spark\\input\\student7.json")
        df.show()
        df.createTempView("tmp")
        val frame1: DataFrame = session.sql(
            """
              |select student,
              |        sum(case when subject='Chinese' then score end)`Chinese`,
              |        sum(case when subject='Math' then score end)`Math`,
              |        sum(case when subject='English' then score end)`English`
              |from tmp
              |group by student
              |""".stripMargin
        )
        frame1.show()

    }

    def work7_2: Unit ={
        val df: DataFrame = session.read.json("file:///D:\\JavaInThinking\\IDEA\\sz2003_spark\\input\\student7.json")
        df.show()
        /**
         *      行转列
         */
        val frame: DataFrame = df.groupBy("student").pivot("subject").sum("score")
        frame.show()

        /**
         *      列转行
         */
        frame.createTempView("stu")
        val frame1: DataFrame = session.sql(
            """
              |select student,stack(3,'Chinese',`Chinese`,'Math',`Math`,'English',`English`)as(`subject`,`score`)
              |from stu
              |""".stripMargin)
        frame1.show()
    }


    /**
     *  8.1	每个部门中工资低于本部门平均工资的人
     */
    def work8_1: Unit ={
        val df1: DataFrame = session.read.option("sep", "\t").option("header", true)
          .csv("file:///D:\\JavaInThinking\\IDEA\\sz2003_spark\\input/EMP.csv")
        df1.createTempView("emp")
        val frame: DataFrame = session.sql(
            """
              |select ENAME,DEPTNO,SAL,AVG(SAL)over(distribute by DEPTNO) as `avg_sal`
              |from emp
              |having SAL< `avg_sal`
              |""".stripMargin
        )
        //        frame.show()

        val frame1: DataFrame = df1.select($"ename", $"deptno", $"sal", avg("sal") over (Window.partitionBy("deptno")) as ("avg_sal"))
        frame1.show()
        val value: Dataset[Row] = frame1.where("sal<avg_sal")
        //        value.show()
    }

    /**
     *  8.2	直属领导下大于等于2名员工的人
     */
    def work8_2: Unit ={
        val df1: DataFrame = session.read.option("sep", "\t").option("header", true).csv("file:///D:\\JavaInThinking\\IDEA\\sz2003_spark\\input/EMP.csv")
        //        df1.show()
        df1.createTempView("emp")
        val f2 = session.sql(//B.name为直属领导
            """
              |select *,count(B.empno)over(distribute by B.empno)`num`
              |from emp as A join emp as B
              |where A.mgr = B.empno
              |having num>=2
              |""".stripMargin)
        //        f2.show()

//        val frame2: DataFrame = df1.crossJoin(df1)//笛卡尔积
        val frame2: DataFrame = {
            df1.as("A").join(df1.as("B")).where("A.mgr=B.empno")
              .select($"B.ename", count("B.empno") over (Window.partitionBy("B.empno"))as("empNum") )
              .where("empNum>=2")

        }
        frame2.show()
    }

}
