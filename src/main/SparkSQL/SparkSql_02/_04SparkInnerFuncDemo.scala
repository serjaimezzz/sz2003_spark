package SparkSql_02

import org.apache.spark.sql.{DataFrame, SparkSession}

object _04SparkInnerFuncDemo {
    private val session: SparkSession = SparkSession.builder().master("local").appName("test").getOrCreate()
    // 优先读取配置文件中的路径
    val df: DataFrame = session.read.json("file:///D:\\JavaInThinking\\IDEA\\sz2003_spark\\input/emp.json")

    import org.apache.spark.sql.functions._
    import session.implicits._

    def main(args: Array[String]): Unit = {
//        queryTest1
//        innerFuncTest1
        session.stop()
    }

    def queryTest1: Unit ={
        //查询每个部门，每个职位的最高工资，最高奖金，总人数
        df.groupBy("deptno","job")
          .agg("sal"->"max","comm"->"max","comm"->"count")
          .show()
    }

    def innerFuncTest1: Unit ={
        //使用spark的内置函数
        df.groupBy("deptno","job")
          .agg(max($"sal"),min('sal)as 'minSal)
          .sort(desc("deptno"),'minSal.asc)
          .show()
    }


}
