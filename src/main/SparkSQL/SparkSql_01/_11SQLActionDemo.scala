package SparkSql_01

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._


object _11SQLActionDemo {
    val session: SparkSession = SparkSession.builder().appName("test").master("local").getOrCreate()
    import session.implicits._
    val dataFrame: DataFrame = session.read.json("input/emp.json")

    /**
     * 需要导入的包
     * import org.apache.spark.sql.functions._
     */
    def main(args: Array[String]): Unit = {
//        selectTest
//        filterTest
//        selectExprTest
//        selectColTest
//        sortWithinPartitionsTest
//        cubeTest


    }

    def selectTest: Unit ={
        //写法1：
        dataFrame.select("empno","ename","job","sal","deptno").where($"deptno=10").show()

        //写法2：
        dataFrame.select(dataFrame("empno"),dataFrame("ename"),dataFrame("deptno")).where($"deptno=10").show()
    }

    def filterTest: Unit ={
        val value: Dataset[Row] = dataFrame.filter("deptno = 10 or sal >3000")
        value.show()
    }

    def selectExprTest: Unit ={
        val df: DataFrame = dataFrame.selectExpr("ename","empno as id","mgr as leader")
        df.show()
    }

    def selectColTest: Unit ={
        val df: DataFrame = dataFrame.select(col("ename"))
        df.show()
    }

    def sortWithinPartitionsTest: Unit ={
        /**
         * 重新分区并按照指定字段排序
         */
        val datas: Dataset[Row] = dataFrame.repartition(2)
        val value: Dataset[Row] = datas.sortWithinPartitions("sal")
        value.show()
    }

    def cubeTest: Unit ={
        /**
         * 对cube中的字段进行求和
         * 为指定表达式集的每个可能组合创建分组集。
         * 首先会对(A、B、C)进行group by，然后依次是(A、B)，(A、C)，(A)，(B、C)，(B)，( C)，
         * 最后对全表进行group by操作。
         */
        val df: DataFrame = dataFrame.cube("deptno").sum("sal")
        df.show()
    }
}
