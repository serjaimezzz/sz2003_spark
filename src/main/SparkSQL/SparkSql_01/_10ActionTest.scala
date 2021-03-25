package SparkSql_01

import java.util

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object _10ActionTest {
    val spark: SparkSession = SparkSession.builder().appName("test").master("local").getOrCreate()
    val dataFrame: DataFrame = spark.read.json("input/emp.json")

    def main(args: Array[String]): Unit = {
        /**
         * 字段按Unicode编码(字典序)顺序排列comm|deptno|empno|ename...
         */
        dataFrame.show()
//        collectTest
//        dataFrame.collect().foreach(println)
//       collectAsListTest
//        describeTest
//        firstTest
//        headTest
//        takeTest
//        takeAsListTest
    }

    def collectTest: Unit ={
        val rows: Array[Row] = dataFrame.collect()
        rows.foreach(println)
    }

    def collectAsListTest: Unit ={
        val rows: util.List[Row] = dataFrame.collectAsList()
        println(rows)
    }

    def describeTest: Unit ={
        /**
         * 统计每一个字段的count(fields)、平均值mean()、标准差stddev和最值
         */
        val df: DataFrame = dataFrame.describe()
        df.show()
    }

    def firstTest: Unit ={
        //取第一条记录并打印
        val row: Row = dataFrame.first()
        println(row)
    }

    def headTest: Unit ={
        val row: Row = dataFrame.head()
        println(row)
    }

    def takeTest: Unit ={
        val rows: Array[Row] = dataFrame.take(3)
        rows.foreach(println)
    }

    def takeAsListTest: Unit ={
        val list: util.List[Row] = dataFrame.takeAsList(3)
        println(list)
    }



}
