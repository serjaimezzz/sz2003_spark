package _04StructureStreaming

import java.sql.{Connection, DriverManager,Statement}

import org.apache.spark.sql.{DataFrame, ForeachWriter, Row, SparkSession}

object _07Sink_MySql {
    private val session: SparkSession = SparkSession.builder().master("local[*]").appName("test").getOrCreate()
    import session.implicits._

    def main(args: Array[String]): Unit = {
        val frame: DataFrame = session.readStream.format("kafka")
          .option("kafka.bootstrap.servers","master:9092,slave1:9092,slave2:9092")
          .option("subscribe","pet")
          .load()

        val frame1 = frame.selectExpr("cast(value as string)").as[String]
        val frame2 = frame1.map(
            x=>{
                val arr: Array[String] = x.split("::")
                (arr(0).toInt,arr(1),arr(2))
            })
          .as[(Int,String,String)]
          .toDF("id","name","info")

        //保存到MySQL上
        frame2.writeStream
          .foreach(new MyWritwer)
          .start()
          .awaitTermination()
    }
}

class MyWritwer extends ForeachWriter[Row]{
    private val driver = "com.mysql.jdbc.Driver"
    private val url = "jdbc:mysql://localhost:3306/sz2003_db"
    private var connection:Connection = _
    private var statement:Statement = _

    //连接mysql
    @Override
    override def open(partitionId: Long, version: Long): Boolean = {
        Class.forName(driver)
        connection = DriverManager.getConnection(url, "root", "123456")
        statement = connection.createStatement()
        true
    }

    //处理方法，用于向数据库中插入数据
    @Override
    override def process(value: Row): Unit = {
        statement.executeUpdate(s"insert into movie values(${value.get(0)},'${value.get(1)}','${value.get(2)}')")
    }

    @Override
    override def close(errorOrNull: Throwable): Unit ={
        connection.close()
    }
}
