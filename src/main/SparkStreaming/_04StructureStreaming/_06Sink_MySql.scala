package _04StructureStreaming

import java.sql.{Connection, DriverManager, PreparedStatement}
import org.apache.spark.sql.{DataFrame, ForeachWriter, Row, SparkSession}

/*测试数据：
1::Toy Story (1995)::Animation|Children's|Comedy
2::Jumanji (1995)::Adventure|Children's|Fantasy
3::Grumpier Old Men (1995)::Comedy|Romance
4::Waiting to Exhale (1995)::Comedy|Drama
5::Father of the Bride Part II (1995)::Comedy
 */
object _06Sink_MySql {
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
          .foreach(new MySqlWritwer)
          .start()
          .awaitTermination()
    }
}

class MySqlWritwer extends ForeachWriter[Row]{
    private var connection:Connection = _
    private var statement:PreparedStatement = _

    //连接mysql
    @Override
    override def open(partitionId: Long, version: Long): Boolean = {
        Class.forName("com.mysql.jdbc.Driver")
        connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/sz2003_db", "root", "123456")
        statement = connection.prepareStatement(s"insert into movie values(?,?,?)")
        true
    }

    //处理方法，用于向数据库中插入数据
    @Override
    override def process(value: Row): Unit = {
        //给问号赋值
        statement.setInt(1,value.getAs("id"))
        statement.setString(2,value.getAs("name"))
        statement.setString(3,value.get(2).toString)
        statement.execute()
    }

    @Override
    override def close(errorOrNull: Throwable): Unit ={
        connection.close()
    }
}