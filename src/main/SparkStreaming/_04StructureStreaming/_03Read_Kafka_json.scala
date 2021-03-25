package _04StructureStreaming

import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object _03Read_Kafka_json {
    private val session: SparkSession = SparkSession.builder().master("local[*]").appName("test").getOrCreate()
    import session.implicits._
    import org.apache.spark.sql.functions._
    session.sparkContext.setLogLevel("ERROR")

    def main(args: Array[String]): Unit = {
        val frame: DataFrame = session.readStream.format("kafka")
          .option("kafka.bootstrap.servers","master:9092,slave1:9092,slave2:9092")
          .option("subscribe","pet")
          .load()

        val last_event = new StructType()
          .add("has_sound",DataTypes.BooleanType)
          .add("has_motion",DataTypes.BooleanType)
          .add("has_person",DataTypes.BooleanType)
          .add("start_time",DataTypes.DateType)
          .add("end_time",DataTypes.DateType)

        val cameras = new StructType()
          .add("device_id",DataTypes.StringType)
          .add("last_event",last_event)

        val devices = new StructType().add("cameras",cameras)
        val schema = new StructType().add("devices",devices)

        val jsonOptions = Map("timestampFormat"->"yyyy-MM-dd'T'HH:mm:ss.sss")

        val frame1 = frame.selectExpr("cast(value as string)")
            .select(from_json('value,schema,jsonOptions).as("value"))

        //查询has_person,start_time,end_time
        val frame2 = frame1.selectExpr(
            "value.devices.cameras.last_event.has_person",
            "value.devices.cameras.last_event.start_time",
            "value.devices.cameras.last_event.end_time")
          .filter($"has_person" === true)
          .groupBy($"has_person",$"start_time")
          .count()

        frame2.writeStream.outputMode(OutputMode.Update())
          .format("console")
          .start()
          .awaitTermination()

    }
}

/*
  val item_id = StructField("item_id",DataTypes.StringType)

        val display = StructField("display",DataTypes.IntegerType)
        val box = StructField("box",DataTypes.StringType)
        val viewpoint = StructField("viewpoint",DataTypes.IntegerType)
        val label = StructField("label",DataTypes.StringType)
        val instance_id = StructField("instance_id",DataTypes.IntegerType)
        val annotations = new StructType(Array(display,box,viewpoint,label,instance_id))

        val img_name = StructField("img_name",DataTypes.StringType)
 */

/**
  {
  "devices": {
       "cameras": {
           "device_id": "awJo6rH",
           "last_event": {
               "has_sound": true,
               "has_motion": true,
               "has_person": true,
               "start_time": "2016-12-29T00:00:00.000Z",
               "end_time": "2016-12-29T18:42:00.000Z"
             }
         }
    }
  }
 */