package com.machinedoll.wonderland

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types.StructType

object SparkStreamingFromLocalFSQuickStart {
  /*
    local source dir: /tmp/local_fs_for_spark_streaming/dev-1/status.json
   */
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("SparkStreamingFromLocalFS")
      .getOrCreate()

    import spark.implicits._

    val deviceSchema = new StructType().add("name", "string").add("value", "integer")

    val jsonSourceReader = spark
      .readStream
      .schema(deviceSchema)
      .format("json")
      .load("/tmp/local_fs_for_spark_streaming/devices/status*")
      //TODO spark does not support nested folder files lookup
//      .json("/tmp/local_fs_for_spark_streaming/dev-1")


    val query = jsonSourceReader
      .as[DeviceInfo]
      .groupBy("name")
      .sum("value")
      .writeStream
      .trigger(Trigger.ProcessingTime("1 second"))
      .outputMode(OutputMode.Update())
      .format("console")
      .start()

    query.awaitTermination()
  }

  case class DeviceInfo(name: String, value: String)

}

