package com.machinedoll.wonderland

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType

object SparkStreamingTwoLocalFSStreamJoinDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("TwoStreamJoin")
      .getOrCreate()

    import spark.implicits._

    val deviceSchema = new StructType().add("name", "string").add("value", "integer")

    val cpuStatusReader = spark.readStream.schema(deviceSchema).format("json").load("/tmp/local_fs_for_spark_streaming/devices/cpu-status*")
    val memStatusReader = spark.readStream.schema(deviceSchema).format("json").load("/tmp/local_fs_for_spark_streaming/devices/mem-status*")

    //TODO Multiple streaming aggregations (i.e. a chain of aggregations on a streaming DF) are not yet supported on streaming Datasets.
    val cpuQuery = cpuStatusReader
      .as[DeviceInfo]
      .withColumnRenamed("value", "cpu-value")
//      .groupBy("name")
//      .agg(sum("value").as("cpu-value"))
    //      .sum("value").alias("value")

    val memQuery = memStatusReader
      .as[MemInfo]
      .withColumnRenamed("value", "mem-value")
//      .groupBy("name")
//      .agg(sum("value").as("mem-value"))

    val sumQuery = cpuQuery
      .join(memQuery, "name")
      //TODO not support inner join with update mode
//      .groupBy("name")
//      .agg(sum("cpu-value") + sum("mem-value"))
      .writeStream
      .outputMode(OutputMode.Append())
      .format("console")
      .start()

    sumQuery.awaitTermination()
  }

  case class DeviceInfo(name: String, value: Int)

  case class MemInfo(name: String, value: Int)

}

