package com.machinedoll.wonderland

import org.apache.spark.sql.SparkSession

object SparkStreamingFromConsoleQuickStart {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[1]")
      .appName("StructuredNetworkWordCount")
      .getOrCreate()

    import spark.implicits._


    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    // Split the lines into words
    val words = lines.as[String].flatMap(_.split(" "))

    // Generate running word count
    val wordCounts = words.groupBy("value").count()

    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()
  }
}
