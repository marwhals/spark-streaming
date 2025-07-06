package part2_structured_streaming

import common.stocksSchema
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

import scala.concurrent.duration.DurationInt

object StreamingDataFrames {

  val spark = SparkSession.builder()
    .appName("Our first streams")
    .master("local[2]")
    .getOrCreate()

  def readFromSocket() = {
    val lines: DataFrame = spark.readStream //Reading a DataFrame
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    val shortLines: DataFrame = lines.filter(functions.length(col("value")) <= 5) // Transformation

    // how to tell if a DataFrame is a streaming one or a static one
    println(shortLines.isStreaming)

    val query = shortLines.writeStream // Consuming a DataFrame
      .format("console")
      .outputMode("append")
      .start() //Start a streaming query. This query will need be started and waited for. The start command is asynchronous

    //Wait for the stream to finish
    query.awaitTermination()
    // Code structure is very similar to static DataFrames
  }

  def readFromFiles() = {
    val stocksDF: DataFrame = spark.readStream
      .format("csv")
      .option("header", "false")
      .option("dateFormat", "MMM d yyyy")
      .schema(stocksSchema)
      .load("src/main/resources/data/stocks")

    stocksDF.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  def demoTriggers() = {
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    // Write the lines DataFrame at a certain trigger
    lines.writeStream
      .format("console")
      .outputMode("append")
      .trigger(
        // Trigger.ProcessingTime(2.seconds) // every 2 seconds run the query. I.e check the data frame for new data
        // Trigger.Once() // Single batch of the DataFrame and then terminate
        Trigger.Continuous(2.seconds) // Experimental - every 2 seconds create a batch with whatever you have. This will run regardless of whether there is data or not.
      )
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
//    readFromSocket()
//    readFromFiles()
    demoTriggers()
  }

  /**
   * Summary
   * - Streaming DFs can be read via the Spark session
   * - Streaming DFs have identical API to non-streaming DFs
   * - Streaming DFs can be written via a call to start
   */

}