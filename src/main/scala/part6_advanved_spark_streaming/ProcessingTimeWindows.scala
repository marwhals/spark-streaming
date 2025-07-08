package part6_advanved_spark_streaming

import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{col, current_timestamp, sum, window}


/***
 * Window functions on structured streaming
 * - Handle records by processing time i.e the time they arrive at spark not the time they were created
 */

object ProcessingTimeWindows {

  val spark = SparkSession.builder()
    .appName("Processing Time Windows")
    .master("local[2]")
    .getOrCreate()

  def aggregateByProcessingTime() = {
    val linesCharCountByWindowDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12346)
      .load()
      .select(col("value"), current_timestamp().as("processingTime")) // This is essentially processing time. The time Spark has become aware of the record.
      .groupBy(window(col("processingTime"),  "10 seconds").as("window"))
      .agg(sum(functions.length(col("value"))).as("charCount")) // counting characters every 10 seconds by processing time
      .select(
        col("window").getField("start").as("start"),
        col("window").getField("end").as("end"),
        col("charCount")
      )

    linesCharCountByWindowDF.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    aggregateByProcessingTime()
  }

  /**
   * Summary
   * - See above for adding processing time to existing records
   * - Window processing
   *  - Spark doesn't care what the time-based column means
   *  - The only criterion is that it's a time or date column
   * Window duration and sliding interval must be a multiple of the batch interval
   * -> Output mode will influence the results
   */

}
