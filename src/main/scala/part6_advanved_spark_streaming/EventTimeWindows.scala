package part6_advanved_spark_streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json, sum, window}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, TimestampType}

/**
 * - Handle records by event time
 * - Learn window functions on Structured Streaming
 *
 * Event Time
 * - The moment when the record was generated
 * - Set by the data generation system
 * - Usually a column in the dataset
 * - Different from processing time = the time the record arrives in Spark
 *
 * Window Functions
 * - Aggregations on time-based groups
 *
 * - Essential concepts
 * --- Windows durations
 * --- Window sliding intervals
 * - As opposed to DStreams
 * --- Records are not necessarily taken between "now" and a certain past date
 * --- We can control output modes
 *
 *
 * - Assume we have a count by window, in complete mode
 * --- Batch time = 10 minutes
 * --- Window duration = 20 minutes
 * --- Window sliding interval = 10 minutes
 *
 * - Assume we have a count by window in append mode
 * --- batch time = 10 minutes
 * --- window duration = 20 minutes
 * --- window sliding interval = 10 minutes
 *
 * - Assume we have a count by window, in update mode
 * --- batch time = 10 minutes
 * --- window duration = 20 minutes
 * --- window sliding interval = 10 minutes
 *
 */

object EventTimeWindows {

  val spark = SparkSession.builder()
    .appName("Event Time Windows")
    .master("local[2]")
    .getOrCreate()

  val onlinePurchaseSchema = StructType(Array(
    StructField("id", StringType),
    StructField("time", TimestampType),
    StructField("item", StringType),
    StructField("quantity", IntegerType)
  ))

  def readPurchasesFromSocket() = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 12345)
    .load()
    .select(from_json(col("value"), onlinePurchaseSchema).as("purchase"))
    .selectExpr("purchase.*")

  def readPurchasesFromFile() = spark.readStream
    .schema(onlinePurchaseSchema)
    .json("src/main/resources/data/purchases")

  def aggregatePurchasesByTumblingWindow() = {
    val purchasesDF = readPurchasesFromSocket()

    val windowByDay = purchasesDF
      .groupBy(window(col("time"), "1 day").as("time")) // tumbling window: sliding duration == window duration
      .agg(sum("quantity").as("totalQuantity"))
      .select(
        col("time").getField("start").as("start"),
        col("time").getField("end").as("end"),
        col("totalQuantity")
      )

    windowByDay.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def aggregatePurchasesBySlidingWindow() = {
    val purchasesDF = readPurchasesFromSocket()

    val windowByDay = purchasesDF
      .groupBy(window(col("time"), "1 day", "1 hour").as("time")) // struct column: has fields {start, end}
      .agg(sum("quantity").as("totalQuantity"))
      .select(
        col("time").getField("start").as("start"),
        col("time").getField("end").as("end"),
        col("totalQuantity")
      )

    windowByDay.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  /**
   * TODO: Exercises
   *
   */

  def main(args: Array[String]): Unit = {
    aggregatePurchasesByTumblingWindow()
  }
}
