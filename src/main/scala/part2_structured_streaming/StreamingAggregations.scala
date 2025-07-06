package part2_structured_streaming

import org.apache.spark.sql.functions.{col, mean}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

/**
 * - Aggregate data in micro-batches
 * - Structured Streaming aggregation API
 */

object StreamingAggregations {

  val spark = SparkSession.builder()
    .appName("Streaming Aggregations")
    .master("local[2]")
    .getOrCreate()

  def streamingCount() = {
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    val lineCount: DataFrame = lines.selectExpr("count(*) as lineCount")

    // aggregations with distinct are not supported, since this would require Spark to keep track of everything

    lineCount.writeStream
      .format("console")
      .outputMode("complete") // append and update not supported on aggregations without watermarks
      .start()
      .awaitTermination()
  }

  def numericalAggregations(aggFunction: Column => Column): Unit = {//Pass in your own custom function.......
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    // aggregates here
    val numbers = lines.select(col("value").cast("integer").as("number"))
    val aggregationDataFrame = numbers.select(aggFunction(col("number")).as("agg_so_far"))

    aggregationDataFrame.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def groupNames(): Unit = {
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    // counting occurrences of the "name" value
    val names = lines
      .select(col("value").as("name"))
      .groupBy(col("name")) // This will return a RelationalGroupedDataset
      .count()

    names.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def main(array: Array[String]): Unit = {
//    streamingCount()
    numericalAggregations(mean)
//    groupNames()
  }

  /**
   * Summary
   * - Same aggregation API as non-streaming DFs
   * - Keep in mind
   *  - Aggregations work at a micro batch level
   *  - The append output mode is not supported without watermarks
   *  - Some aggregations are no supported, e.g sorting or chained aggregations. This is by nature of streams. I.e unbounded data.
   */
}
