package part6_advanved_spark_streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}

import java.io.PrintStream
import java.net.ServerSocket
import java.sql.Timestamp
import scala.concurrent.duration._

/**
 * - Handle late data in time-based aggregations -- use with DataSender object to programmatically setup websocket or some service which represents the data sender
 *
 * Watermark
 * - How far back we still consider records before dropping them
 * ---> assume watermark of 10 minutes
 * ---> Say max time was 5:04 the watermark in the batch is 4:54. 4:01 for example ****will/may**** not be considered for this batch.
 * ---> Records before the watermark will be dropped when performing the computation.
 *
 * Summary
 * - With every batch, Spark will
 * ---> Update the max time ever recorded
 * ---> Update the watermark as (max time - watermark duration)
 * Guarantees
 * - In every batch, all records with time greater than the watermark will be considered
 * - If using window functions, a window will be updated until the watermark surpasses the window
 * No Guarantees
 * - Records whose time is less than the water mark *Will not necessarily be dropped*
 * Aggregations and joins in append mode need watermarks
 * - A watermark allows Spark to drop old records from state management
 *
 */

object Watermarks {

  // Reduce log level for Spark
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)
  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  Logger.getRootLogger.setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .appName("Late Data with Watermarks")
    .master("local[2]")
    .getOrCreate()

  // 3000,blue --- data being sent via a socket

  import spark.implicits._

  def debugQuery(query: StreamingQuery) = {
    // useful skill for debugging -- print output of spark job to console via another thread
    new Thread(() => {
      (1 to 100).foreach { i =>
        Thread.sleep(1000)
        val queryEventTime =
          if (query.lastProgress == null) "[]"
          else query.lastProgress.eventTime.toString

        println(s"$i: $queryEventTime")
      }
    }).start()
  }

  def testWatermark() = {
    val dataDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()
      .as[String]
      .map { line =>
        val tokens = line.split(",")
        val timestamp = new Timestamp(tokens(0).toLong)
        val data = tokens(1)

        (timestamp, data)
      }
      .toDF("created", "color")

    val watermarkedDF = dataDF
      .withWatermark("created", "2 seconds") // Adding a 2 second watermark to the DataFrame
      .groupBy(window(col("created"), "2 seconds"), col("color"))
      .count()
      .selectExpr("window.*", "color", "count")

    /**
      A 2 second watermark means
      -----> a window will only be considered until the watermark surpasses the window end
      - an element/a row/a record will only be considered if its is *after* the watermark
     */

    val query = watermarkedDF.writeStream
      .format("console")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime(2.seconds))
      .start()

    debugQuery(query)
    query.awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    testWatermark()
  }
}