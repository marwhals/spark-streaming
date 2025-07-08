package part3_low_level_streaming_with_DStreams

import common.Stock
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.io.{File, FileWriter}
import java.sql.Date
import java.text.SimpleDateFormat

/**
 * - DStreams - Discretized Streams
 *
 * - What are Discretized Streams?
 *    - Never ending sequence of RDDs
 *      - Nodes clocks are synchronised
 *      - Batches are triggered at the same time in the cluster
 *      - Each batch is an RDD
 *
 * TODO - Create Diagram
 *    - Essentially a distributed collection of elements of the same type
 *      - Functional operators e.g: map, flatMap, filter, reduce.
 *      - Accessors to each RDD
 *        ---> Mode advanced operators exist
 *
 *    - Needs a receiver to perform computations
 *      - One reciever per DStream
 *      - Fetches data from the source, sends to Spark, creates blocks.
 *      - Is managed by the StreamingContext on the driver
 *      - Occupies one core on the machine
 *
 */

object DStreams {

  val spark = SparkSession.builder()
    .appName("DStreams")
    .master("local[2]")
    .getOrCreate()

  /**
   * Spark Streaming context
   * - needs the spark context above
   * - a duration = batch interval
   */
  val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

  /**
   * Structure of an application with DStreams:
   * - define input sources by creating DStreams
   * - define transformations of DStreams
   * - call an action on DStreams
   * - start ALL computations with ssc.start()
   *  - no more computations can be added
   *    - await termination, or stop the computation
   *  - you cannot restart a computation
   *
   *
   */

  def readFromSocket() = {
    val socketStream: DStream[String] = ssc.socketTextStream("localhost", 12345)
    // transformations are lazy
    val wordsStream: DStream[String] = socketStream.flatMap(line => line.split(" "))

    // actions
//    wordsStream.print()

    wordsStream.saveAsTextFiles("src/main/resources/data/words/") // each folder = RDD = batch, each file = a partition of the RDD

    ssc.start()
    ssc.awaitTermination()
//    ssc.awaitTerminationOrTimeout() <--- will be useful
  }

  def createNewFile() = {
    new Thread(() => {
      Thread.sleep(5000)

      val path = "src/main/resources/data/stocks"
      val dir = new File(path) // directory where I will store a new file
      val nFiles = dir.listFiles().length
      val newFile = new File(s"$path/newStocks$nFiles.csv")
      newFile.createNewFile()

      val writer = new FileWriter(newFile)
      writer.write(
        """
          |AAPL,Sep 1 2000,12.88
          |AAPL,Oct 1 2000,9.78
          |AAPL,Nov 1 2000,8.25
          |AAPL,Dec 1 2000,7.44
          |AAPL,Jan 1 2001,10.81
          |AAPL,Feb 1 2001,9.12
        """.stripMargin.trim)

      writer.close()
    }).start()
  }

  def readFromFile() = {
    createNewFile() // operates on another thread

    // defined DStream
    val stocksFilePath = "src/main/resources/data/stocks"

    /**
      - ssc.textFileStream monitors a directory for *new files* -- this is very useful for monitoring distributed file systems like HDFS or S3
     */
    val textStream = ssc.textFileStream(stocksFilePath)

    //transformations
    val dateFormat = new SimpleDateFormat("MMM d yyyy")


    val stocksStream: DStream[Stock] = textStream.map { line =>
      val tokens = line.split(",")
      val company = tokens(0)
      val date = new Date(dateFormat.parse(tokens(1)).getTime)
      val price = tokens(1).toDouble

      Stock(company, date, price)

    }

    // action
    stocksStream.print()

    // start the computations
    ssc.start()
    ssc.awaitTermination()

  }


  def main(args: Array[String]): Unit = {
//    readFromSocket()
    readFromFile()
  }

  /**
   * - Summary
   * -- DStreams are a never-ending sequence of RDDs
   * -- Available under a StreamingContext
   * -- Support various transformations - i.e Scala methods like map, flatMap etc
   * -- Computation started via an action + start of the streaming context.....(compare to terminal operators in Java 8+)
   *
   */

}
