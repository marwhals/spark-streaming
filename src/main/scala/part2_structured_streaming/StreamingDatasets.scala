package part2_structured_streaming

import common.{Car, carsSchema}
import org.apache.spark.sql.functions.{avg, col, from_json}
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}

/**
 * - Type safe structured streams
 */
object StreamingDatasets {

  val spark = SparkSession.builder()
    .appName("Streaming Datasets")
    .master("local[2]")
    .getOrCreate()

  // include encoders for DF -> DS transformations
  // Required for turning rows into case class instances
  import spark.implicits._

  def readCars(): Dataset[Car] = {
    // useful for DataFrame -> DataSet transformations
    val carEncoder = Encoders.product[Car] // See docs on product method for more info

    spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load() // DF with single string column "value"
      .select(from_json(col("value"), carsSchema).as("car")) // composite column (struct)
      .selectExpr("car.*") // DF with multiple columns
      .as[Car](carEncoder) // encoder can be passed implicitly with spark.implicits -- see above
  }

  def showCarNames() = {
    val carsDS: Dataset[Car] = readCars()

    // transformations here
    val carNamesDF: DataFrame = carsDS.select(col("Name")) // DataFrame - no type information

    // collection transformations maintain type info
    val carNamesAlt: Dataset[String] = carsDS.map(_.Name)

    carNamesAlt.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()

//    Keep. Useful for writing to text file
//    carNamesAlt.writeStream
//      .format("text")
//      .option("path", "output/car_names")
//      .option("checkpointLocation", "checkpoint/car_names") // Required for fault tolerance
//      .outputMode("append")
//      .start()
//      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
//    ex1()
//    ex2()
    ex3()
  }

  /**
   * Exercises
   * 1) Count how many powerful cars we have in the DS (HP > 140)
   * 2) Average HP for the entire dataset (use the complete output mode)
   * 3) Count the cars by origin
   */

  def ex1() = {
    val carsDS = readCars()
    carsDS.filter(_.Horsepower.getOrElse(0L) > 140)
      .writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  def ex2() = {
    val carsDS = readCars()
    carsDS.select(avg(col("Horsepower")))
      .writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def ex3() = {
    val carsDS = readCars()

    val carCountByOrigin = carsDS.groupBy(col("Origin")).count() // option 1

    val carCountByOriginAlt = carsDS.groupByKey(car => car.Origin).count() // option 2 with the Dataset API

    carCountByOriginAlt
      .writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  /**
   * Summary
   * - Same DS conversion as non-streaming DS
   * - Streaming DSs support functional operators
   * - Tradeoffs
   *  - Pros: type safety and expressiveness --- i.e can use Scala features such as flatMap, for comprehensions etc
   *  - Cons: Potential performance implications as lambdas cannot be optimised by Spark
   */

}
