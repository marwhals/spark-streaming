package part2_structured_streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json}

object StreamingJoins {

  val spark = SparkSession.builder()
    .appName("Streaming Joins")
    .master("local[2]")
    .getOrCreate()

  val guitarPlayers = spark.read
    .option("inferSchema", true)
    .json("src/main/resources/data/guitarPlayers")

  val guitars = spark.read
    .option("inferSchema", true)
    .json("src/main/resources/data/guitars")

  val bands = spark.read
    .option("inferSchema", true)
    .json("src/main/resources/data/bands")

  // Joining static dataframes
  val joinCondition = guitarPlayers.col("band") === bands.col("id")
  val guitaristsBands = guitarPlayers.join(bands, joinCondition, "inner")
  val bandsSchema = bands.schema

  def joinStreamWithStatic() = {
    val streamedBandsDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load() // This is a  DataFrame with a single column "value" of type String
      .select(from_json(col("value"), bandsSchema).as("band"))
      .selectExpr("band.id as id", "band.name as name", "band.hometown as hometown", "band.year as year")

    // Joining a Streamed Data frame with a static data frame ----- join happens PER BATCH --- can be seen as lots of small joins happening between the static data frame and the streamed one.
    val streamedBandsGuitaristsDF = streamedBandsDF.join(guitarPlayers, guitarPlayers.col("band") === streamedBandsDF.col("id"), "inner")

    /**
      restricted joins: ---- Spark does not allow the unbounded accumulation of data
      - stream joining with static: RIGHT outer join/full outer join/right_semi not permitted
      - static joining with streaming: LEFT outer join/full/left_semi not permitted
     These are the restrictions which are enforced when you are joining a static data frame with a streamed data frame
     */

    streamedBandsGuitaristsDF.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  // since Spark 2.3 stream to stream joins have been supported
  def joinStreamWithStream() = {
    val streamedBandsDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load() // a DF with a single column "value" of type String
      .select(from_json(col("value"), bandsSchema).as("band"))
      .selectExpr("band.id as id", "band.name as name", "band.hometown as hometown", "band.year as year")

    val streamedGuitaristsDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12346)
      .load()
      .select(from_json(col("value"), guitarPlayers.schema).as("guitarPlayer"))
      .selectExpr("guitarPlayer.id as id", "guitarPlayer.name as name", "guitarPlayer.guitars as guitars", "guitarPlayer.band as band")

    // joining a stream with stream
    val streamedJoin = streamedBandsDF.join(streamedGuitaristsDF, streamedGuitaristsDF.col("band") === streamedBandsDF.col("id"))

    /**
      - inner joins are supported
      - left/right outer joins ARE supported, but MUST have watermarks
      - full outer joins are NOT supported
     */

    streamedJoin.writeStream
      .format("console")
      .outputMode("append") // only append supported for stream vs stream join
      .start()
      .awaitTermination()
  }


  def main(args: Array[String]): Unit = {
    joinStreamWithStatic()
  }

  /**
   * Summary
   * - Same join API as non-streaming DataFrames
   * - Some join types are not supported
   * -- RIGHT outer and RIGHT semi joins with static DFs
   * -- LEFT outer and LEFT semi if static DataFrame is joining against streaming DataFrames
   * -- Full outer joins
   *
   * - Stream-stream joins are supported
   * -- inner join optionally with a watermark
   * -- left/right out join ONLY with a watermark*
   */

}
