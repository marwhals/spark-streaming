package part7_science_demo_project

import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import org.apache.spark.sql.{Dataset, SparkSession}

object ScienceSparkAggregator {


  val spark = SparkSession.builder()
    .appName("The Science project")
    .master("local[2]")
    .getOrCreate()

  import spark.implicits._

  case class UserResponse(sessionId: String, clickDuration: Long)

  case class UserAvgResponse(sessionId: String, avgDuration: Double)

  def readUserResponses(): Dataset[UserResponse] = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "science")
    .load()
    .select("value")
    .as[String]
    .map { line =>
      val tokens = line.split(",")
      val sessionId = tokens(0)
      val time = tokens(1).toLong

      UserResponse(sessionId, time)
    }

  def updateUserResponseTime
  (n: Int)
  (sessionId: String, group: Iterator[UserResponse], state: GroupState[List[UserResponse]]): Iterator[UserAvgResponse] = {
    group.flatMap { record =>
      val lastWindow =
        if (state.exists) state.get
        else List()

      val windowLength = lastWindow.length
      val newWindow =
        if (windowLength >= n) lastWindow.tail :+ record
        else lastWindow :+ record

      // for Spark to give us access to the state in the next batch
      state.update(newWindow)

      if (newWindow.length >= n) {
        val newAverage = newWindow.map(_.clickDuration).sum * 1.0 / n
        Iterator(UserAvgResponse(sessionId, newAverage))
      } else {
        Iterator()
      }
    }
  }

  def getAverageResponseTime(n: Int) = {
    readUserResponses()
      .groupByKey(_.sessionId)
      .flatMapGroupsWithState(OutputMode.Append, GroupStateTimeout.NoTimeout())(updateUserResponseTime(n))
      .writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  // aggregate the Rolling average response time over the past 10 clicks
  def logUserResponses() = {
    readUserResponses().writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    getAverageResponseTime(3)
  }
}
