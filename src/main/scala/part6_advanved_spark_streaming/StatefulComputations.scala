package part6_advanved_spark_streaming

import org.apache.spark.sql.functions.{col, sum}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout}
import utils.utils.ignoreLogs


/**
 * - Aggregate data in an arbitrary way
 * - Manage stream data "manually"
 *
 * Scenario
 * - Running a social network and people share updates
 * - Your social network needs to store everything
 * - You want to compute the average storage used by every type of post
 * ----> Average storage = total storage / total count, grouped by post type
 *
 * Note: There is also flatMapWithState when a group produces multiple outputs
 */

object StatefulComputations {

  ignoreLogs

  val spark = SparkSession.builder()
    .appName("Stateful Computation")
    .master("local[2]")
    .getOrCreate()

  import spark.implicits._

  case class SocialPostRecord(postType: String, count: Int, storageUsed: Int)
  case class SocialPostBulk(postType: String, count: Int, totalStorageUsed: Int)
  case class AveragePostStorage(postType: String, averageStorage: Double)


  // postType,count,storageUsed
  def readSocialUpdates() = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 12345)
    .load()
    .as[String]
    .map { line =>
      val tokens = line.split(",")
      SocialPostRecord(tokens(0), tokens(1).toInt, tokens(2).toInt)
    }

  def updateAverageStorage(
                            postType: String, // The key by which the grouping was made
                            group: Iterator[SocialPostRecord], // A batch of data associated to the key
                            state: GroupState[SocialPostBulk] // Like an "option", I have to manage manually
                          ) : AveragePostStorage = { // A single value that will be output per group of posts

    /** Pseudo algorithm for calculating the average post storage
      - Extract the state to start with
      - For all the items in the group
        - Aggregate data:
          - Summing up the total count
          - Summing up the total storage
      - Update the state with the new aggregated data
      - Return a single value of type AveragePostStorage
     */

    // extract the state to start with
    val previousBulk =
      if (state.exists) state.get
      else SocialPostBulk(postType, 0, 0)

    // iterate through the group
    val totalAggregatedData: (Int, Int) = group.foldLeft((0, 0)) { (currentData, record) =>
      val (currentCount, currentStorage) = currentData
      (currentCount + record.count, currentStorage + record.storageUsed)
    }

    // update the state with new aggregated data
    val (totalCount, totalStorage) = totalAggregatedData
    val newPostBulk = SocialPostBulk(postType, previousBulk.count + totalCount, previousBulk.totalStorageUsed + totalStorage)
    state.update(newPostBulk)

    // return a single output value
    AveragePostStorage(postType, newPostBulk.totalStorageUsed * 1.0 / newPostBulk.count)
  }

  def getAveragePostStorage() = {
    val socialStream = readSocialUpdates()

    val regularSqlAverageByPostType = socialStream
      .groupByKey(_.postType)
      .agg(sum(col("count")).as("totalCount").as[Int], sum(col("storageUsed")).as("totalStorage").as[Int])
      .selectExpr("key as postType", "totalStorage/totalCount as avgStorage")

    val averageByPostType = socialStream
      .groupByKey(_.postType)
      .mapGroupsWithState(GroupStateTimeout.NoTimeout())(updateAverageStorage)

    averageByPostType.writeStream
      .outputMode("update") // append not supported on mapGroupsWithState
      .foreachBatch { (batch: Dataset[AveragePostStorage], _: Long ) =>
        batch.show()
      }
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    getAveragePostStorage()
  }
}

//Data for entering into ncat window
/*
-- batch 1
text,3,3000
text,4,5000
video,1,500000
audio,3,60000
-- batch 2
text,1,2500
average for text = 10500 / 8
 */