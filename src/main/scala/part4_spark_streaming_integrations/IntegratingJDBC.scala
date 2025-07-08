package part4_spark_streaming_integrations

import common.{Car, carsSchema}
import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * Bad News
 * - You can't read streams from JDBC
 * - You can't write to JDBC in a streaming fashion - ACID! (this is very important)
 * ---> You can write data in batches instead
 */

object IntegratingJDBC {

  val spark = SparkSession.builder()
    .appName("Integrating JDBC")
    .master("local[2]")
    .getOrCreate()

  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val user = "docker"
  val password = "docker"

  import spark.implicits._

  def writeStreamToPostgres() = {
    val carsDF = spark.readStream
      .schema(carsSchema)
      .json("src/main/resources/data/cars")

    val carsDS = carsDF.as[Car]

    /**
     * Used for supported JDBC, per batch when writing.
     * - Reading data in a streaming fashion is not supported
     */
    carsDS.writeStream
      .foreachBatch { (batch: Dataset[Car], _: Long) =>
        // each executor can control the batch
        // use this to write each batch to Postgres DB
        // batch is a STATIC Dataset/DataFrame

        batch.write
          .format("jdbc")
          .option("driver", driver)
          .option("url", url)
          .option("user", user)
          .option("password", password)
          .option("dbtable", "public.cars")
          .save()
      }
      .start()
      .awaitTermination()

  }

  def main(args: Array[String]): Unit = {
    writeStreamToPostgres()
  }

}
