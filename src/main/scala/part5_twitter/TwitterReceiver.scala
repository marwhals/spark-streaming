package part5_twitter

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import twitter4j.{StallWarning, Status, StatusDeletionNotice, StatusListener, TwitterStream, TwitterStreamFactory}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Promise

/**
 * TODO - check X API docs to fix this. No tweets in console.
 */

class TwitterReceiver extends Receiver[Status](StorageLevel.MEMORY_ONLY){

  val twitterStreamPromise = Promise[TwitterStream]
  val twitterStreamFuture = twitterStreamPromise.future

  private def simpleStatusListener = new StatusListener {
    override def onStatus(status: Status): Unit = store(status)

    override def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice): Unit = ()

    override def onTrackLimitationNotice(numberOfLimitedStatuses: Int): Unit = ()

    override def onScrubGeo(userId: Long, upToStatusId: Long): Unit = ()

    override def onStallWarning(warning: StallWarning): Unit = ()

    override def onException(ex: Exception): Unit = ()
  }

  // this is run asynchronously
  override def onStart(): Unit = {
    val twitterStream: TwitterStream = new TwitterStreamFactory("src/main/resources/twitter4j.properties")
      .getInstance()
      .addListener(simpleStatusListener)
      .sample("en") // call the sample endpoint for English tweets

    twitterStreamPromise.success(twitterStream)

  }
  override def onStop(): Unit = twitterStreamFuture.foreach { twitterStream =>
    twitterStream.cleanUp()
    twitterStream.shutdown()

  }
}
