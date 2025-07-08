package utils

import org.apache.log4j.{Level, Logger}

/**
 * Useful functions for this course
 */
object utils {
  def ignoreLogs =  {
    // Reduce log level for Spark
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getRootLogger.setLevel(Level.ERROR)
  }
}
