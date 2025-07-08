package part6_advanved_spark_streaming.datasender

import java.io.PrintStream
import java.net.ServerSocket


/**
 * Basic data sender for use with Spark jobs.
 * Can also use ncat or netcat (something that will set up websocket server)
 */

// sending data "manually" through socket to be as deterministic as possible
object DataSender {
  val serverSocket = new ServerSocket(12345)
  val socket = serverSocket.accept() // blocking call
  val printer = new PrintStream(socket.getOutputStream)

  println("socket accepted")

  def example1() = {
    Thread.sleep(7000) // First seven seconds will empty
    printer.println("7000,blue")
    Thread.sleep(1000)
    printer.println("8000,green")
    Thread.sleep(4000)
    printer.println("14000,blue")
    Thread.sleep(1000)
    printer.println("9000,red") // discarded: older than the watermark
    Thread.sleep(3000)
    printer.println("15000,red")
    printer.println("8000,blue") // discarded: older than the watermark
    Thread.sleep(1000)
    printer.println("13000,green")
    Thread.sleep(500)
    printer.println("21000,green")
    Thread.sleep(3000)
    printer.println("4000,purple") // expect to be dropped - it's older than the watermark
    Thread.sleep(2000)
    printer.println("17000,green")
  }

  def example2() = {
    printer.println("5000,red")
    printer.println("5000,green")
    printer.println("4000,blue")
    Thread.sleep(7000)
    printer.println("1000,yellow")
    printer.println("2000,cyan")
    printer.println("3000,magenta")
    printer.println("5000,black")

    Thread.sleep(3000)
    printer.println("10000,pink")
  }

  def example3() = {
    Thread.sleep(2000)
    printer.println("9000,blue")
    Thread.sleep(3000)
    printer.println("2000,green")
    printer.println("1000,blue")
    printer.println("8000,red")
    Thread.sleep(2000)
    printer.println("5000,red") // discarded
    printer.println("18000,blue")
    Thread.sleep(1000)
    printer.println("2000,green") // discarded
    Thread.sleep(2000)
    printer.println("30000,purple")
    printer.println("10000,green")
  }


  def statefulExample = {
    Thread.sleep(2000)
    printer.println("text,3,3000")        // batch 1
    printer.println("text,4,5000")
    printer.println("video,1,500000")
    printer.println("audio,3,60000")

    Thread.sleep(5000)
    printer.println("text,1,2500")        // batch 2

    Thread.sleep(5000)
    printer.println("video,1,450000")     // batch 3
    printer.println("text,2,4000")
    printer.println("audio,2,20000")
    printer.println("image,1,12000")

    Thread.sleep(5000)
    printer.println("video,3,1350000")    // batch 4
    printer.println("text,5,6000")
    printer.println("image,2,18000")
  }

  def main(args: Array[String]): Unit = {
    statefulExample
  }
}
