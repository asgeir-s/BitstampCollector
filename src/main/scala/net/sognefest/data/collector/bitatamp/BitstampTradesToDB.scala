package net.sognefest.data.collector.bitatamp

import java.io._
import java.net._

import org.json4s._
import org.json4s.native.JsonMethods._

import scala.slick.jdbc.{StaticQuery => Q}
import scala.slick.lifted.TableQuery


/**
 * Get the newest trades from Bitstamp.
 * Used to breach the gap between the 15 min delayed trades received from Bitcoincharts and new live trades.
 */
class BitstampTradesToDB(dbWriter: DBWriter) {

  val startTimestamp = dbWriter.getEndTime
  println("BitstampTradesToDB: startTimestamp:" + startTimestamp)

  val tickTable = TableQuery[TickTable]
  val bitcoinchartsURL = new URL("https://www.bitstamp.net/api/transactions/")
  val connection = bitcoinchartsURL.openConnection()
  val bufferedReader: BufferedReader = new BufferedReader(new InputStreamReader(connection.getInputStream))
  var line: String = null
  val stringData = Stream.continually(bufferedReader.readLine()).takeWhile(_ != null).mkString("\n")
  bufferedReader.close()

  val children = parse(stringData).children
  children.reverse.foreach(x => {
    val timestamp = x.children.apply(0).values.toString.toInt
    if (timestamp > startTimestamp) {
      val id = None
      val sourceId = Some(x.children(1).values.toString.toLong)
      val price = x.children.apply(2).values.toString.toDouble
      val amount = x.children.apply(3).values.toString.toDouble
      dbWriter.newTick(TickDataPoint(id, sourceId, timestamp, price, amount))
    }
  })

  println("BitstampTradesToDB: getEndTime:" + dbWriter.getEndTime)

}
