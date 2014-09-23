package net.sognefest.data.collector.bitatamp

import java.io._
import java.net._

import com.cctrader.data.TickDataPoint

import scala.slick.jdbc.{StaticQuery => Q}
import scala.slick.lifted.TableQuery

/**
 * Get "new" trades from Bitcoincharts.
 * The trades retrieved are delayed by approx. 15 minutes
 */
class BitcoinChartsTradesToDB(dbWriter: DBWriter) {

  val startTimestamp = dbWriter.getEndTime
  println("BitcoinChartsTradesToDB: startTimestamp:" + startTimestamp)

  val tickTable = TableQuery[TickTable]
  val bitcoinchartsURL = new URL("http://api.bitcoincharts.com/v1/trades.csv?symbol=bitstampUSD&start=" + (startTimestamp + 1))
  val connection = bitcoinchartsURL.openConnection()
  val bufferedReader: BufferedReader = new BufferedReader(new InputStreamReader(connection.getInputStream))
  var line: String = null
  val str = Stream.continually(bufferedReader.readLine()).takeWhile(_ != null).mkString("\n")
  bufferedReader.close()
  val stringArray = str.split("\n")
  stringArray.foreach(x => {
    val pointData = x.split(",")
    dbWriter.newTick(TickDataPoint(None, None, pointData(0).toInt, pointData(1).toDouble, pointData(2).toDouble))
  })
  println("BitcoinChartsTradesToDB: getEndTime:" + dbWriter.getEndTime)

}