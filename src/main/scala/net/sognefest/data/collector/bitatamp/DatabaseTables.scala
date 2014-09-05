package net.sognefest.data.collector.bitatamp

import com.cctrader.data.{DataPoint, TickDataPoint}

import scala.slick.driver.PostgresDriver.simple._
import scala.slick.jdbc.{StaticQuery => Q}


/**
 * Tables
 */

class TickTable(tag: Tag) extends Table[TickDataPoint](tag, "bitstamp_btc_usd_tick") {
  // This is the primary key column
  def id = column[Option[Long]]("id", O.PrimaryKey, O.AutoInc)

  def sourceId = column[Option[Long]]("sourceId")

  def timestamp = column[Int]("timestamp")

  def price = column[Double]("price")

  def amount = column[Double]("amount")

  // Every table needs a * projection with the same type as the table's type parameter
  def * = (id, sourceId, timestamp, price, amount) <>(TickDataPoint.tupled, TickDataPoint.unapply)
}

class InstrumentTable(tag: Tag, tableName: String) extends Table[DataPoint](tag, tableName){
  def id = column[Option[Long]]("id", O.PrimaryKey, O.AutoInc)

  def sourceId = column[Option[Long]]("sourceId")

  def timestamp = column[Int]("timestamp")

  def open = column[Double]("open")

  def close = column[Double]("close")

  def low = column[Double]("low")

  def high = column[Double]("high")

  def volume = column[Double]("volume")

  // Every table needs a * projection with the same type as the table's type parameter
  def * = (id, sourceId, timestamp, open, close, low, high, volume) <>(DataPoint.tupled, DataPoint.unapply)
}