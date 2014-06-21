package net.sognefest.data.collector.bitatamp


import scala.slick.driver.PostgresDriver.simple._
import scala.slick.jdbc.{StaticQuery => Q}


/**
 *
 */
trait DPTable {
  this: Table[_] =>
  def id = column[Option[Long]]("id", O.PrimaryKey, O.AutoInc)

  def sourceId = column[Option[Long]]("sourceId")

  def timestamp = column[Int]("timestamp")

  def open = column[Double]("open")

  def close = column[Double]("close")

  def low = column[Double]("low")

  def high = column[Double]("high")

  def volume = column[Double]("volume")

  // Every table needs a * projection with the same type as the table's type parameter
  protected val common_* = (id, sourceId, timestamp, open, close, low, high, volume) <>(DataPoint.tupled, DataPoint.unapply)
}

