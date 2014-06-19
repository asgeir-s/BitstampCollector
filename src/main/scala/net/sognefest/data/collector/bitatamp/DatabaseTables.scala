package net.sognefest.data.collector.bitatamp

import scala.slick.driver.PostgresDriver.simple._
import scala.slick.jdbc.{StaticQuery => Q}


/**
 *  TODO: Should find out how to create only two table definitions (on for tick and one for granularity).
 */

class TickTable(tag: Tag) extends Table[TickDataPoint](tag, "tick") {
  // This is the primary key column
  def id = column[Option[Long]]("id", O.PrimaryKey, O.AutoInc)

  def sourceId = column[Option[Long]]("sourceId")

  def timestamp = column[Int]("timestamp")

  def price = column[Double]("price")

  def amount = column[Double]("amount")

  // Every table needs a * projection with the same type as the table's type parameter
  def * = (id, sourceId, timestamp, price, amount) <>(TickDataPoint.tupled, TickDataPoint.unapply)
}

class Min1Table(tag: Tag) extends Table[DataPoint](tag, "min1") {
  // This is the primary key column
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


class Min2Table(tag: Tag) extends Table[DataPoint](tag, "min2") {
  // This is the primary key column
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


class Min5Table(tag: Tag) extends Table[DataPoint](tag, "min5") {
  // This is the primary key column
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


class Min10Table(tag: Tag) extends Table[DataPoint](tag, "min10") {
  // This is the primary key column
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


class Min15Table(tag: Tag) extends Table[DataPoint](tag, "min15") {
  // This is the primary key column
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


class Min30Table(tag: Tag) extends Table[DataPoint](tag, "min30") {
  // This is the primary key column
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


class Hour1Table(tag: Tag) extends Table[DataPoint](tag, "hour1") {
  // This is the primary key column
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


class Hour2Table(tag: Tag) extends Table[DataPoint](tag, "hour2") {
  // This is the primary key column
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

class Hour5Table(tag: Tag) extends Table[DataPoint](tag, "hour5") {
  // This is the primary key column
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


class Hour12Table(tag: Tag) extends Table[DataPoint](tag, "hour12") {
  // This is the primary key column
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

class DayTable(tag: Tag) extends Table[DataPoint](tag, "day") {
  // This is the primary key column
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