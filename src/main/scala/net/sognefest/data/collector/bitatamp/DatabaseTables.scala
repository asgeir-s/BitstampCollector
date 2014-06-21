package net.sognefest.data.collector.bitatamp

import scala.slick.driver.PostgresDriver.simple._
import scala.slick.jdbc.{StaticQuery => Q}


/**
 * Tables
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

// granularity tables
class Min1Table(tag: Tag) extends Table[DataPoint](tag, "min1") with DPTable {def * = common_*}
class Min2Table(tag: Tag) extends Table[DataPoint](tag, "min2") with DPTable {def * = common_*}
class Min5Table(tag: Tag) extends Table[DataPoint](tag, "min5") with DPTable {def * = common_*}
class Min10Table(tag: Tag) extends Table[DataPoint](tag, "min10") with DPTable {def * = common_*}
class Min15Table(tag: Tag) extends Table[DataPoint](tag, "min15") with DPTable {def * = common_*}
class Min30Table(tag: Tag) extends Table[DataPoint](tag, "min30") with DPTable {def * = common_*}
class Hour1Table(tag: Tag) extends Table[DataPoint](tag, "hour1") with DPTable {def * = common_*}
class Hour2Table(tag: Tag) extends Table[DataPoint](tag, "hour2") with DPTable {def * = common_*}
class Hour5Table(tag: Tag) extends Table[DataPoint](tag, "hour5") with DPTable {def * = common_*}
class Hour12Table(tag: Tag) extends Table[DataPoint](tag, "hour12") with DPTable {def * = common_*}
class DayTable(tag: Tag) extends Table[DataPoint](tag, "day") with DPTable {def * = common_*}
