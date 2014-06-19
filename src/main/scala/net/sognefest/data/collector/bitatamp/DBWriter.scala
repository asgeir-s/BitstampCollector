package net.sognefest.data.collector.bitatamp


import scala.slick.driver.PostgresDriver.simple._
import scala.slick.jdbc.meta.MTable
import scala.slick.jdbc.{StaticQuery => Q}

/**
 * Writes ticks to the database and create (and write to the database) granularity's.
 */
class DBWriter(inSession: Session, addTickFromDb: Boolean) {

  implicit val session = inSession

  val tickTable = TableQuery[TickTable]

  println("Creating data granularity-tables - Start")
  val list: List[TickDataPoint] = tickTable.list
  val iterator = list.iterator
  var tickDataPoint = iterator.next()
  var lastTick = tickDataPoint

  val min1Table = TableQuery[Min1Table]
  var min1Row = NextRow(60, tickDataPoint)
  if (makeTableMap.contains("min1")) {
    min1Table.ddl.drop
  }
  min1Table.ddl.create

  val min2Table = TableQuery[Min2Table]
  var min2Row = NextRow(120, tickDataPoint)
  if (makeTableMap.contains("min2")) {
    min2Table.ddl.drop
  }
  min2Table.ddl.create

  val min5Table = TableQuery[Min5Table]
  var min5Row = NextRow(300, tickDataPoint)
  if (makeTableMap.contains("min5")) {
    min5Table.ddl.drop
  }
  min5Table.ddl.create

  val min10Table = TableQuery[Min10Table]
  var min10Row = NextRow(600, tickDataPoint)
  if (makeTableMap.contains("min10")) {
    min10Table.ddl.drop
  }
  min10Table.ddl.create

  val min15Table = TableQuery[Min15Table]
  var min15Row = NextRow(900, tickDataPoint)
  if (makeTableMap.contains("min15")) {
    min15Table.ddl.drop
  }
  min15Table.ddl.create

  val min30Table = TableQuery[Min30Table]
  var min30Row = NextRow(1800, tickDataPoint)
  if (makeTableMap.contains("min30")) {
    min30Table.ddl.drop
  }
  min30Table.ddl.create

  val hour1Table = TableQuery[Hour1Table]
  var hour1Row = NextRow(3600, tickDataPoint)
  if (makeTableMap.contains("hour1")) {
    hour1Table.ddl.drop
  }
  hour1Table.ddl.create

  val hour2Table = TableQuery[Hour2Table]
  var hour2Row = NextRow(7200, tickDataPoint)
  if (makeTableMap.contains("hour2")) {
    hour2Table.ddl.drop
  }
  hour2Table.ddl.create

  val hour5Table = TableQuery[Hour5Table]
  var hour5Row = NextRow(18000, tickDataPoint)
  if (makeTableMap.contains("hour5")) {
    hour5Table.ddl.drop
  }
  hour5Table.ddl.create

  val hour12Table = TableQuery[Hour12Table]
  var hour12Row = NextRow(43200, tickDataPoint)
  if (makeTableMap.contains("hour12")) {
    hour12Table.ddl.drop
  }
  hour12Table.ddl.create

  val dayTable = TableQuery[DayTable]
  var dayRow = NextRow(86400, tickDataPoint)
  if (makeTableMap.contains("day")) {
    dayTable.ddl.drop
  }
  dayTable.ddl.create

  if (addTickFromDb) {
    while (iterator.hasNext) {
      tickDataPoint = iterator.next()
      granulateTick(tickDataPoint)
    }
  }

  def newTick(tickDataPoint: TickDataPoint) {
    //add the tick to the tick database
    println("TICK: sourceId:" + tickDataPoint.sourceId + ", unixTimestamp:" + tickDataPoint.timestamp + ", price:" + tickDataPoint.price + ", amount" + tickDataPoint.amount)
    tickTable += tickDataPoint
    granulateTick(tickDataPoint)
  }

  def granulateTick(tickDataPoint: TickDataPoint) {
    //create granutations
    while (min1Row.endTimestamp < tickDataPoint.timestamp) {
      min1Table += min1Row.thisRow
      min1Row = min1Row.updateNoTickNextRow()
    }
    min1Row.addTick(tickDataPoint)

    while (min2Row.endTimestamp < tickDataPoint.timestamp) {
      min2Table += min2Row.thisRow
      min2Row = min2Row.updateNoTickNextRow()
    }
    min2Row.addTick(tickDataPoint)

    while (min5Row.endTimestamp < tickDataPoint.timestamp) {
      min5Table += min5Row.thisRow
      min5Row = min5Row.updateNoTickNextRow()
    }
    min5Row.addTick(tickDataPoint)

    while (min10Row.endTimestamp < tickDataPoint.timestamp) {
      min10Table += min10Row.thisRow
      min10Row = min10Row.updateNoTickNextRow()
    }
    min10Row.addTick(tickDataPoint)

    while (min15Row.endTimestamp < tickDataPoint.timestamp) {
      min15Table += min15Row.thisRow
      min15Row = min15Row.updateNoTickNextRow()
    }
    min15Row.addTick(tickDataPoint)

    while (min30Row.endTimestamp < tickDataPoint.timestamp) {
      min30Table += min30Row.thisRow
      min30Row = min30Row.updateNoTickNextRow()
    }
    min30Row.addTick(tickDataPoint)

    while (hour1Row.endTimestamp < tickDataPoint.timestamp) {
      hour1Table += hour1Row.thisRow
      hour1Row = hour1Row.updateNoTickNextRow()
    }
    hour1Row.addTick(tickDataPoint)

    while (hour2Row.endTimestamp < tickDataPoint.timestamp) {
      hour2Table += hour2Row.thisRow
      hour2Row = hour2Row.updateNoTickNextRow()
    }
    hour2Row.addTick(tickDataPoint)

    while (hour5Row.endTimestamp < tickDataPoint.timestamp) {
      hour5Table += hour5Row.thisRow
      hour5Row = hour5Row.updateNoTickNextRow()
    }
    hour5Row.addTick(tickDataPoint)

    while (hour12Row.endTimestamp < tickDataPoint.timestamp) {
      hour12Table += hour12Row.thisRow
      hour12Row = hour12Row.updateNoTickNextRow()
    }
    hour12Row.addTick(tickDataPoint)

    while (dayRow.endTimestamp < tickDataPoint.timestamp) {
      dayTable += dayRow.thisRow
      dayRow = dayRow.updateNoTickNextRow()
    }
    dayRow.addTick(tickDataPoint)

    lastTick = tickDataPoint

  }

  def getEndTime: Int = {
    val lengthString = tickTable.length.run
    val lastRow = tickTable.filter(x => x.id === lengthString.toLong).take(1)
    val value = lastRow.firstOption map (x => x.date)
    (value.get.getTime / 1000).toInt
  }

  def javaNewTick(sourceId: Long, unixTimestamp: Int, price: Double, amount: Double) {
    newTick(TickDataPoint(None, Some(sourceId), unixTimestamp, price, amount))
  }

  println("Creating granularity-tables - Finished")

  def makeTableMap: Map[String, MTable] = {
    val tableList = MTable.getTables.list(session)
    val tableMap = tableList.map { t => (t.name.name, t)}.toMap
    tableMap
  }

  case class NextRow(intervalSec: Int, firstTick: TickDataPoint) {
    var open = firstTick.price
    var high = firstTick.price
    var low = firstTick.price
    var volume = firstTick.amount
    var close = firstTick.price
    var endTimestamp = firstTick.timestamp + intervalSec
    var lastSourceId = firstTick.sourceId

    def addTick(tick: TickDataPoint): Unit = {
      lastSourceId = tick.sourceId
      if (volume == 0) {
        open = tick.price
        high = tick.price
        low = tick.price
        volume = tick.amount
        close = tick.price
      }
      else {
        if (tick.price > high) {
          high = tick.price
        }
        else if (tick.price < low) {
          low = tick.price
        }
        volume = volume + tick.amount
        close = tick.price
      }
    }

    def updateNoTickNextRow() = {
      NextRow(intervalSec, TickDataPoint(None, lastSourceId, endTimestamp, close, 0))
    }

    def thisRow: DataPoint = {
      DataPoint(None, lastSourceId, endTimestamp, open, close, low, high, volume)
    }
  }


}

