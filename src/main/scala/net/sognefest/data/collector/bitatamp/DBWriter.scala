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

  val tableMap = Map(
    Granularity.min1 -> TableQuery[Min1Table],
    Granularity.min2 -> TableQuery[Min2Table],
    Granularity.min5 -> TableQuery[Min5Table],
    Granularity.min10 -> TableQuery[Min10Table],
    Granularity.min15 -> TableQuery[Min15Table],
    Granularity.min30 -> TableQuery[Min30Table],
    Granularity.hour1 -> TableQuery[Hour1Table],
    Granularity.hour2 -> TableQuery[Hour2Table],
    Granularity.hour5 -> TableQuery[Hour5Table],
    Granularity.hour12 -> TableQuery[Hour12Table],
    Granularity.day -> TableQuery[DayTable]
  )

  val tableRows = Map(
    Granularity.min1 -> NextRow(60, tickDataPoint),
    Granularity.min2 -> NextRow(120, tickDataPoint),
    Granularity.min5 -> NextRow(300, tickDataPoint),
    Granularity.min10 -> NextRow(600, tickDataPoint),
    Granularity.min15 -> NextRow(900, tickDataPoint),
    Granularity.min30 -> NextRow(1800, tickDataPoint),
    Granularity.hour1 -> NextRow(3600, tickDataPoint),
    Granularity.hour2 -> NextRow(7200, tickDataPoint),
    Granularity.hour5 -> NextRow(18000, tickDataPoint),
    Granularity.hour12 -> NextRow(43200, tickDataPoint),
    Granularity.day -> NextRow(86400, tickDataPoint)
  )
  // drop all tables if exists and create new once.
  tableMap.foreach(x => {
    if (makeTableMap.contains(x._1.toString)) {
      x._2.ddl.drop
    }
    x._2.ddl.create
  })

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
    tableMap.foreach(x => {
      val granularity = x._1
      val table = x._2
      val row = tableRows(granularity)
      while (row.endTimestamp < tickDataPoint.timestamp) {
        table += row.thisRow
        row.updateNoTickNextRow()
      }
      row.addTick(tickDataPoint)
    })
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

    def reinitialize(tick: TickDataPoint) {
      open = tick.price
      high = tick.price
      low = tick.price
      volume = tick.amount
      close = tick.price
      endTimestamp = tick.timestamp + intervalSec
      lastSourceId = tick.sourceId
    }

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
      reinitialize(TickDataPoint(None, lastSourceId, endTimestamp, close, 0))
    }

    def thisRow: DataPoint = {
      DataPoint(None, lastSourceId, endTimestamp, open, close, low, high, volume)
    }
  }


}

