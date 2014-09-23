package net.sognefest.data.collector.bitatamp


import java.util.Date

import com.cctrader.data.{DataPoint, TickDataPoint}

import scala.slick.driver.PostgresDriver.simple._
import scala.slick.jdbc.meta.MTable
import scala.slick.jdbc.{StaticQuery => Q}

/**
 * Writes ticks to the database and create (and write to the database) granularity's.
 */
class DBWriter(inSession: Session, addTickFromDb: Boolean) {

  implicit val session = inSession

  val tickTable = TableQuery[TickTable]

  var isLive = false

  val list: List[TickDataPoint] = tickTable.list
  val iterator = list.iterator
  var tickDataPoint = iterator.next()

  val tableMap = Map(
    "bitstamp_btc_usd_1min" -> TableQuery[InstrumentTable]((tag: Tag) => new InstrumentTable(tag, "bitstamp_btc_usd_1min")),
    "bitstamp_btc_usd_2min" -> TableQuery[InstrumentTable]((tag: Tag) => new InstrumentTable(tag, "bitstamp_btc_usd_2min")),
    "bitstamp_btc_usd_5min" -> TableQuery[InstrumentTable]((tag: Tag) => new InstrumentTable(tag, "bitstamp_btc_usd_5min")),
    "bitstamp_btc_usd_10min" -> TableQuery[InstrumentTable]((tag: Tag) => new InstrumentTable(tag, "bitstamp_btc_usd_10min")),
    "bitstamp_btc_usd_15min" -> TableQuery[InstrumentTable]((tag: Tag) => new InstrumentTable(tag, "bitstamp_btc_usd_15min")),
    "bitstamp_btc_usd_30min" -> TableQuery[InstrumentTable]((tag: Tag) => new InstrumentTable(tag, "bitstamp_btc_usd_30min")),
    "bitstamp_btc_usd_1hour" -> TableQuery[InstrumentTable]((tag: Tag) => new InstrumentTable(tag, "bitstamp_btc_usd_1hour")),
    "bitstamp_btc_usd_2hour" -> TableQuery[InstrumentTable]((tag: Tag) => new InstrumentTable(tag, "bitstamp_btc_usd_2hour")),
    "bitstamp_btc_usd_5hour" -> TableQuery[InstrumentTable]((tag: Tag) => new InstrumentTable(tag, "bitstamp_btc_usd_5hour")),
    "bitstamp_btc_usd_12hour" -> TableQuery[InstrumentTable]((tag: Tag) => new InstrumentTable(tag, "bitstamp_btc_usd_12hour")),
    "bitstamp_btc_usd_day" -> TableQuery[InstrumentTable]((tag: Tag) => new InstrumentTable(tag, "bitstamp_btc_usd_day"))
  )

  val tableRows = {
    if (!addTickFromDb) {
      tickDataPoint = {
        val lengthString = tickTable.length.run
        val lastRow = tickTable.filter(x => x.id === lengthString.toLong).take(1)
        val value = lastRow.firstOption
        value.get
      }
    }
    Map(
      "bitstamp_btc_usd_1min" -> NextRow(60, tickDataPoint),
      "bitstamp_btc_usd_2min" -> NextRow(120, tickDataPoint),
      "bitstamp_btc_usd_5min" -> NextRow(300, tickDataPoint),
      "bitstamp_btc_usd_10min" -> NextRow(600, tickDataPoint),
      "bitstamp_btc_usd_15min" -> NextRow(900, tickDataPoint),
      "bitstamp_btc_usd_30min" -> NextRow(1800, tickDataPoint),
      "bitstamp_btc_usd_1hour" -> NextRow(3600, tickDataPoint),
      "bitstamp_btc_usd_2hour" -> NextRow(7200, tickDataPoint),
      "bitstamp_btc_usd_5hour" -> NextRow(18000, tickDataPoint),
      "bitstamp_btc_usd_12hour" -> NextRow(43200, tickDataPoint),
      "bitstamp_btc_usd_day" -> NextRow(86400, tickDataPoint)
    )
  }

  if (addTickFromDb) {
    println("Creating new data granularity-tables - Start")
    // drop all tables if exists and create new once.
    tableMap.foreach(x => {
      if (makeTableMap.contains(x._1.toString)) {
        x._2.ddl.drop
      }
      x._2.ddl.create
    })
    //add all ticks
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
        if (isLive) {
          // notify trading systems that a new dataPoint is added with id
          Q.updateNA("NOTIFY " + x._1.toString + " , '" + table.list.last.id + "'").execute
        }
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
    isLive = true
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

