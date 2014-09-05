package com.cctrader.data

import java.util.Date

/**
 * Summery of a interval of ticks
 *
 * @param timestamp in seconds for last trade in this interval
 * @param id my id for the trade, set automatically. Use None for creation
 * @param sourceId id of last trade in this interval, given buy source (bitstamp)
 * @param open open price for interval
 * @param close close price for interval
 * @param low lowest price in interval
 * @param high highest price in interval
 * @param volume number of bitcoins traded in interval
 */
case class DataPoint(id: Option[Long],
                     sourceId: Option[Long],
                     timestamp: Int,
                     open: Double,
                     close: Double,
                     low: Double,
                     high: Double,
                     volume: Double) {
  val date = new Date(timestamp * 1000L)
}

/**
 *
 * @param timestamp timestamp in seconds
 * @param id my id for the trade, set automatically. Use None for creation
 * @param sourceId id given buy source (bitstamp)
 * @param price price at trade
 * @param amount trade size
 */
case class TickDataPoint(
                          id: Option[Long],
                          sourceId: Option[Long],
                          timestamp: Int,
                          price: Double,
                          amount: Double) {
  val date = new Date(timestamp * 1000L)
}

/**
 *
 * @param id my id for the trade, set automatically. Use None for creation
 * @param writettimestamp the the signal is written to the database
 * @param dptimestamp time on dataPoint (that lead to the trade)
 * @param signal BUY, SELL
 */
case class Trade(
                  id: Option[Long],
                  writettimestamp: Int,
                  dptimestamp: Int,
                  signal: String,
                  price: Double)


object Signal extends Enumeration {
  type Signal = Value
  val LOONG, SHORT, CLOSE = Value
}

object Mode extends Enumeration {
  type Mode = Value
  val TESTING, LIVE = Value
}