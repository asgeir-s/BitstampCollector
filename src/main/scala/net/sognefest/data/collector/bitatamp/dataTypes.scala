package net.sognefest.data.collector.bitatamp

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


object Granularity extends Enumeration {
  type Granularity = Value
  val min1 = Value("min1")
  val min2 = Value("min2")
  val min5 = Value("min5")
  val min10 = Value("min10")
  val min15 = Value("min15")
  val min30 = Value("min30")
  val hour1 = Value("hour1")
  val hour2 = Value("hour2")
  val hour5 = Value("hour5")
  val hour12 = Value("hour12")
  val day = Value("day")
  val tick = Value("tick")
}

object Exchange extends Enumeration {
  type Exchange = Value
  val bitstamp, btcChina = Value
}


object OrderType extends Enumeration {
  type OrderType = Value
  val BUY, SELL = Value
}

object CurrencyPair extends Enumeration {
  type CurrencyPair = Value
  val BTC_USD = Value
}