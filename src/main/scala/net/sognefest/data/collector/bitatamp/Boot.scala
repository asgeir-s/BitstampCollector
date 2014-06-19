package net.sognefest.data.collector.bitatamp

import com.typesafe.config.ConfigFactory

import scala.slick.jdbc.JdbcBackend._

/**
 * Used to start the actor system
 */

object Boot extends App {

  val config = ConfigFactory.load()


  val databaseFactory = Database.forURL(
    url = "jdbc:postgresql://" + config.getString("postgres.host") + ":" + config.getString("postgres.port") + "/" + config
      .getString("postgres.dbname"),
    driver = config.getString("postgres.driver"),
    user = config.getString("postgres.user"),
    password = config.getString("postgres.password"))

  val dbSession = databaseFactory.createSession()

  println("-------------------------- STEP1 - bitcoinChartsHistoryToDB - Start --------------------------")
  val bitcoinChartsHistoryToDB = new BitcoinChartsHistoryToDB(true, true, true, true, "", dbSession)
  println("-------------------------- STEP1 - bitcoinChartsHistoryToDB - end ----------------------------")

  println("-------------------------- STEP2 - DBWriter - Start ------------------------------------------")
  val dbWriter = new DBWriter(dbSession, true)
  println("-------------------------- STEP2 - DBWriter - Initialization done ----------------------------")

  println("-------------------------- STEP3 - BitcoinChartsTradesToDB - Start ---------------------------")
  new BitcoinChartsTradesToDB(dbWriter)
  println("-------------------------- STEP3 - BitcoinChartsTradesToDB - end -----------------------------")

  println("-------------------------- STEP4 - BitstampTradesToDB - Start --------------------------------")
  new BitstampTradesToDB(dbWriter)
  println("-------------------------- STEP4 - BitstampTradesToDB - end ----------------------------------")

  println("-------------------------- STEP5 - BitstampLive - Start --------------------------------------")
  new BitstampLive(dbWriter)
}