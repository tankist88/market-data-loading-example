package io.github.tankist88.mdle.mdl.task.load

import io.github.tankist88.mdle.mdl.dto.MarketRecord
import io.github.tankist88.mdle.mdl.dto.task.DefaultTaskResult
import io.github.tankist88.mdle.mdl.task.DefaultTask
import io.github.tankist88.mdle.mdl.utils.LoadUtils.createTradesDbUrl
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.functions.{col, to_date}

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

class ProcessMarketDataLost extends DefaultTask {
  override def serviceName(): String = "PROCESS_MARKET_DATA_LOST"

  override def process(sc: SparkContext, sqlCtx: SQLContext): DefaultTaskResult = {
    try {
//      val prevDate = plusDaysToDate(new Date(), -1)
//      val prevDateStr = new SimpleDateFormat("yyyy-MM-dd").format(prevDate)

      val prevDateStr = "2021-09-01"

      val deals = sqlCtx.read
        .format("jdbc")
        .option("url", createTradesDbUrl())
        .option("driver", "org.postgresql.Driver")
        .option("dbtable", "DEAL")
//        .option("user", "trades_user")
//        .option("password", "password123")
        .option("numPartitions", 10)
        .load()
        .where(to_date(col("TRADEDATE"), "dd.MM.yyyy") === prevDateStr)
        .rdd
        .map(createMarketRecord)

      val dealsCount = deals.cache().count()

      new MarketDataReader().processMarketRdd(deals)

      deals.unpersist()

      DefaultTaskResult(success = true, dealsCount)
    } catch {
      case e: Throwable =>
        logger.error(e.getMessage, e)
        DefaultTaskResult(success = false, 0L)
    }
  }

  def plusDaysToDate(date: java.util.Date, days: Int): java.util.Date = {
    val c = Calendar.getInstance
    c.setTime(date)
    c.add(Calendar.DAY_OF_MONTH, days)
    new java.util.Date(c.getTime.getTime)
  }

  private def createMarketRecord(row: Row): MarketRecord = {
    MarketRecord(
      row.getAs[Long]("tradeno"),
      row.getAs[String]("tradedate"),
      row.getAs[String]("tradetime"),
      row.getAs[String]("secid"),
      row.getAs[String]("boardid"),
      row.getAs[java.math.BigDecimal]("price").doubleValue(),
      row.getAs[Int]("quantity"),
      row.getAs[java.math.BigDecimal]("value").doubleValue(),
      row.getAs[String]("buysell"),
      row.getAs[String]("tradingsession")
    )
  }
}
