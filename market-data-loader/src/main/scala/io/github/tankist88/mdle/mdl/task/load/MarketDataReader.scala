package io.github.tankist88.mdle.mdl.task.load

import io.github.tankist88.mdle.mdl.dto.MarketRecord
import io.github.tankist88.mdle.mdl.model.Candle
import io.github.tankist88.mdle.mdl.task.StreamingTask
import io.github.tankist88.mdle.mdl.utils.KafkaSink
import io.github.tankist88.mdle.mdl.utils.LoadUtils.{createMapper, createServingDbUrl, loadTableJdbc, saveTableFromDF}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

class MarketDataReader extends StreamingTask[MarketRecord] {
  val TIME_TICK_MINUTE = 5

  case class TradeRow(date: Date, ticker: String, price: Double, value: Int)

  override def serviceName(): String = "MARKET_DATA_READER"

  override def filter(cRec: ConsumerRecord[String, String], allowedEvents: Array[String]): Boolean = {
    try {
      cRec
        .headers
        .toArray
        .filter(header => "eventType" == header.key())
        .filter(header => header.value() != null)
        .exists(header => allowedEvents.contains(new String(header.value())))
    } catch {
      case e: Throwable =>
        logger.error(e.getMessage, e)
        false
    }
  }

  override def transform(msg: String): MarketRecord = {
    logger.debug("{}", msg)

    createMapper().readValue(msg, classOf[MarketRecord])
  }

  override def processRDD(rdd: RDD[MarketRecord], kafkaSink: Broadcast[KafkaSink]): Boolean = {
    try {
      processMarketRdd(rdd, "CANDLES_STREAM")
      true
    } catch {
      case e: Throwable =>
        logger.error(e.getMessage, e)
        false
    }
  }

  def mergeCandleFrames(df1: DataFrame, df2: DataFrame): DataFrame = {
    val oldData = df1
      .withColumnRenamed("datetime", "old_datetime")
      .withColumnRenamed("ticker", "old_ticker")
      .withColumnRenamed("open", "old_open")
      .withColumnRenamed("high", "old_high")
      .withColumnRenamed("low", "old_low")
      .withColumnRenamed("close", "old_close")
      .withColumnRenamed("value", "old_value")

    val newData = df2
      .withColumnRenamed("datetime", "new_datetime")
      .withColumnRenamed("ticker", "new_ticker")
      .withColumnRenamed("open", "new_open")
      .withColumnRenamed("high", "new_high")
      .withColumnRenamed("low", "new_low")
      .withColumnRenamed("close", "new_close")
      .withColumnRenamed("value", "new_value")
      .join(
        oldData,
        col("new_datetime") === col("old_datetime") && col("new_ticker") === col("old_ticker"),
        "full_outer"
      )

    val newPart = newData
      .where(col("new_datetime").isNotNull && col("new_ticker").isNotNull)
      .withColumnRenamed("new_datetime", "datetime")
      .withColumnRenamed("new_ticker", "ticker")
      .withColumnRenamed("new_open", "open")
      .withColumnRenamed("new_high", "high")
      .withColumnRenamed("new_low", "low")
      .withColumnRenamed("new_close", "close")
      .withColumnRenamed("new_value", "value")
      .select("datetime", "ticker", "open", "high", "low", "close", "value")

    val oldPart = newData
      .where(col("new_datetime").isNull && col("new_ticker").isNull)
      .withColumnRenamed("old_datetime", "datetime")
      .withColumnRenamed("old_ticker", "ticker")
      .withColumnRenamed("old_open", "open")
      .withColumnRenamed("old_high", "high")
      .withColumnRenamed("old_low", "low")
      .withColumnRenamed("old_close", "close")
      .withColumnRenamed("old_value", "value")
      .select("datetime", "ticker", "open", "high", "low", "close", "value")

    oldPart.union(newPart)
  }

  def processMarketRdd(rdd: RDD[MarketRecord], tableMain: String, tableShort: String = null): Unit = {
    val sqlCtx = SQLContext.getOrCreate(rdd.sparkContext)

    val candleRdd = rdd
      .map(row =>
        (
          createTimeBasedKey(row),
          TradeRow(stackDateTime(row, "dd.MM.yyyy HH:mm:ss"), row.secId, row.price, row.value.toInt)
        )
      )
      .aggregateByKey[Seq[TradeRow]](Seq())((sq, tr) => sq.+:(tr), (sq1, sq2) => sq1.++:(sq2))
      .map(row => ((row._1._1, row._1._2), createCandle(row._2)))
      .aggregateByKey[Seq[Candle]](Seq())((ls, c) => ls.+:(c), (ls1, ls2) => ls1.++:(ls2))
      .map(row => (row._1, row._2.sortBy(f => f.datetime.getTime)))
      .sortBy(f => f._1._1.getTime)
      .flatMap(row => row._2)

    val mergedMainData = mergeCandleFrames(
      loadTableJdbc(sqlCtx, tableMain, createServingDbUrl()),
      sqlCtx.createDataFrame(candleRdd, classOf[Candle])
    )

    if (tableShort != null) {
      val shortData = loadTableJdbc(sqlCtx, tableShort, createServingDbUrl())
      val mergedShortData = mergeCandleFrames(mergedMainData, shortData)
      saveTableFromDF(tableMain, mergedShortData, SaveMode.Overwrite)
    } else {
      saveTableFromDF(tableMain, mergedMainData, SaveMode.Overwrite)
    }
  }

  private def createTimeBasedKey(row: MarketRecord): (Date, String, String, Int) = {
    (
      new SimpleDateFormat("dd.MM.yyyy").parse(row.tradeDate),
      row.secId,
      row.tradeTime.split(":")(0),
      row.tradeTime.split(":")(1).toInt / TIME_TICK_MINUTE
    )
  }

  private def stackDateTime(row: MarketRecord, format: String): Date = {
    val date = row.tradeDate
    val time = row.tradeTime
    new SimpleDateFormat(format).parse(s"$date $time")
  }

  private def createCandle(trades: Seq[TradeRow]): Candle = {
    val ticker = trades.head.ticker
    val sortedTrades = trades.sortBy(tr => tr.date)
    val prices = sortedTrades.map(tr => tr.price)
    val value = sortedTrades.map(tr => tr.value).sum

    val open = prices.head
    val high = prices.max
    val low = prices.min
    val close = prices.last

    val calendar = Calendar.getInstance()
    calendar.setTime(sortedTrades.head.date)
    calendar.set(Calendar.SECOND, 0)
    calendar.set(Calendar.MINUTE, (calendar.get(Calendar.MINUTE) / TIME_TICK_MINUTE) * 5)

    new Candle(
      new java.sql.Timestamp(calendar.getTime.getTime),
      ticker,
      open,
      high,
      low,
      close,
      value
    )
  }
}
