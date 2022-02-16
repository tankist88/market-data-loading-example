package io.github.tankist88.mdle.mdl.task

import io.github.tankist88.mdle.mdl.task.load.{MarketDataReader, ProcessMarketDataLost}

object TaskDict extends Enumeration {
  type TaskType = Value

  val MARKET_DATA_READER: TaskDict.Value = Value(classOf[MarketDataReader].getName)
  val PROCESS_MARKET_DATA_LOST: TaskDict.Value = Value(classOf[ProcessMarketDataLost].getName)
}
