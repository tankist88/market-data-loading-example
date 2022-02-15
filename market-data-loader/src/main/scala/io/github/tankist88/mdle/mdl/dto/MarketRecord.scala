package io.github.tankist88.mdle.mdl.dto

case class MarketRecord(
                         tradeNo: Long,
                         tradeDate: String,
                         tradeTime: String,
                         secId: String,
                         boardId: String,
                         price: Double,
                         quantity: Int,
                         value: Double,
                         buySell: String,
                         tradingSession: String
                       )
