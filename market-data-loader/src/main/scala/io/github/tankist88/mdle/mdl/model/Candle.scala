package io.github.tankist88.mdle.mdl.model

import java.sql.Timestamp
import scala.beans.BeanProperty

class Candle (
               @BeanProperty val datetime: Timestamp,
               @BeanProperty val ticker: String,
               @BeanProperty val open: Double,
               @BeanProperty val high: Double,
               @BeanProperty val low: Double,
               @BeanProperty val close: Double,
               @BeanProperty val value: Int
             )