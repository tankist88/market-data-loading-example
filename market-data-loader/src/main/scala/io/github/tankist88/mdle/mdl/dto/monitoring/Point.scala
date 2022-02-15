package io.github.tankist88.mdle.mdl.dto.monitoring

case class Point(success: Boolean, serviceName: String, requestTime: Long, batchSize: Long)
