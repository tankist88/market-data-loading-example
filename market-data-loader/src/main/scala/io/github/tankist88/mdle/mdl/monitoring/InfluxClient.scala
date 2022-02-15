package io.github.tankist88.mdle.mdl.monitoring

import io.github.tankist88.mdle.mdl.dto.monitoring.Point
import io.github.tankist88.mdle.mdl.utils.PropertyUtils
import org.influxdb.InfluxDBFactory
import org.slf4j.{Logger, LoggerFactory}

object InfluxClient {
  val logger: Logger = LoggerFactory.getLogger(getClass)

  private val MEASUREMENT = "metrics_raw"
  private val RETENTION_POLICY = "autogen"

  def sendPoint(point: Point): Unit = {
    try {
      val databaseURL = PropertyUtils.getString("mdl.influxdb.url")
      val db = PropertyUtils.getString("mdl.influxdb.database")
      val user = PropertyUtils.getString("mdl.influxdb.username")
      val password = PropertyUtils.getString("mdl.influxdb.password")

      if (databaseURL == null || db == null || user == null || password == null) {
        logger.warn("Metric sending skipped. Monitoring properties does not set.")
      } else {
        val influxDB = InfluxDBFactory.connect(databaseURL, user, password)
        influxDB.write(
          db,
          RETENTION_POLICY,
          org.influxdb.dto.Point.measurement(MEASUREMENT)
            .tag("status", if (point.success) "OK" else "FAIL")
            .tag("service_name", point.serviceName)
            .field("request_time", point.requestTime)
            .field("batch_size", point.batchSize)
            .build
        )
        logger.debug("Metric successfully sent to InfluxDB. Metric: {}", point)
      }
    } catch {
      case ex: Throwable => logger.error("Error while sending metric to InfluxDB. " + ex.getMessage, ex)
    }
  }
}
