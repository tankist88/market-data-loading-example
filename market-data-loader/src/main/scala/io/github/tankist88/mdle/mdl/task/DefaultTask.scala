package io.github.tankist88.mdle.mdl.task

import io.github.tankist88.mdle.mdl.dto.monitoring.Point
import io.github.tankist88.mdle.mdl.dto.task.DefaultTaskResult
import io.github.tankist88.mdle.mdl.monitoring.InfluxClient
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

abstract class DefaultTask extends Task {
  def process(sc: SparkContext, sqlCtx: SQLContext): DefaultTaskResult

  override def createSparkConf(): SparkConf = {
    new SparkConf().setAppName(getClass.getName)
  }

  override def run(mode: String): Unit = {
    val startTime = System.currentTimeMillis()

    logger.info("Running Task {} on Spark", getClass.getName)

    val sc = new SparkContext(createSparkConf())

    try {
      val sqlCtx = new SQLContext(sc)
      SQLContext.setActive(sqlCtx)

      val result = process(sc, sqlCtx)

      val elapsedTime = System.currentTimeMillis() - startTime

      InfluxClient.sendPoint(Point(result.success, serviceName(), elapsedTime, result.batchSize))
    } finally {
      if (sc != null) sc.stop()
    }
  }
}
