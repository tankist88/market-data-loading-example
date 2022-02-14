package io.github.tankist88.mdle.mdl.task

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

abstract class DefaultTask extends Task {
  def process(sc: SparkContext, sqlCtx: SQLContext, startTime: Long): Unit

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

      process(sc, sqlCtx, startTime)
    } finally {
      if (sc != null) sc.stop()
    }
  }
}
