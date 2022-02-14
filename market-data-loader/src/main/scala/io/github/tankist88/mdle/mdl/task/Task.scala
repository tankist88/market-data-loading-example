package io.github.tankist88.mdle.mdl.task

import org.apache.spark.SparkConf
import org.slf4j.{Logger, LoggerFactory}

trait Task extends Serializable {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def createSparkConf(): SparkConf

  def run(mode: String): Unit

  def serviceName(): String
}
