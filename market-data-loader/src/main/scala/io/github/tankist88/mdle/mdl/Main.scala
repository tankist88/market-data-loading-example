package io.github.tankist88.mdle.mdl

import io.github.tankist88.mdle.mdl.task.{Task, TaskDict}
import org.slf4j.LoggerFactory

object Main {
  private val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    try {
      if (args.length == 0) {
        logger.info("Please specify job class name as argument.")
      } else {
        TaskDict.withName(args(0))
        Class.forName(args(0)).getDeclaredConstructor().newInstance().asInstanceOf[Task].run(args(1))
      }
    } catch {
      case e: Throwable =>
        logger.error(e.getMessage, e)
        throw e
    }
  }
}
