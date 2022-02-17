package io.github.tankist88.mdle.mdl.utils

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

import java.util.Properties

object LoadUtils {
  def createServingDbUrl(): String = {
    "jdbc:postgresql://postgresql-servingdb:5432/servingdb?" + "user=serving_user&" + "password=password123"
  }

  def createTradesDbUrl(): String = {
    "jdbc:postgresql://postgresql-tradesdb:5432/tradesdb?" + "user=trades_user&" + "password=password123"
  }

  def createMapper(): ObjectMapper = {
    val objectMapper = new ObjectMapper()
    objectMapper.registerModule(DefaultScalaModule)
    objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
    objectMapper
  }

  def loadTableJdbc(sqlCtx: SQLContext, table: String, url: String): DataFrame = {
    sqlCtx.read
      .format("jdbc")
      .option("url", url)
      .option("driver", "org.postgresql.Driver")
      .option("dbtable", table)
      .option("numPartitions", "10")
      .option("fetchsize", "100")
      .option("queryTimeout", "10")
      .load()
  }

  def saveTableFromDF(table: String, dataFrame: DataFrame, mode: SaveMode = SaveMode.Overwrite): Unit = {
    dataFrame
      .write
      .mode(mode)
      .option("numPartitions", "10")
      .option("queryTimeout", "10")
      .option("truncate", "true")
      .option("driver", "org.postgresql.Driver")
      .jdbc(createServingDbUrl(), table, new Properties())
  }
}
