package io.github.tankist88.mdle.mdl.utils

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.sql.{DataFrame, SaveMode}

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

  def saveTableFromDF(table: String, dataFrame: DataFrame): Unit = {

    val props = new Properties()
//    props.put("user", "serving_user")
//    props.put("password", "password123")
    props.put("driver", "org.postgresql.Driver")

    dataFrame.write.mode(SaveMode.Append).jdbc(createServingDbUrl(), table, props)
  }
}
