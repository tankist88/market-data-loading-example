package io.github.tankist88.mdle.mdl.utils

import org.slf4j.LoggerFactory

import java.io.FileInputStream
import java.util.Properties

object PropertyUtils {
  private val PROPERTY_FILE = "mdl.properties"

  private val prop = initProperties()

  private def initProperties(): Properties = {
    try {
      val prop = new Properties()

      val classPathRes = this.getClass.getClassLoader.getResourceAsStream(PROPERTY_FILE)
      if (classPathRes != null) {
        prop.load(classPathRes)
      } else if (System.getProperty("mdl.config.path") != null) {
        prop.load(new FileInputStream(System.getProperty("mdl.config.path")))
      } else {
        prop.load(new FileInputStream(PROPERTY_FILE))
      }
      prop
    } catch {
      case e: Exception =>
        LoggerFactory.getLogger(this.getClass).error(e.getMessage, e)
        throw e
    }
  }

  def getString(key: String): String = {
    prop.getProperty(key)
  }

  def getBoolean(key: String): Boolean = {
    val value = prop.getProperty(key)
    if (value != null) value.toBoolean else false
  }

  def getInt(key: String): Int = {
    val value = prop.getProperty(key)
    if (value != null) value.toInt else 0
  }

  def getLong(key: String): Long = {
    val value = prop.getProperty(key)
    if (value != null) value.toLong else 0L
  }

  def getStringArray(key: String): Array[String] = {
    val value = getString(key)
    if (value == null) Array.empty[String]
    else value.replace(" ", "").split(",")
  }

  def getWithPrefix(prefix: String, removePrefix: Boolean = false): Map[String, String] = {
    prop
      .keySet()
      .toArray
      .map(key => key.toString)
      .filter(key => key.startsWith(prefix))
      .map(key => {
        if (removePrefix) (key.substring(key.indexOf(prefix) + prefix.length + 1), prop.get(key).toString)
        else (key, prop.get(key).toString)
      })
      .toMap
  }
}
