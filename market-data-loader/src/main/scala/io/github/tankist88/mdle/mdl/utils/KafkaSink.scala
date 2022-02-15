package io.github.tankist88.mdle.mdl.utils

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.{Logger, LoggerFactory}

import java.util

class KafkaSink(createProducer: () => KafkaProducer[String, String]) extends Serializable {
  lazy val producer: KafkaProducer[String, String] = createProducer()

  def send(topic: String, value: String, headers: util.List[Header]): Unit = sendMsgToKafka(producer, topic, value, headers)

  private def sendMsgToKafka(producer: KafkaProducer[String, String], topic: String, value: String, headers: util.List[Header]): Unit = {
    LoggerFactory.getLogger(classOf[KafkaSink]).info("Sending msg... Topic {}, Value {}, Headers {}", topic, value, headers)

    producer.send(new ProducerRecord[String, String](
      topic,
      null,
      null,
      null,
      value,
      headers)
    )
  }
}

object KafkaSink {
  def apply(appName: String): KafkaSink = {
    val f = () => {
      val logger: Logger = LoggerFactory.getLogger(classOf[KafkaSink])

      val producer = createKafkaProducer(appName)

      sys.addShutdownHook {
        if (producer != null) {
          logger.info("Closing Kafka Producer")
          producer.close()
        } else {
          logger.info("Kafka Producer not created")
        }
      }

      producer
    }

    new KafkaSink(f)
  }

  def createKafkaProducer(appName: String): KafkaProducer[String, String] = {
    LoggerFactory.getLogger(classOf[KafkaSink]).info("Creating Kafka Producer")

    val brokers = PropertyUtils.getString(appName.toLowerCase + ".kafka.brokers")

    val config = new util.HashMap[String, Object]()
    config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])

    PropertyUtils
      .getWithPrefix("mdl.kafka", removePrefix = true)
      .foreach(e => config.put(e._1, e._2))

    new KafkaProducer[String, String](config)
  }
}

