package io.github.tankist88.mdle.mdl.task

import io.github.tankist88.mdle.mdl.dto.monitoring.Point
import io.github.tankist88.mdle.mdl.model.exception.LoadKafkaOffsetsException
import io.github.tankist88.mdle.mdl.monitoring.InfluxClient
import io.github.tankist88.mdle.mdl.utils.LoadUtils.createPgDbUrl
import io.github.tankist88.mdle.mdl.utils.{KafkaSink, PropertyUtils}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Assign
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import java.sql.{Connection, Date, DriverManager, PreparedStatement, ResultSet, Statement}
import java.time.Duration
import java.util
import scala.collection.JavaConversions.collectionAsScalaIterable
import scala.collection.mutable
import scala.reflect.ClassTag

abstract class StreamingTask[T](implicit c: ClassTag[T]) extends Task {
  private val INSERT_OFFSET_QUERY = s"" +
    s"INSERT INTO OFFSET_CHECKPOINT VALUES(?, ?, ?, ?) " +
    s"ON CONFLICT (APP_NAME, TOPIC, PART) " +
    s"DO UPDATE SET APP_NAME = EXCLUDED.APP_NAME, TOPIC = EXCLUDED.TOPIC, PART = EXCLUDED.PART"

  private val INSERT_STATE_QUERY = s"" +
    s"INSERT INTO CLUSTER_APP_STATE VALUES(?, ?, ?) " +
    s"ON CONFLICT (APP_NAME) " +
    s"DO UPDATE SET APP_NAME = EXCLUDED.APP_NAME"

  def filter(cRec: ConsumerRecord[String, String], allowedEvents: Array[String]): Boolean

  def transform(msg: String): T

  def processRDD(rdd: RDD[T], kafkaSink: Broadcast[KafkaSink]): Boolean

  def processAll(messages: DStream[ConsumerRecord[String, String]], allowedEvents: Array[String], kafkaSink: Broadcast[KafkaSink]): Unit = {
    var startTime = 0L

    var offsetRanges = Array[OffsetRange]()

    messages.transform(rdd => {
        if (rdd.isInstanceOf[HasOffsetRanges]) {
          offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        }
        startTime = System.currentTimeMillis()
        rdd
      })
      .filter(cRec => filter(cRec, allowedEvents))
      .map(cRec => transform(cRec.value()))
      .filter(cRec => cRec != null)
      .foreachRDD(rdd => {
        if (!rdd.isEmpty()) {
          val rddSize = rdd.cache().count()

          logger.info("Batch size {} rows", rddSize)

          val success = processRDD(rdd, kafkaSink)

          rdd.unpersist()

          val elapsedTime = System.currentTimeMillis() - startTime

          logger.debug("Batch with {} rows processing time {} ms", rddSize, elapsedTime)

          InfluxClient.sendPoint(Point(success, serviceName(), elapsedTime, rddSize))
        }

        saveOffsets(offsetRanges)
      })
  }

  override def createSparkConf(): SparkConf = {
    new SparkConf()
      .setAppName(getClass.getName)
      .setAll(PropertyUtils.getWithPrefix("mdl.spark", removePrefix = true))
  }

  def createStreamingContext(): StreamingContext = {
    val batchDuration = PropertyUtils.getInt("mdl.spark.streaming.batch.duration.seconds")

    logger.info(
      "Running Task {} on Spark Streaming with batch duration {} seconds",
      this.getClass.getName,
      batchDuration)

    val sc = SparkContext.getOrCreate(createSparkConf())

    val ssc = new StreamingContext(sc, Seconds(batchDuration))

    SQLContext.setActive(SQLContext.getOrCreate(sc))

    val brokers = PropertyUtils.getString(getClass.getName.toLowerCase + ".kafka.brokers")
    val group = PropertyUtils.getString(getClass.getName.toLowerCase + ".kafka.group")
    val topics = PropertyUtils.getString(getClass.getName.toLowerCase + ".kafka.topics")
    val extraKafkaProps = PropertyUtils.getWithPrefix("mdl.kafka", removePrefix = true)

    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.GROUP_ID_CONFIG -> group,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
    )

    val consumerStrategy = getConsumerStrategy[String, String](topicsSet, kafkaParams.++(extraKafkaProps))
    val messages = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, consumerStrategy)

    val allowedEvents = PropertyUtils.getStringArray(getClass.getName.toLowerCase + ".allowed.events")

    val kafkaSink = sc.broadcast(KafkaSink(getClass.getName))

    processAll(messages, allowedEvents, kafkaSink)

    ssc
  }

  def startInClientMode(): Unit = {
    val ssc = createStreamingContext()

    try {
      ssc.start()
      ssc.awaitTermination()
    } finally {
      if (ssc != null) ssc.stop(stopSparkContext = true, stopGracefully = true)
    }
  }

  def startInClusterMode(): Unit = {
    val shutdownCheckTimeout = PropertyUtils.getLong(getClass.getName.toLowerCase + ".cluster.mode.shutdown.check.timeout")

    val ssc = createStreamingContext()

    createRunningFlag()

    ssc.start()

    var isStopped = false
    while (!isStopped) {
      isStopped = ssc.awaitTerminationOrTimeout(shutdownCheckTimeout)
      if (isStopped) logger.info("The Spark Streaming context is stopped. Exiting application...")
      if (!isStopped && appIsStopping()) {
        logger.info("Stopping the ssc Spark Streaming Context...")
        ssc.stop(stopSparkContext = true, stopGracefully = true)
        logger.info("Spark Streaming Context is Stopped!")
      }
    }
  }

  override def run(mode: String): Unit = {
    if (mode == "client") {
      startInClientMode()
    } else if (mode == "cluster") {
      startInClusterMode()
    }
  }

  private def getConsumerStrategy[K, V](topics: Set[String], kafkaParams: Map[String, Object]): ConsumerStrategy[K, V] = {
    val actualOffsetRanges = mutable.Map[TopicPartition, (Long, Long)]()

    val kafkaParamsJMap = new util.HashMap[String, Object]()
    kafkaParams.foreach(kv => kafkaParamsJMap.put(kv._1, kv._2))

    logger.info("Creating Kafka Consumer for read topics topology...")
    var consumer: KafkaConsumer[String, String] = null
    try {
      consumer = new KafkaConsumer[String, String](kafkaParamsJMap)
      val topicPartitions = new util.ArrayList[TopicPartition]()
      topics.foreach(topic => {
        consumer.partitionsFor(topic).foreach(p => topicPartitions.add(new TopicPartition(p.topic(), p.partition())))
      })

      val startOffsets = consumer.beginningOffsets(topicPartitions)
      logger.debug("Beginning Offsets: {}", startOffsets)

      val endOffsets = consumer.endOffsets(topicPartitions)
      logger.debug("End Offsets: {}", endOffsets)

      topicPartitions.foreach(tp => actualOffsetRanges.put(tp, (startOffsets.get(tp), endOffsets.get(tp))))
      logger.debug("Actual Offset Ranges: {}", actualOffsetRanges)
    } finally {
      logger.info("Closing Kafka Consumer after read topics topology...")
      if (consumer != null) {
        consumer.close(Duration.ofSeconds(30))
        logger.info("Topology Kafka Consumer closed.")
      } else {
        logger.info("Topology Kafka Consumer not found!")
      }
    }

    val savedOffsets = getOffsets(topics).map(tp => {
      val actualRange = actualOffsetRanges.get(tp._1).orNull
      if (actualRange != null) {
        val start = actualRange._1
        val end = actualRange._2

        if (tp._2 >= start && tp._2 <= end) tp
        else (tp._1, end)
      } else tp
    })

    val actualOffsets =
      if (savedOffsets.nonEmpty) {
        logger.info("Saved offsets from DB {}", savedOffsets)

        savedOffsets
      } else {
        val endOffsets = actualOffsetRanges.map(tp => (tp._1, tp._2._2)).toMap

        logger.info("End offsets from Kafka {}", endOffsets)

        endOffsets
      }

    if (actualOffsets.isEmpty) throw new LoadKafkaOffsetsException("Can't load actual kafka offsets")

    Assign[K, V](actualOffsets.keys.toList, kafkaParams, actualOffsets)
  }

  private def createRunningFlag(): Unit = {
    Class.forName("org.postgresql.Driver")

    var connection: Connection = null
    var stmt: PreparedStatement = null
    try {
      connection = DriverManager.getConnection(createPgDbUrl())
      connection.setAutoCommit(false)
      stmt = connection.prepareStatement(INSERT_STATE_QUERY)
      stmt.setString(1, getClass.getName)
      stmt.setString(2, "RUNNING")
      stmt.setDate(3, new Date(System.currentTimeMillis()))
      stmt.executeUpdate()
      connection.commit()

      logger.debug("Application {} running", getClass.getName)
    } finally {
      if (stmt != null) stmt.close()
      if (connection != null) connection.close()
    }
  }

  private def appIsStopping(): Boolean = {
    Class.forName("org.postgresql.Driver")

    var connection: Connection = null
    var stmt: Statement = null
    var resultSet: ResultSet = null
    try {
      connection = DriverManager.getConnection(createPgDbUrl())

      stmt = connection.createStatement()

      val query = s"SELECT APP_STATE FROM CLUSTER_APP_STATE WHERE APP_NAME = '${getClass.getName}'"

      resultSet = stmt.executeQuery(query)

      if (resultSet.next()) {
        resultSet.getString("APP_STATE") == "STOPPING"
      } else {
        true
      }
    } finally {
      if (resultSet != null) resultSet.close()
      if (stmt != null) stmt.close()
      if (connection != null) connection.close()
    }
  }

  private def saveOffsets(offsetRanges: Array[OffsetRange]): Unit = {
    Class.forName("org.postgresql.Driver")

    offsetRanges.foreach(offsetRange => {
      var connection: Connection = null
      var stmt: PreparedStatement = null
      try {
        connection = DriverManager.getConnection(createPgDbUrl())
        connection.setAutoCommit(false)
        stmt = connection.prepareStatement(INSERT_OFFSET_QUERY)
        stmt.setString(1, getClass.getName)
        stmt.setString(2, offsetRange.topic)
        stmt.setInt(3, offsetRange.partition)
        stmt.setLong(4, offsetRange.untilOffset)
        stmt.executeUpdate()
        connection.commit()

        logger.debug("Saved offset {}", offsetRange)
      } finally {
        if (stmt != null) stmt.close()
        if (connection != null) connection.close()
      }
    })
  }

  def getOffsets(topics: Set[String]): Map[TopicPartition, Long] = {
    Class.forName("org.postgresql.Driver")

    var connection: Connection = null
    var stmt: Statement = null
    var resultSet: ResultSet = null
    try {
      connection = DriverManager.getConnection(createPgDbUrl())

      stmt = connection.createStatement()

      val query =
        s"SELECT " +
          s"TOPIC, PART, LAST_OFFSET " +
        s"FROM " +
          s"OFFSET_CHECKPOINT " +
        s"WHERE " +
          s"APP_NAME = '${getClass.getName}' AND TOPIC IN (${topics.map(t => s"'$t'").mkString(", ")})"

      resultSet = stmt.executeQuery(query)

      val topicMap = mutable.Map[TopicPartition, Long]()
      while (resultSet.next()) {
        topicMap.put(
          new TopicPartition(
            resultSet.getString("TOPIC"),
            resultSet.getInt("PART")
          ), resultSet.getLong("LAST_OFFSET")
        )
      }

      topicMap.toMap
    } finally {
      if (resultSet != null) resultSet.close()
      if (stmt != null) stmt.close()
      if (connection != null) connection.close()
    }
  }
}
