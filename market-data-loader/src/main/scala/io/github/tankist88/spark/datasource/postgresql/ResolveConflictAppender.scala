package io.github.tankist88.spark.datasource.postgresql

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.sources.v2.writer.{DataSourceWriter, DataWriter, DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, WriteSupport}
import org.apache.spark.sql.types.{DataTypes, StructType}

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.Optional

class ResolveConflictAppender extends AnyRef with DataSourceV2 with WriteSupport {
  override def createWriter(
                             writeUUID: String,
                             schema: StructType,
                             mode: SaveMode,
                             options: DataSourceOptions
                           ): Optional[DataSourceWriter] = {

    if (SaveMode.Append != mode) throw new IllegalArgumentException("Only Append mode supported")

    val urlOpt = options.get("url")
    val tableNameOpt = options.get("tableName")
    val keysOpt = options.get("keys")

    Optional.of(
      new Writer(
        ConnectionProperties(
          if (urlOpt.isPresent) urlOpt.get() else throw new IllegalArgumentException("url option must be set"),
          if (tableNameOpt.isPresent) tableNameOpt.get() else throw new IllegalArgumentException("tableName option must be set"),
          if (keysOpt.isPresent) keysOpt.get() else throw new IllegalArgumentException("keys option must be set")
        ),
        schema
      )
    )
  }
}

case class ConnectionProperties(url: String, tableName: String, keys: String)

object WriteSucceeded extends WriterCommitMessage

class Writer(connectionProperties: ConnectionProperties, schema: StructType) extends DataSourceWriter {
  override def createWriterFactory(): DataWriterFactory[InternalRow] = {
    new RowDataWriterFactory(connectionProperties, schema)
  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = {}

  override def abort(messages: Array[WriterCommitMessage]): Unit = {}
}

class RowDataWriterFactory(connectionProperties: ConnectionProperties, schema: StructType) extends DataWriterFactory[InternalRow] {
  override def createDataWriter(partitionId: Int, taskId: Long, epochId: Long): DataWriter[InternalRow] = {
    new PostgresWriter(connectionProperties, schema)
  }
}

class PostgresWriter(connectionProperties: ConnectionProperties, schema: StructType) extends DataWriter[InternalRow] {
  val connection: Connection = getConnection
  val preparedStatement: PreparedStatement = createStatement()

  private def getConnection: Connection = {
    Class.forName("org.postgresql.Driver")
    val conn = DriverManager.getConnection(connectionProperties.url)
    conn.setAutoCommit(true)
    conn
  }

  private def createStatement(): PreparedStatement = {
    val tableName = connectionProperties.tableName.toUpperCase
    val values = schema.fieldNames.map(_ => "?").mkString(",")
    val fields = schema.fieldNames.mkString(",")
    val keys = connectionProperties.keys.replace(" ", "").toUpperCase
    val excluded = keys.split(",").map(key => s"$key = EXCLUDED.$key").mkString(",")

    val statement =
      s"INSERT INTO $tableName($fields) VALUES($values) " +
      s"ON CONFLICT ($keys) " +
      s"DO UPDATE SET $excluded"

    connection.prepareStatement(statement)
  }

  override def write(record: InternalRow): Unit = {
    schema.fieldNames.foreach(name => {
      val sf = schema.apply(name)
      val index = schema.fieldIndex(name)
      if (sf.dataType == DataTypes.StringType) {
        preparedStatement.setString(index + 1, record.getString(index))
      } else if (sf.dataType == DataTypes.LongType) {
        preparedStatement.setLong(index + 1, record.getLong(index))
      } else if (sf.dataType == DataTypes.IntegerType) {
        preparedStatement.setInt(index + 1, record.getInt(index))
      } else if (sf.dataType == DataTypes.TimestampType) {
        preparedStatement.setTimestamp(index + 1, DateTimeUtils.toJavaTimestamp(record.getLong(index)))
      } else if (sf.dataType == DataTypes.DoubleType) {
        preparedStatement.setDouble(index + 1, record.getDouble(index))
      } else if (sf.dataType == DataTypes.FloatType) {
        preparedStatement.setFloat(index + 1, record.getFloat(index))
      } else if (sf.dataType == DataTypes.BooleanType) {
        preparedStatement.setBoolean(index + 1, record.getBoolean(index))
      } else if (sf.dataType == DataTypes.ShortType) {
        preparedStatement.setShort(index + 1, record.getShort(index))
      } else if (sf.dataType == DataTypes.ByteType) {
        preparedStatement.setByte(index + 1, record.getByte(index))
      } else if (sf.dataType == DataTypes.DateType) {
        preparedStatement.setDate(index + 1, DateTimeUtils.toJavaDate(record.getInt(index)))
      } else if (sf.dataType == DataTypes.NullType) {
        throw new IllegalStateException(s"Field $name of type ${sf.dataType} not supported")
      } else if (sf.dataType == DataTypes.BinaryType) {
        throw new IllegalStateException(s"Field $name of type ${sf.dataType} not supported")
      } else if (sf.dataType == DataTypes.CalendarIntervalType) {
        throw new IllegalStateException(s"Field $name of type ${sf.dataType} not supported")
      }
    })

    preparedStatement.executeUpdate()
  }

  override def commit(): WriterCommitMessage = {
    connection.close()
    WriteSucceeded
  }

  override def abort(): Unit = {
    connection.close()
  }
}