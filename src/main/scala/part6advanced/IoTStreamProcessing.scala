package part6advanced

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import org.apache.spark.sql.types._
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.Dataset
import java.util.Date
import java.text.SimpleDateFormat

import java.sql.Timestamp

case class IoTData(deviceId: String, timestamp: String, metric: Double)
case class DeviceMetricState(sum: Double, windowStart: Long, windowEnd: Long) {
  override def toString: String = s"DeviceMetricState : $sum, ${epochToDate(windowStart)}, ${epochToDate(windowEnd)}"

  def epochToDate(epochMillis: Long): String = {
    val df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    df.format(epochMillis)
  }
}
case class DeviceWindowKey(deviceId: String, windowStart: Long){
  override def toString: String = s"DeviceWindowKey : $deviceId, ${epochToDate(windowStart)}"

  def epochToDate(epochMillis: Long): String = {
    val df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    df.format(epochMillis)
  }
}

case class DeviceWindowKey2(deviceId: String){
  override def toString: String = s"DeviceWindowKey : $deviceId"

}

object IoTStreamProcessing {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    val spark = SparkSession.builder
      .appName("IoT Stream Processing")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Define schema for incoming data
    val schema = new StructType()
      .add("deviceId", StringType)
      .add("timestamp", StringType)
      .add("metric", DoubleType)

    // Create MemoryStream for testing
    implicit val sqlContext = spark.sqlContext
    val iotDataStream = MemoryStream[IoTData]

    // Sample data
    val sampleData = Seq(
      IoTData("device1", "2024-05-27T01:01:01", 10.0),
      IoTData("device1", "2024-05-27T01:02:01", 20.0),
      IoTData("device2", "2024-05-27T01:01:01", 30.0),
      IoTData("device2", "2024-05-27T01:03:01", 40.0)
    )

    iotDataStream.addData(sampleData)

    // Function to round down a timestamp to the nearest 5-minute mark
    def get5MinWindowStart(ts: String): Long = {
      val timestamp = Timestamp.valueOf(ts.replace("T", " "))
      val millis = timestamp.getTime
      millis - (millis % (5 * 60 * 1000))
    }

    // Register UDF
    val get5MinWindowStartUDF = udf(get5MinWindowStart _)

    // Define function to update state
    def updateStateWithEvent(key: DeviceWindowKey, events: Iterator[IoTData], oldState: GroupState[DeviceMetricState]): Iterator[(DeviceWindowKey, DeviceMetricState)] = {
      val eventsList = events.toList

      if (eventsList.nonEmpty) {
        val windowStart = key.windowStart
        val windowEnd = windowStart + (5 * 60 * 1000) - 1

        val currentState = oldState.getOption.getOrElse(DeviceMetricState(0.0, windowStart, windowEnd))

        val timestamp = Timestamp.valueOf(eventsList.head.timestamp.replace("T", " "))
        val millis = timestamp.getTime
        if (millis < windowEnd) {
          val newSum = eventsList.map(_.metric).sum + currentState.sum
          val newState = DeviceMetricState(newSum, windowStart, windowEnd)
          oldState.update(newState)
          Iterator((key, newState))
        } else {
          oldState.remove()
          Iterator.empty
        }
      } else {
        Iterator.empty
      }
    }

    // Create a Dataset from MemoryStream
    val iotStream: Dataset[IoTData] = iotDataStream.toDS()

    // Apply the state update function with flatMapGroupsWithState
    val stateDStream = iotStream
      .withColumn("windowStart", get5MinWindowStartUDF($"timestamp"))
      .as[IoTData]
      .groupByKey(iotData => DeviceWindowKey(iotData.deviceId, get5MinWindowStart(iotData.timestamp)))
      .flatMapGroupsWithState(OutputMode.Append(), GroupStateTimeout.NoTimeout())(updateStateWithEvent)

    // Write the resulting state to console
    val query = stateDStream.writeStream
      .outputMode(OutputMode.Append())
      .format("console")
      .option("truncate", "false")
      .start()

    query.awaitTermination
  }
}
