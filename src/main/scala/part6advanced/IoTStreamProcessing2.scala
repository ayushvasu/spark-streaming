package part6advanced

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, GroupState, GroupStateTimeout}
import org.apache.spark.sql.types._
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.Dataset
import org.apache.log4j.{Level, Logger}
import java.sql.Timestamp


object IoTStreamProcessing2 {
  def main(args: Array[String]): Unit = {
    // Set log level to error
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
      IoTData("device1", "2024-05-26T23:55:01", 10.0),
      IoTData("device1", "2024-05-26T23:57:01", 20.0),
      IoTData("device2", "2024-05-27T01:01:01", 30.0),
      IoTData("device2", "2024-05-27T01:03:01", 40.0),
      IoTData("device1", "2024-05-27T00:01:01", 50.0),
      IoTData("device2", "2024-05-27T01:08:01", 60.0),
      // Additional data to help trigger watermarking
      IoTData("device1", "2024-05-27T01:11:01", 15.0),
      IoTData("device2", "2024-05-27T01:13:01", 25.0),
      //IoTData("device1", "2024-05-28T01:11:01", 15.0),
      //IoTData("device2", "2024-05-28T01:13:01", 25.0),
      //IoTData("device1", "2024-05-29T01:11:01", 15.0),
      //IoTData("device2", "2024-05-29T01:13:01", 25.0),
      //IoTData("device1", "2024-05-30T01:11:01", 15.0),
      //IoTData("device2", "2024-05-30T01:13:01", 25.0)

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
      val windowStart = key.windowStart
      val windowEnd = windowStart + (5 * 60 * 1000) - 1

      if (oldState.hasTimedOut) {

        println("window time out !! ")
        val state = oldState.getOption.getOrElse(DeviceMetricState(0.0, windowStart, windowEnd))
        oldState.remove()
        Iterator((key, state))
      } else {

        val currentState = oldState.getOption.getOrElse(DeviceMetricState(0.0, windowStart, windowEnd))
        println(oldState)
        val newSum = eventsList.map(_.metric).sum + currentState.sum
        val newState = DeviceMetricState(newSum, windowStart, windowEnd)
        oldState.update(newState)
        println(oldState)
        oldState.setTimeoutTimestamp(windowEnd + 1) // set timeout after window end
        Iterator.empty
      }
    }

    // Create a Dataset from MemoryStream
    val iotStream: Dataset[IoTData] = iotDataStream.toDS()

    // Apply the state update function with flatMapGroupsWithState
    val stateDStream = iotStream
      .withColumn("eventTime", unix_timestamp($"timestamp", "yyyy-MM-dd'T'HH:mm:ss").cast(TimestampType))
      .withWatermark("eventTime", "1 day")  // Allow for a large watermark to handle out-of-order data
      .withColumn("windowStart", get5MinWindowStartUDF($"timestamp"))
      .as[IoTData]
      .groupByKey(iotData => DeviceWindowKey(iotData.deviceId, get5MinWindowStart(iotData.timestamp)))
      .flatMapGroupsWithState(OutputMode.Append(), GroupStateTimeout.EventTimeTimeout())(updateStateWithEvent)

    // Write the resulting state to console
    val query = stateDStream.writeStream
      .outputMode(OutputMode.Append())
      .format("console")
      .option("truncate", "false")
      .start()

    query.awaitTermination
  }
}
