package part6advanced

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, GroupState, GroupStateTimeout}
import org.apache.spark.sql.types._
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.Dataset
import org.apache.log4j.{Level, Logger}
import java.sql.Timestamp


case class DeviceMetricState4(metrics: Map[Long, Double])
case class DeviceWindowResult4(deviceId: String, windowStart: Long, windowEnd: Long, sum: Double)

object IoTStreamProcessing4 {
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
      IoTData("device1", "2024-05-26T22:55:01", 10.0),
      IoTData("device1", "2024-05-26T22:57:01", 20.0),
      IoTData("device2", "2024-05-27T01:01:01", 30.0),
      IoTData("device2", "2024-05-27T01:03:01", 40.0),
      IoTData("device1", "2024-05-27T00:01:01", 50.0),
      //IoTData("device2", "2024-05-27T01:08:01", 60.0),
      //IoTData("device1", "2024-05-27T01:11:01", 15.0),
      //IoTData("device2", "2024-05-27T01:13:01", 25.0)
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
    def updateStateWithEvent(deviceId: String, events: Iterator[IoTData], oldState: GroupState[DeviceMetricState4]): Iterator[DeviceWindowResult4] = {
      val eventsList = events.toList

      println(deviceId + eventsList.mkString(" | "))

      // Get the current state or initialize an empty state
      val currentState: DeviceMetricState4 = oldState.getOption.getOrElse(DeviceMetricState4(Map.empty[Long, Double]))

      // Update state with new events
      val updatedMetrics = eventsList.foldLeft(currentState.metrics) { (metrics, event) =>
        val windowStart = get5MinWindowStart(event.timestamp)
        val newSum = metrics.getOrElse(windowStart, 0.0) + event.metric
        metrics + (windowStart -> newSum)
      }

      // Get current time to determine expired windows
      val currentTime = eventsList.map(a => get5MinWindowStart(a.timestamp)).toSet
      val windowDuration = 5 * 60 * 1000

      // Filter out expired windows
      val (expired, active) = updatedMetrics.partition { case (windowStart, _) =>
        println(deviceId + " : " + currentTime.exists(_ > windowStart + windowDuration) + " : " + windowStart.toString + " : " +  currentTime.mkString(", "))
        currentTime.exists(_ > windowStart + windowDuration)
      }

      // Emit expired windows
      val expiredResults = expired.map { case (windowStart, sum) =>
        DeviceWindowResult4(deviceId, windowStart, windowStart + windowDuration - 1, sum)
      }

      println(active)
      // Update state with active windows
      oldState.update(DeviceMetricState4(active))

      expiredResults.iterator
    }

    // Create a Dataset from MemoryStream
    val iotStream: Dataset[IoTData] = iotDataStream.toDS()

    // Apply the state update function with flatMapGroupsWithState
    val stateDStream = iotStream
      .withColumn("eventTime", unix_timestamp($"timestamp", "yyyy-MM-dd'T'HH:mm:ss").cast(TimestampType))
      .as[IoTData]
      .groupByKey(_.deviceId)
      .flatMapGroupsWithState(OutputMode.Append(), GroupStateTimeout.NoTimeout())(updateStateWithEvent)

    // Write the resulting state to console
    val query = stateDStream.writeStream
      .outputMode(OutputMode.Append())
      .format("console")
      .option("truncate", "false")
      .start()

    query.awaitTermination()
  }
}
