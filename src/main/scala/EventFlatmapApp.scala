import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.json4s._
import org.json4s.native.JsonMethods._

import scala.util.Try

/**
  * Flatten out input records to discrete events.  Write to parquet.
  */
object EventFlatmapApp extends App {
  val spark = SparkSession.builder.master("local[*]").appName("EventFlatmap").getOrCreate()

  import spark.implicits._

  val eventSource = spark.sparkContext.wholeTextFiles("./data/input/")

  val parsedJson = eventSource.map { case (_, json) =>
    (Try(parse(json)), json)
  }.cache()

  /**
    * Handle invalid JSON somehow
    */
  parsedJson
    .filter { case (parseAttempt, _) => parseAttempt.isFailure }
    .foreach { case (failure, invalidJson) =>
      failure.recover { case ex =>
        println("This record is invalid.  Let's put the record and parse error somewhere for later review..")
        println(s"Exception: $ex")
        println("Invalid JSON")
        println(invalidJson)
      }
    }

  /**
    * Valid JSON
    */
  val explodedEvents: Dataset[String] = parsedJson
    .filter { case (parseAttempt, _) => parseAttempt.isSuccess }

    /**
      * "Explode" JSON using a flatMap to create 1:0..Many relationship.
      */
    .flatMap { case (parseAttempt, _) =>

      implicit val formats = DefaultFormats

      val jValue: JValue = parseAttempt.get

      val explodedEvents: Seq[String] = for {
        JObject(envelope) <- jValue
        appId @ JField("app_id", _) <- envelope
        userId @ JField("user_id", _) <- envelope
        clientAppVersion @ JField("client_app_version", _) <- envelope
        deviceId @ JField("device_id", _) <- envelope
        serverEventTimestamp @ JField("server_event_timestamp", _) <- envelope
        JField("events", JArray(jEvents)) <- envelope
        event <- jEvents
      } yield {
        /**
          * Recreating de-normalized data as JSON to take advantage of Spark's JSON dataframe reader.  This reader
          * will build an aggregate schema for us row-by-row, saving us from doing it.  This may not be very efficient,
          * but the point is it makes for a simpler implementation because I don't have to write the logic to
          * rebuild a schema every time we flush to parquet on disk.
          */
        compact(render(
          JObject(appId, userId, clientAppVersion, deviceId, serverEventTimestamp)
            .merge(event.mapField {
              case (fieldName, value) => (s"exploded_event_$fieldName", value)
            })
        ))
      }

      explodedEvents
    }.toDS().cache()

  explodedEvents.show()

  spark.read
    .json(explodedEvents)
    .write
    .mode(SaveMode.Append)
    .parquet("./data/output.parquet")

  spark.stop()
}
