import org.apache.spark.sql.SparkSession

/**
  * Template app to perform analytics on parquet output of `EventFlatmapApp`
  */
object EventFlatmapAnalyticsApp extends App {
  val spark = SparkSession.builder.master("local[*]").appName("EventFlatmapAnalytics").getOrCreate()

  import spark.implicits._

  /**
    * Merge schema across partitions using `spark.sql.parquet.mergeSchema` setting
    *
    * "Like ProtocolBuffer, Avro, and Thrift, Parquet also supports schema evolution. Users can start with a simple
    * schema, and gradually add more columns to the schema as needed. In this way, users may end up with multiple
    * Parquet files with different but mutually compatible schemas. The Parquet data source is now able to automatically
    * detect this case and merge schemas of all these files."    *
    * https://spark.apache.org/docs/latest/sql-programming-guide.html#schema-merging
    */
  val events = spark.read
    .option("mergeSchema", "true")
    .parquet("./data/output.parquet")

  events.printSchema()

  events.where($"exploded_event_event_type" === 123).show()

  spark.stop()
}
