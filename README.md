# Event Flatmap

> A demonstration of flattening nested JSON input with dynamic fields to parquet for analysis

## `EventFlatmapApp`

This app has the following workflow.

1. Loads all files in `./data/input/`
2. Parses JSON using json4s
3. Alerts invalid JSON to be directed somewhere else
4. Process valid JSON
  - `flatMap` nested `events` field and create a 0..Many flattened, denormalized JSON records with all the fields of the
   parent and dynamic fields in the child.
5. Use Dataframe API's JSON reader to load RDD and generate a schema on the fly
6. Write to parquet partition files to `./data/output.parquet/`

## `EventFlatmapAnalyticsApp`

This is a sample application to perform analytics with a merged schema from the generated parquet output of
`EventFlatmapApp`.  At this time it's implemented to select and display all records with an event_id of 123.
