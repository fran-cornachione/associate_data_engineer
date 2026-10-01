# Databricks notebook source
# MAGIC %md
# MAGIC ### Auto Loader File Detection Modes
# MAGIC
# MAGIC Auto Loader supports two modes for detecting new files: **directory listing* and **file notification.** 
# MAGIC
# MAGIC You can switch file discovery modes across stream restarts and still obtain exactly-once data processing guarantees.
# MAGIC
# MAGIC #### Directory Listing Mode
# MAGIC
# MAGIC In directory listing mode, **Auto Loader identifies new files by listing the input directory.** Directory listing mode allows you to quickly start Auto Loader streams without any permission configurations other than access to your data on cloud storage.
# MAGIC
# MAGIC In Databricks Runtime 9.1 and above, Auto Loader can automatically detect whether files are arriving with lexical ordering to your cloud storage and significantly reduce the amount of API calls needed to detect new files. See [Auto Loader streams with directory listing mode](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/directory-listing-mode) for more details.
# MAGIC
# MAGIC #### File Notification Mode
# MAGIC
# MAGIC File notification mode leverages file notification and queue services in your cloud infrastructure account. Auto Loader can automatically set up a notification service and queue service that subscribe to file events from the input directory. If you enable file events on the external location that contains the files in question, you do not need to provide additional permissions when you set up the Auto Loader stream.
# MAGIC
# MAGIC File notification mode with file events is more performant and scalable than directory listing. Databricks recommends file notification mode using file events instead of directory listing mode for most workloads. If you are using Auto Loader in directory listing mode today, Databricks recommends that you migrate to file notification mode using file events to see significant performance improvements. See [Configure Auto Loader streams in file notification mode.](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/file-notification-mode)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Auto Loader Schema Evolution
# MAGIC
# MAGIC - `addNewColumns` (default): Stream fails. New columns are added to the schema. Existing columns do not evolve data types. 
# MAGIC - `rescue`: Schema is never evolved and stream does not fail due to schema changes. All new columns are recorded in the rescued data column
# MAGIC - `failOnNewColumns`: Stream fails. Stream does not restart unless the provided schema is updated, or the offending data file is removed.
# MAGIC - `none`: Does not evolve the schema, new columns are ignored, and data is not rescued unless the rescuedDataColumn option is set. Stream does not fail due to schema changes.
# MAGIC
# MAGIC > `addNewColumns` mode is the default when a schema is not provided, but `none` is the default when you provide a schema. `addNewColumns` is not allowed when the schema of the stream is provided, but does work if you provide your schema as a schema hint.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rescued Data Column
# MAGIC
# MAGIC When Auto Loader infers the schema, a rescued data column is automatically added to your schema as `_rescued_data`. You can rename the column or include it in cases where you provide a schema by setting the option `rescuedDataColumn`.
# MAGIC
# MAGIC The rescued data column ensures that columns that don't match with the schema are rescued instead of being dropped. The rescued data column contains any data that isn't parsed for the following reasons:
# MAGIC
# MAGIC - The column is missing from the schema.
# MAGIC - Type mismatches.
# MAGIC - Case mismatches.
# MAGIC - The rescued data column contains a JSON containing the rescued columns and the source file path of the record.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Auto Loader Options
# MAGIC
# MAGIC ### Generic Options
# MAGIC
# MAGIC #### `cloudfiles.maxBytesPerTrigger`
# MAGIC
# MAGIC Type: Byte String
# MAGIC
# MAGIC The maximum number of new bytes to be processed in every trigger. You can specify a byte string such as `10g` to limit each microbatch to 10 GB of data. This is a soft maximum. If you have files that are 3 GB each, Databricks processes 12 GB in a microbatch. When used together with `cloudFiles.maxFilesPerTrigger`, Databricks consumes up to the lower limit of `cloudFiles.maxFilesPerTrigger` or `cloudFiles.maxBytesPerTrigger`, whichever is reached first. This option has no effect when used with `trigger.once()` (Trigger.Once() is deprecated).
# MAGIC
# MAGIC **Default:** `None`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### `cloudfiles.maxBytesPerTrigger`
# MAGIC
# MAGIC **Type:** `Integer`
# MAGIC
# MAGIC The maximum number of new files to be processed in every trigger. When used together with `cloudFiles.maxBytesPerTrigger`, Databricks consumes up to the lower limit of `cloudFiles.maxFilesPerTrigger` or `cloudFiles.maxBytesPerTrigger`, whichever is reached first. This option has no effect when used with Trigger.Once() (deprecated).
# MAGIC
# MAGIC **Default:** `1000`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### `cloudfiles.inferColumnTypes`
# MAGIC
# MAGIC Type: Boolean
# MAGIC
# MAGIC Whether to infer exact column types when leveraging schema inference. By default, columns are inferred as strings when inferring JSON and CSV datasets. See schema inference for more details.
# MAGIC
# MAGIC Default: false
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### `ignoreCorruptFiles`
# MAGIC
# MAGIC Type: `Boolean`
# MAGIC
# MAGIC Whether to ignore corrupt files. If `true`, the Spark jobs will continue to run when encountering corrupted files and the contents that have been read will still be returned. Observable as `numSkippedCorruptFiles` in the `operationMetrics` column of the Delta Lake history. Available in Databricks Runtime 11.3 LTS and above.
# MAGIC
# MAGIC **Default value:** `false`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### `ignoreMissingFiles`
# MAGIC
# MAGIC Type: `Boolean`
# MAGIC
# MAGIC Whether to ignore missing files. If `true`, the Spark jobs will continue to run when encountering missing files and the contents that have been read will still be returned. Available in Databricks Runtime 11.3 LTS and above.
# MAGIC
# MAGIC **Default value:** `false` for Auto Loader, `true` for `COPY INTO` (legacy)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### `modifiedAfter`
# MAGIC
# MAGIC Type: `Timestamp String`, for example, `2021-01-01 00:00:00.000000 UTC+0`
# MAGIC
# MAGIC An optional timestamp as a filter to only ingest files that have a modification timestamp after the provided timestamp.
# MAGIC
# MAGIC **Default value:** `None`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### `modifiedBefore`
# MAGIC
# MAGIC **Type:** `Timestamp String`, for example, `2021-01-01 00:00:00.000000 UTC+0`
# MAGIC
# MAGIC An optional timestamp as a filter to only ingest files that have a modification timestamp before the provided timestamp.
# MAGIC
# MAGIC **Default value:** `None`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### CSV Options
# MAGIC
# MAGIC #### `badRecordsPath`
# MAGIC
# MAGIC Type: `String`
# MAGIC
# MAGIC The path to store files for recording the information about bad CSV records.
# MAGIC
# MAGIC Default value: `None`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### `columnNameOfCorruptRecord`
# MAGIC
# MAGIC Supported for Auto Loader. Not supported for COPY INTO (legacy).
# MAGIC
# MAGIC **Type:** `String`
# MAGIC
# MAGIC The column for storing records that are malformed and cannot be parsed. If the mode for parsing is set as `DROPMALFORMED`, this column will be empty.
# MAGIC
# MAGIC **Default value:** `_corrupt_record`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### `dateFormat`
# MAGIC
# MAGIC **Type:** `String`
# MAGIC
# MAGIC The format for parsing date strings.
# MAGIC
# MAGIC **Default value:** `yyyy-MM-dd`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### `timestampFormat`
# MAGIC
# MAGIC Type: `String`
# MAGIC
# MAGIC The format for parsing timestamp strings.
# MAGIC
# MAGIC Default value: `yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC `mode`
# MAGIC
# MAGIC Type: `String`
# MAGIC
# MAGIC Parser mode around handling malformed records. One of `'PERMISSIVE'`, `'DROPMALFORMED'`, and `'FAILFAST'`.
# MAGIC
# MAGIC Default value: `PERMISSIVE`
# MAGIC
# MAGIC #### `cloudFiles.allowOverwrites`
# MAGIC
# MAGIC **Type:** `Boolean`
# MAGIC
# MAGIC Whether to allow input directory file changes to overwrite existing data.
# MAGIC
# MAGIC **Default:** `false`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### `cloudFiles.backfillInterval`
# MAGIC
# MAGIC **Type:** `Interval String`
# MAGIC
# MAGIC Auto Loader can trigger asynchronous backfills at a given interval. For example `1 day` to backfill daily or `1 week` to backfill weekly.
# MAGIC
# MAGIC Do not use when `cloudFiles.useManagedFileEvents` is set to `true`.
# MAGIC
# MAGIC **Default:** `None`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### `cloudFiles.cleanSource`
# MAGIC
# MAGIC Type: `String`
# MAGIC
# MAGIC Whether to **automatically delete processed files from the input directory**. When set to `OFF` (default), no files are deleted.
# MAGIC
# MAGIC When set to `DELETE`, Auto Loader automatically deletes files 30 days after they are processed. To do this, Auto Loader must have write permissions to the source directory.
# MAGIC
# MAGIC When set to `MOVE`, Auto Loader automatically moves files to the specified location in `cloudFiles.cleanSource.moveDestination` 30 days after they are processed. To do this, Auto Loader must have write permissions to the source directory as well as to the move location.
# MAGIC
# MAGIC A file is considered processed when it has a non-null value for `commit_time` in the result of the `cloud_files_state` table valued function. The 30 day additional wait after processing can be configured using `cloudFiles.cleanSource.retentionDuration`.
# MAGIC
# MAGIC > **Note:** Databricks does not recommend using this option if there are multiple streams consuming data from the source location because the fastest consumer will delete the files and they will not be ingested in the slower sources.
# MAGIC
# MAGIC **Default:** OFF
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### `cloudFiles.cleanSource.retentionDuration`
# MAGIC
# MAGIC **Type:** `Interval String`
# MAGIC
# MAGIC Amount of time to wait before processed files become candidates for archival with cleanSource. Must be greater than 7 days for DELETE. No minimum restriction for MOVE.
# MAGIC
# MAGIC **Default value:** `30 days`
# MAGIC
# MAGIC ### AWS Specific Options
# MAGIC
# MAGIC #### `cloudFiles.queueUrl`
# MAGIC
# MAGIC **Type:** `String`
# MAGIC
# MAGIC The URL of the SQS queue. If provided, Auto Loader directly consumes events from this queue instead of setting up its own AWS SNS and SQS services.
# MAGIC
# MAGIC **Default:** `None`
# MAGIC
# MAGIC #### `cloudFiles.region`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Type:** `String`
# MAGIC
# MAGIC The region where the source S3 bucket resides and where the AWS SNS and SQS services will be created.
# MAGIC
# MAGIC **Default:** The region of the EC2 instance.
# MAGIC
# MAGIC `cloudFiles.queueUrl`
# MAGIC
# MAGIC **Type:** `String`
# MAGIC
# MAGIC The URL of the SQS queue. If provided, Auto Loader directly consumes events from this queue instead of setting up its own AWS SNS and SQS services.
# MAGIC
# MAGIC **Default:** `None`

# COMMAND ----------

# MAGIC %md
# MAGIC #### `pathGlobFilter` or `fileNamePattern`
# MAGIC
# MAGIC Type: `String`
# MAGIC
# MAGIC A potential glob pattern to provide for choosing files. Equivalent to `PATTERN` in `COPY INTO` (legacy). `fileNamePattern` can be used in `read_files`.
# MAGIC
# MAGIC **Default value:** `None`

# COMMAND ----------

# MAGIC %md
# MAGIC ### Common Data Loading Patterns
# MAGIC
# MAGIC | Patttern                 | Description                                                                                       |
# MAGIC |------------------------|---------------------------------------------------------------------------------------------------|
# MAGIC | `?`                    | Matches any single character.                                                      |
# MAGIC | `*`                    | Matches zero or more characters.                                                               |
# MAGIC | `[abc]`                | Matches a single character from character set {a, b, c}.                              |
# MAGIC | `[a-z]`                | Matches a single character from the character range {a…z}.                                    |
# MAGIC | `[^a]`                 | Matches a single character that is not from character set or range {a}. Note that the `^` character must occur immediately to the right of the opening bracket. |
# MAGIC | `{ab,cd}`            | Matches a string from the string set {ab, cd}.                                         |
# MAGIC | `{ab,c{de,fh}}`      | Matches a string from the string set {ab, cde, cfh}.                                   |

# COMMAND ----------

# MAGIC %md
# MAGIC For example, if you would like to parse only `png` files in a directory that contains files with different suffixes, you can do:

# COMMAND ----------

# DBTITLE 1,pathGlobFilter - Example
df = spark.readStream.format("cloudFiles") \
  .option("cloudFiles.format", "binaryFile") \
  .option("pathGlobfilter", "*.png") \
  .load(base-path)