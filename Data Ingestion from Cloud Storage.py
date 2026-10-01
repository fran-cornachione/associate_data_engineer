# Databricks notebook source
# MAGIC %md
# MAGIC ### **CREATE TABLE AS SELECT (CTAS)**
# MAGIC
# MAGIC Creates a Delta table by default from files in cloud object storage
# MAGIC
# MAGIC The `read_files()` function reads files under a provided location and returns the data in tabular form.
# MAGIC
# MAGIC - Supports reading file formats like JSON, CSV, XML, TEXT, BINARYFILE, PARQUET, AVRO, and ORC file formats.
# MAGIC
# MAGIC - Can detect the **file format automatically and infer a unified schema** across all files.
# MAGIC
# MAGIC - Specify **specific file format options** to read in the data based on the source file format.
# MAGIC
# MAGIC - Can be used in **streaming tables** to **incrementally** ingest files into Delta Lake using Auto Loader.

# COMMAND ----------

# DBTITLE 1,CTAS Syntax
# MAGIC %sql
# MAGIC CREATE TABLE new_table
# MAGIC AS SELECT
# MAGIC   *
# MAGIC FROM read_files(
# MAGIC   'path/to/files',
# MAGIC   format => 'file_type',
# MAGIC   -- other_format_specific_options
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ### **COPY INTO**
# MAGIC
# MAGIC - Is a **retriable and idempotent operation** and will skip files that have already been loaded (**incremental**)
# MAGIC
# MAGIC - Supports various common files types like parquet, JSON, XML, etc
# MAGIC
# MAGIC - **FROM** specifies the path of the cloud storage location continuously adding files
# MAGIC
# MAGIC - **FORMAT_OPTIONS()** control how the source files are parsed and interpreted. The available options depends on the file format
# MAGIC
# MAGIC - **COPY_OPTIONS()** controls the behavior of the COPY INTO operation itself, such as schema evolution (**mergeSchema**) and idempotency (**force**)
# MAGIC
# MAGIC **1. Create an empty table to copy data into**
# MAGIC
# MAGIC - You can create an empty table without a schema
# MAGIC
# MAGIC - You also can explicitly create the table with a schema

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE new_table;
# MAGIC
# MAGIC COPY INTO new_table
# MAGIC FROM
# MAGIC   'path/to/files'
# MAGIC FILEFORMAT = file_type
# MAGIC FORMAT_OPTIONS(options)
# MAGIC COPY_OPTIONS(options);

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Auto Loader**
# MAGIC
# MAGIC - Incrementally and efficiently processes new data files (in batch or streaming) as they arrive in cloud storage without any additional setup
# MAGIC - Auto Loader has support for both Python and SQL (leveraging Declarative Pipelines)
# MAGIC - You can use Auto Loader to process billions of files
# MAGIC - Auto Loader is built upon Spark Structured Streaming
# MAGIC - A deep dive into Auto Loader is out of scope for this course, please refer to these links and courses for more in depth information resources:

# COMMAND ----------

# DBTITLE 1,Python Auto Loader
(spark
.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", checkpoint_location)
    .load("/Volumes/catalog/schema/files")
.writeStream
    .option("checkpointLocation", checkpoint_location)
    .trigger(processingTime="5 seconds")
    .toTable("catalog.database.table")
)

# COMMAND ----------

# DBTITLE 1,SQL Auto Loader
# MAGIC %sql
# MAGIC CREATE OR REFRESH STREAMING TABLE
# MAGIC   catalog.schema.table_name
# MAGIC SCHEDULE EVERY
# MAGIC   1 HOUR
# MAGIC AS SELECT
# MAGIC   *
# MAGIC FROM STREAM read_files(
# MAGIC   'dir_path',
# MAGIC   format => 'file_type'
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC | FEATURE                | CREATE TABLE AS (CTAS) + spark.read | COPY INTO                     | Auto Loader                                                                 |
# MAGIC |------------------------|-------------------------------------|-------------------------------|-----------------------------------------------------------------------------|
# MAGIC | **Ingestion Type**         | Batch                               | Incremental Batch             | Incremental (Batch or Streaming)                                            |
# MAGIC | **Use Cases**              | Best for smaller datasets           | Ideal for thousands of files  | Scale to millions+ of files per hour, backfills with billions of files     |
# MAGIC | **Syntax/Interface**       | ● Python (`spark.read`)               | SQL                           | ● Python (`spark.readStream`)                                                 |
# MAGIC |                        | ● SQL (CTAS)                        |                               | ● SQL with Declarative Pipelines (CREATE OR REFRESH STREAMING TABLES)       |
# MAGIC |                        |                                     |                               | ○ Use streaming tables in Databricks SQL                                    |
# MAGIC | **Idempotency**            | No                                  | Yes                           | Yes                                                                         |
# MAGIC | **Schema Evolution**       | Manual or inferred during read      | Supported with options        | Auto Loader automatically detects and evolves schemas. It supports loading data without predefined schemas and handles new columns as they appear. |
# MAGIC | **Latency**                | High                                | Moderate (scheduled)          | Low or high depending configuration                                        |
# MAGIC | Ease of Use            | Simple                              | Simple and SQL-based          | Intermediate to advanced depending on the implementation (Python or SQL, incremental batch or streaming) |
# MAGIC | **Summary**                | Best for one time, ad hoc ingestion. Can be scheduled to always read and process all data. | Simple and repeatable for incremental file ingestion. Great for scheduled jobs or pipelines. | Best for near real-time streaming or incremental ingestion, with high automation and scalability. |

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Appending Metadata Columns on Ingest**
# MAGIC
# MAGIC In data ingestion from cloud storage, metadata columns like source file name and modification time can be appended using the `_metadata` column.
# MAGIC
# MAGIC - `_metadata.file_modification_time`: Provides the last modification timestamp of the input file.
# MAGIC
# MAGIC - `_metadata.file_name`: Returns the name of the source file for each row.

# COMMAND ----------

# DBTITLE 1,Metadata Columns - SQL
# MAGIC %sql
# MAGIC SELECT
# MAGIC   _metadata.file_modification_time AS file_modification_time,
# MAGIC   _metadata.file_name AS file_name,
# MAGIC   current_timestamp() AS ingestion_time,
# MAGIC FROM
# MAGIC   read_files(
# MAGIC     'dir_path',
# MAGIC     format => 'file_type'
# MAGIC   );

# COMMAND ----------

# DBTITLE 1,Metadata Columns - Python
from pyspark.sql.functions import col, from_unixtime, current_timestamp
from pyspark.sql.types import DateType

df = spark.read.format("csv").load("some_csv/path/file.csv")

df_with_metadata = (
    df.withColumn("first_touch_date", from_unixtime(col("user_first_touch_timestamp") / 1_000_000).cast(DateType()))
      .withColumn("file_modification_time", col("_metadata.file_modification_time"))
      .withColumn("source_file", col("_metadata.file_name"))
      .withColumn("ingestion_time", current_timestamp())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Rescued Data**
# MAGIC
# MAGIC During data ingestion there are times when the input data doesn't match with the schema in your table. Ingestion techniques like `read_files()`, `spark.read` or Auto Loader provide a rescued data column during ingestion. The rescued data column **ensures that columns that don't match with the schema are rescued instead of being dropped.**
# MAGIC
# MAGIC ![](https://cdn5.dcbstatic.com/files/d/a/databricks_docebosaas_com/1767819600/VyoFcUu7LxPchoEE_q4uyQ/authoring/674/674_full_slide3_1.jpg)

# COMMAND ----------

# DBTITLE 1,Rescued Data - Python
df = (spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "json")
  # Aquí defines el nombre de la columna que guardará los errores
  .option("rescuedDataColumn", "_rescued_data") 
  .option("cloudFiles.schemaLocation", path_schema)
  .load(path_data))

# COMMAND ----------

# DBTITLE 1,Rescued Data - SQL
# MAGIC %sql
# MAGIC CREATE OR REFRESH STREAMING TABLE my_table
# MAGIC AS
# MAGIC SELECT
# MAGIC   *,
# MAGIC   _rescued_data
# MAGIC FROM read_files(
# MAGIC   's3://your-bucket/path/',
# MAGIC   format => 'json',
# MAGIC   rescuedDataColumn => '_rescued_data'
# MAGIC );