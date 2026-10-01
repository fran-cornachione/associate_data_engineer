# Databricks notebook source
# MAGIC %md
# MAGIC - For conceptual information and an overview of using Python for pipelines, see [Develop pipeline code with Python.](https://docs.databricks.com/aws/en/ldp/developer/python-dev)

# COMMAND ----------

# MAGIC %md
# MAGIC Lakeflow Spark Declarative Pipelines Python functions are defined in the `pyspark.pipelines` module (imported as `dp`). Your pipelines implemented with the Python API must import this module:

# COMMAND ----------

from pyspark import pipelines as dp

# COMMAND ----------

# MAGIC %md
# MAGIC **Note**
# MAGIC
# MAGIC > The pipelines module is only available in the context of a pipeline. It is not available in Python running outside of pipelines. For more information about editing pipeline code, see Develop and debug ETL pipelines with the Lakeflow Pipelines Editor.

# COMMAND ----------

# MAGIC %md
# MAGIC Apache Spark includes declarative pipelines beginning in Spark 4.1, available through the `pyspark.pipelines` module. The Databricks Runtime extends these open source capabilities with additional APIs and integrations for managed production use.
# MAGIC
# MAGIC Code written with the open-source `pipelines` module runs without modification on Databricks. **The following features are not part of Apache Spark:**
# MAGIC
# MAGIC - `dp.create_auto_cdc_flow`
# MAGIC - `dp.create_auto_cdc_from_snapshot_flow`
# MAGIC - `@dp.expect(...)`
# MAGIC - `@dp.temporary_view`
# MAGIC
# MAGIC > The `pipelines` module was previously called `dlt` in Databricks. For details, and more information about the differences from Apache Spark, see [What happened to @dlt?.](https://docs.databricks.com/aws/en/ldp/developer/python-ref#dlt-or-pipeline)

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Functions for dataset definitions**
# MAGIC
# MAGIC Pipelines use Python decorators for defining datasets such as materialized views and streaming tables. See [Functions to define datasets.](https://docs.databricks.com/aws/en/ldp/developer/definition-function)

# COMMAND ----------

# MAGIC %md
# MAGIC The `@table` decorator can be used to define streaming tables in a pipeline.
# MAGIC
# MAGIC To define a streaming table, apply `@table` to a query that performs a streaming read against a data source or use the `create_streaming_table()` function.
# MAGIC
# MAGIC **Note**
# MAGIC
# MAGIC > In the older `dlt` module, the `@table` operator was used to create both streaming tables and materialized views. The `@table` operator in the `pyspark.pipelines` module still works in this way, but Databricks recommends using the `@materialized_view` operator to create materialized views.

# COMMAND ----------

# DBTITLE 1,Create Table (Streaming) - Syntax
from pyspark import pipelines as dp

@dp.table(
  name="<name>",
  comment="<comment>",
  spark_conf={"<key>" : "<value>", "<key>" : "<value>"},
  table_properties={"<key>" : "<value>", "<key>" : "<value>"},
  path="<storage-location-path>",
  partition_cols=["<partition-column>", "<partition-column>"],
  cluster_by_auto = <bool>,
  cluster_by = ["<clustering-column>", "<clustering-column>"],
  schema="schema-definition",
  row_filter = "row-filter-clause",
  private = <bool>)
  def your_function_name():
      return spark.readStream.table("catalog.schema.table")

# COMMAND ----------

# DBTITLE 1,Materialized Views
# Batch read on a table
@dp.materialized_view()
def function_name():
  return spark.read.table("catalog_name.schema_name.table_name")

# Batch read on a path
@dp.materialized_view()
def function_name():
  return spark.read.format("parquet").load("/Volumes/catalog_name/schema_name/volume_name/data_path")

# COMMAND ----------

# DBTITLE 1,Streaming Tables
# Streaming read on a table
@dp.table()
def function_name():
  return spark.readStream.table("catalog_name.schema_name.table_name")

# Streaming read on a path
@dp.table()
def function_name():
  return (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .load("/Volumes/catalog_name/schema_name/volume_name/data_path")
  )

# COMMAND ----------

# Specify a schema
sales_schema = StructType([
  StructField("customer_id", StringType(), True),
  StructField("customer_name", StringType(), True),
  StructField("number_of_line_items", StringType(), True),
  StructField("order_datetime", StringType(), True),
  StructField("order_number", LongType(), True)]
)
@dp.table(
  comment="Raw data on sales",
  schema=sales_schema)
def sales():
  return ("...")

# Specify a schema with SQL DDL, use a generated column, and set clustering columns
@dp.table(
  comment="Raw data on sales",
  schema="""
    customer_id STRING,
    customer_name STRING,
    number_of_line_items STRING,
    order_datetime STRING,
    order_number LONG,
    order_day_of_week STRING GENERATED ALWAYS AS (dayofweek(order_datetime))
    """,
  cluster_by = ["order_day_of_week", "customer_id"])
def sales():
  return ("...")

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Coding requirements for Python pipelines**
# MAGIC
# MAGIC The following are important requirements when you implement pipelines with the Lakeflow Spark Declarative Pipelines (SDP) Python interface:
# MAGIC
# MAGIC - SDP evaluates the code that defines a pipeline multiple times during planning and pipeline runs. Python functions that define datasets should include only the code required to define the table or view. Arbitrary Python logic included in dataset definitions might lead to unexpected behavior.
# MAGIC
# MAGIC - Do not try to implement custom monitoring logic in your dataset definitions. See [Define custom monitoring of pipelines with event hooks.](https://docs.databricks.com/aws/en/ldp/event-hooks)
# MAGIC
# MAGIC - The function used to define a dataset must return a Spark DataFrame. Do not include logic in your dataset definitions that does not relate to a returned DataFrame.
# MAGIC
# MAGIC - Never use methods that save or write to files or tables as part of your pipeline dataset code.
# MAGIC
# MAGIC **Examples of Apache Spark operations that should never be used in pipeline code:**
# MAGIC
# MAGIC - `collect()`
# MAGIC - `count()`
# MAGIC - `toPandas()`
# MAGIC - `save()`
# MAGIC - `saveAsTable()`
# MAGIC - `start()`
# MAGIC - `toTable()`

# COMMAND ----------

# MAGIC %md
# MAGIC ### **What happened to `@dlt`?**
# MAGIC
# MAGIC Previously, Databricks used the `dlt` module to support pipeline functionality. The `dlt` module has been replaced by the `pyspark.pipelines` module. You may still use `dlt`, but Databricks recommends using `pipelines`.
# MAGIC
# MAGIC The following table shows the differences in syntax and functionality between DLT, Lakeflow Spark Declarative Pipelines, and Apache Spark Declarative Pipelines.
# MAGIC
# MAGIC | Area                | DLT Syntax                              | SDP Syntax (Lakeflow and Apache, where applicable)    | Available in Apache Spark |
# MAGIC |---------------------|-----------------------------------------|-------------------------------------------------------|---------------------------|
# MAGIC | Imports             | `import dlt`                            | `from pyspark import pipelines` (as `dp`, optionally) | Yes                       |
# MAGIC | Streaming Table     | `@dlt.table` with a streaming dataframe | `@dp.table`                                           | Yes                       |
# MAGIC | Materialized View   | `@dlt.table` with a batch dataframe     | `@dp.materialized_view`                               | Yes                       |
# MAGIC | Append Flow         | `@dlt.append_flow`                      | `@dp.append_flow`                                     | Yes                       |
# MAGIC | SQL - Streaming     | `CREATE STREAMING TABLE...`             | `CREATE STREAMING TABLE...`                           | Yes                       |
# MAGIC | SQL - Materialized  | `CREATE MATERIALIZED VIEW...`           | `CREATE MATERIALIZED VIEW...`                         | Yes                       |
# MAGIC | SQL - Flow          | `CREATE FLOW...`                        | `CREATE FLOW...`                                      | Yes                       |
# MAGIC | Event Log           | `spark.read.table("event_log")`         | `spark.read.table("event_log")`                       | No                        |
# MAGIC | Apply Changes (CDC) | `dlt.apply_changes(...)`                | `dp.create_auto_cdc_flow(...)`                        | No                        |
# MAGIC | Expectations        | `@dlt.expect(...)`                      | `@dp.expect(...)`                                     | No                        |
# MAGIC | Continuous mode     | Pipeline config with continuous trigger | (same)                                                | No                        |
# MAGIC | Sink                | `@dlt.create_sink`                      | `@dp.create_sink`                                     | Yes                       |