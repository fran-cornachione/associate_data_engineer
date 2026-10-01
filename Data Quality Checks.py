# Databricks notebook source
# MAGIC %md
# MAGIC ## **Manage Data Quality with Pipeline Expectations**
# MAGIC
# MAGIC Use expectations to apply quality constraints that validate data as it flows through ETL pipelines. Expectations provide greater insight into data quality metrics and allow you to fail updates or drop records when detecting invalid records.
# MAGIC
# MAGIC This article has an overview of expectations, including syntax examples and behavior options. For more advanced use cases and recommended best practices, see [Expectation recommendations and advanced patterns.](https://docs.databricks.com/aws/en/ldp/expectation-patterns)
# MAGIC
# MAGIC ![](https://docs.databricks.com/aws/en/assets/images/expectations-flow-graph-02ab5dd2011b18ad791c67c0e8449af6.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ### **What are expectations?**
# MAGIC
# MAGIC Expectations are optional clauses in pipeline materialized view, streaming table, or view creation statements that apply data quality checks on each record passing through a query. Expectations use standard SQL Boolean statements to specify constraints. You can combine multiple expectations for a single dataset and set expectations across all dataset declarations in a pipeline.
# MAGIC
# MAGIC The following sections introduce the three components of an expectation and provide syntax examples.
# MAGIC
# MAGIC **Expectation name**
# MAGIC
# MAGIC Each expectation must have a name, which is used as an identifier to track and monitor the expectation. Choose a name that communicates the metrics being validated. The following example defines the expectation `valid_customer_age` to confirm that `age` is between 0 and 120 years:

# COMMAND ----------

# MAGIC %md
# MAGIC > An expectation name must be unique for a given dataset. You can reuse expectations across multiple datasets in a pipeline. [See Portable and reusable expectations.](https://docs.databricks.com/aws/en/ldp/expectation-patterns#reusable-expectations)

# COMMAND ----------

@dp.table
@dp.expect("valid_customer_age", "age BETWEEN 0 AND 120")
def customers():
  return spark.readStream.table("datasets.samples.raw_customers")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REFRESH STREAMING TABLE customers(
# MAGIC   CONSTRAINT valid_customer_age EXPECT (age BETWEEN 0 AND 120)
# MAGIC ) AS SELECT * FROM STREAM(datasets.samples.raw_customers);

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Constraint to evaluate**
# MAGIC
# MAGIC The constraint clause is a SQL conditional statement that must evaluate to true or false for each record. The constraint contains the actual logic for what is being validated. When a record fails this condition, the expectation is triggered.
# MAGIC
# MAGIC Constraints must use valid SQL syntax and cannot contain the following:
# MAGIC
# MAGIC - Custom Python functions
# MAGIC
# MAGIC - External service calls
# MAGIC
# MAGIC - Subqueries referencing other tables
# MAGIC
# MAGIC The following are examples of constraints that could be added to dataset creation statements:

# COMMAND ----------

# MAGIC %md
# MAGIC The syntax for a constraint in Python is:

# COMMAND ----------

# DBTITLE 1,Constraint Syntax - Python
@dp.expect(<constraint-name>, <constraint-clause>)

# COMMAND ----------

# MAGIC %md
# MAGIC Multiple constraints can be specified:

# COMMAND ----------

@dp.expect(<constraint-name>, <constraint-clause>)
@dp.expect(<constraint2-name>, <constraint2-clause>)

# COMMAND ----------

# MAGIC %md
# MAGIC Examples:

# COMMAND ----------

# Simple constraint
@dp.expect("non_negative_price", "price >= 0")

# SQL functions
@dp.expect("valid_date", "year(transaction_date) >= 2020")

# CASE statements
@dp.expect("valid_order_status", """
   CASE
     WHEN type = 'ORDER' THEN status IN ('PENDING', 'COMPLETED', 'CANCELLED')
     WHEN type = 'REFUND' THEN status IN ('PENDING', 'APPROVED', 'REJECTED')
     ELSE false
   END
""")

# Multiple constraints
@dp.expect("non_negative_price", "price >= 0")
@dp.expect("valid_purchase_date", "date <= current_date()")

# Complex business logic
@dp.expect(
  "valid_subscription_dates",
  """start_date <= end_date
    AND end_date <= current_date()
    AND start_date >= '2020-01-01'"""
)

# Complex boolean logic
@dp.expect("valid_order_state", """
   (status = 'ACTIVE' AND balance > 0)
   OR (status = 'PENDING' AND created_date > current_date() - INTERVAL 7 DAYS)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC The syntax for a constraint in SQL is:

# COMMAND ----------

# DBTITLE 1,Constraint Syntax - SQL
# MAGIC %sql
# MAGIC CONSTRAINT <constraint-name> EXPECT ( <constraint-clause> ) [ON VIOLATION action];

# COMMAND ----------

# MAGIC %md
# MAGIC Multiple constraints must be separated by a comma:

# COMMAND ----------

# MAGIC %sql
# MAGIC CONSTRAINT <constraint-name> EXPECT ( <constraint-clause> ) [ON VIOLATION action],
# MAGIC CONSTRAINT <constraint2-name> EXPECT ( <constraint2-clause> ) [ON VIOLATION action];

# COMMAND ----------

# MAGIC %md
# MAGIC Examples

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Simple constraint
# MAGIC CONSTRAINT non_negative_price EXPECT (price >= 0);
# MAGIC
# MAGIC -- SQL functions
# MAGIC CONSTRAINT valid_date EXPECT (year(transaction_date) >= 2020);
# MAGIC
# MAGIC -- CASE statements
# MAGIC CONSTRAINT valid_order_status EXPECT (
# MAGIC   CASE
# MAGIC     WHEN type = 'ORDER' THEN status IN ('PENDING', 'COMPLETED', 'CANCELLED')
# MAGIC     WHEN type = 'REFUND' THEN status IN ('PENDING', 'APPROVED', 'REJECTED')
# MAGIC     ELSE false
# MAGIC   END
# MAGIC );
# MAGIC
# MAGIC -- Multiple constraints
# MAGIC CONSTRAINT non_negative_price EXPECT (price >= 0),
# MAGIC CONSTRAINT valid_purchase_date EXPECT (date <= current_date());
# MAGIC
# MAGIC -- Complex business logic
# MAGIC CONSTRAINT valid_subscription_dates EXPECT (
# MAGIC   start_date <= end_date
# MAGIC   AND end_date <= current_date()
# MAGIC   AND start_date >= '2020-01-01'
# MAGIC );
# MAGIC
# MAGIC -- Complex boolean logic
# MAGIC CONSTRAINT valid_order_state EXPECT (
# MAGIC   (status = 'ACTIVE' AND balance > 0)
# MAGIC   OR (status = 'PENDING' AND created_date > current_date() - INTERVAL 7 DAYS)
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Action on invalid record**
# MAGIC
# MAGIC You must specify an action to determine what happens when a record fails the validation check. The following table describes the available actions:
# MAGIC
# MAGIC | Action (default: warn) | SQL syntax                           | Python syntax         | Result                                                                                                                              |
# MAGIC |------------------------|--------------------------------------|-----------------------|-------------------------------------------------------------------------------------------------------------------------------------|
# MAGIC | **WARN**                   | `EXPECT`                               | `dp.expect`             | Invalid records are written to the target.                                                                                          |
# MAGIC | **DROP**                   | `EXPECT . . . ON VIOLATION DROP ROW`   | `dp.expect_or_drop `    | Invalid records are dropped before data is written to the target. The count of dropped records is logged alongside other dataset metrics. |
# MAGIC | **FAIL**                   | `EXPECT . . . ON VIOLATION FAIL UPDATE` | `dp.expect_or_fail`     | Invalid records prevent the update from succeeding. Manual intervention is required before reprocessing. This expectation causes a failure of a single flow and does not cause other flows in your pipeline to fail. |

# COMMAND ----------

# MAGIC %md
# MAGIC You can also implement advanced logic to quarantine invalid records without failing or dropping data. See [Quarantine invalid records.](https://docs.databricks.com/aws/en/ldp/expectation-patterns#quarantine-invalid-data)

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Expectation tracking metrics**
# MAGIC
# MAGIC You can see tracking metrics for `warn` or `drop` actions from the pipeline UI. Because fail causes the update to fail when an invalid record is detected, metrics are not recorded.
# MAGIC
# MAGIC To view expectation metrics, complete the following steps:
# MAGIC
# MAGIC In your Databricks workspace's sidebar, click Jobs & Pipelines.
# MAGIC
# MAGIC - Click the Name of your pipeline.
# MAGIC
# MAGIC - Click a dataset with an expectation defined.
# MAGIC
# MAGIC - Select the Data quality tab in the right sidebar.
# MAGIC
# MAGIC You can view data quality metrics by querying the Lakeflow Spark Declarative Pipelines event log. See Query data quality or expectations metrics.
# MAGIC
# MAGIC ### **Retain invalid records**
# MAGIC
# MAGIC Retaining invalid records is the default behavior for expectations. Use the `expect` operator when you want to keep records that violate the expectation but collect metrics on how many records pass or fail a constraint. Records that violate the expectation are added to the target dataset along with valid records:

# COMMAND ----------

# DBTITLE 1,Retain Valid Records - Python
@dp.expect("valid timestamp", "timestamp > '2012-01-01'")

# COMMAND ----------

# DBTITLE 1,Retain Valid Records - SQL
# MAGIC %sql
# MAGIC CONSTRAINT valid_timestamp EXPECT (timestamp > '2012-01-01')

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Drop invalid records**
# MAGIC
# MAGIC Use the `expect_or_drop` operator to prevent further processing of invalid records. Records that violate the expectation are dropped from the target dataset:

# COMMAND ----------

# DBTITLE 1,Drop Invalid Records - Python
@dp.expect_or_drop("valid_current_page", "current_page_id IS NOT NULL AND current_page_title IS NOT NULL")

# COMMAND ----------

# DBTITLE 1,Drop Invalid Records - SQL
# MAGIC %sql
# MAGIC CONSTRAINT valid_current_page EXPECT (current_page_id IS NOT NULL and current_page_title IS NOT NULL) ON VIOLATION DROP ROW

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Fail on invalid records**
# MAGIC
# MAGIC When invalid records are unacceptable, use the `expect_or_fail` operator to stop execution immediately when a record fails validation. If the operation is a table update, the system atomically rolls back the transaction:

# COMMAND ----------

# DBTITLE 1,Fail on Invalid Records - Python
@dp.expect_or_fail("valid_count", "count > 0")

# COMMAND ----------

# DBTITLE 1,Fail on Invalid Records - SQL
# MAGIC %sql
# MAGIC CONSTRAINT valid_count EXPECT (count > 0) ON VIOLATION FAIL UPDATE