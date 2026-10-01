# Databricks notebook source
# MAGIC %md
# MAGIC ### Row Level Security
# MAGIC
# MAGIC Row-level security controls which rows a user can access based on conditions such as user identity, group membership, or data attributes. For example, you might restrict sales data so that users only see rows for their assigned region.
# MAGIC
# MAGIC **Example:** Suppose you have a table `sales` with a column `region`. You can define a row filter so that users in the "West" region only see rows where `region = 'West'`. In SDP, you can use the `row_filter` parameter:

# COMMAND ----------

# DBTITLE 1,Python - Row Level Security Example
@dp.table(
  name="regional_sales",
  row_filter="region = current_user_region()" # User can only see their regions rows
)
def regional_sales():
    return spark.readStream.table("catalog.schema.sales")

# COMMAND ----------

# DBTITLE 1,SQL - Row Level Security Example
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW 
# MAGIC   regional_sales 
# MAGIC AS SELECT 
# MAGIC   *
# MAGIC FROM 
# MAGIC   catalog.schema.sales
# MAGIC WHERE 
# MAGIC   region = CURRENT_USER_REGION();

# COMMAND ----------

# MAGIC %md
# MAGIC `SET ROW FILTER` is a Unity Catalog SQL statement used to define row-level security policies directly on tables. It attaches a filter condition to a table, so only rows matching the condition are visible to users or groups. 

# COMMAND ----------

# DBTITLE 1,SET ROW FILTER
# MAGIC %sql
# MAGIC SET ROW FILTER ON 
# MAGIC   catalog.schema.sales
# MAGIC FOR
# MAGIC   user_group
# MAGIC USING 
# MAGIC   region = 'West'

# COMMAND ----------

# MAGIC %md
# MAGIC This policy means members of `user_group` will only see rows where `region = 'West'` in the `sales` table.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Column Level Security
# MAGIC
# MAGIC Column-level security restricts access to specific columns, hiding sensitive fields from unauthorized users. For example, you might allow users to see sales amounts but not customer names or emails.
# MAGIC
# MAGIC **Example:** Suppose your `sales` table has columns `customer_name`, `email`, and `amount`. You can create a view that only exposes the `amount` column:

# COMMAND ----------

# DBTITLE 1,Python - Column Level Security Example
@dp.materialized_view(
  name="public_sales"
)
def public_sales():
    return spark.read.table("catalog.schema.sales").select("amount")

# COMMAND ----------

# DBTITLE 1,SQL - Column Level Security
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW
# MAGIC   public_sales
# MAGIC AS SELECT
# MAGIC   amount
# MAGIC FROM
# MAGIC   catalog.schema.sales

# COMMAND ----------

# MAGIC %md
# MAGIC ### Column Masks
# MAGIC
# MAGIC Column masks **control what values users see in specific columns, depending on who they are**. At query time, the mask replaces each reference to a column with the result of a masking function. This allows sensitive data, such as SSNs or emails, to be redacted or transformed based on user identity or role.
# MAGIC
# MAGIC **Each column can have one mask.** The mask must be defined as a SQL UDF that returns a value of the same type as the column being masked. The SQL UDF can optionally [call Python or Scala UDFs](https://docs.databricks.com/aws/en/data-governance/unity-catalog/filters-and-masks/manually-apply#wrapper-example) to implement complex masking logic. Column masks can also take other columns as inputs, for example, to vary behavior based on multiple attributes.
# MAGIC
# MAGIC Like row filters, column masks can be applied per table or managed centrally through ABAC policies. They operate at query time and integrate seamlessly with standard SQL, notebooks, and dashboards.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION streaming.security.mask_email(email STRING)
# MAGIC RETURNS STRING
# MAGIC DETERMINISTIC
# MAGIC RETURN CONCAT(
# MAGIC     SUBSTRING(email, 1, 1), -- Takes the first letter
# MAGIC     '***@', 
# MAGIC     SPLIT_PART(email, '@', 2) -- Takes everything after the @
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC | Feature      | Applies to        | Managed using         | Naming impact              | Best used for...                               |
# MAGIC |--------------|-------------------|-----------------------|----------------------------|-----------------------------------------------|
# MAGIC | Dynamic views | Views            | SQL logic             | Creates a new object name  | Sharing filtered data or spanning multiple tables |
# MAGIC | Row filters   | Tables           | ABAC or mapping tables | Table name unchanged       | Row-level access control tied to user or data tags |
# MAGIC | Column masks  | Tables/columns   | ABAC or mapping tables | Table name unchanged       | Redacting sensitive column data based on identity |

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Unity Catalog Policies**
# MAGIC
# MAGIC For advanced security, Unity Catalog supports row and column-level access policies using `GRANT` statements and attribute-based access control (ABAC).

# COMMAND ----------

# DBTITLE 1,Unity Catalog Policies
# MAGIC %sql
# MAGIC -- Grant access to only the 'amount' column
# MAGIC GRANT SELECT (amount) ON TABLE catalog.schema.sales TO user_group;
# MAGIC
# MAGIC -- Row-level filtering with dynamic views
# MAGIC CREATE OR REPLACE VIEW 
# MAGIC   regional_sales 
# MAGIC AS SELECT 
# MAGIC   *
# MAGIC FROM 
# MAGIC   catalog.schema.sales
# MAGIC WHERE 
# MAGIC   region = '${user.region}';

# COMMAND ----------

# MAGIC %md
# MAGIC `current_user`
# MAGIC
# MAGIC This function returns the username (email address) of the user executing the query, it's commonly used in row-level security to filter data so users only see their own records.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW my_data AS
# MAGIC SELECT *
# MAGIC FROM catalog.schema.table
# MAGIC WHERE owner_email = current_user();

# COMMAND ----------

# MAGIC %md
# MAGIC Here, only rows where `owner_email` matches the logged-in user’s email are shown.

# COMMAND ----------

# MAGIC %md
# MAGIC `is_account_member`
# MAGIC
# MAGIC This function checks if a given user is a member of your Databricks account. It's useful for access control, such as restricting data to only account members or specific groups.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM catalog.schema.table
# MAGIC WHERE is_account_member('user@example.com');

# COMMAND ----------

# MAGIC %md
# MAGIC This returns true if `user@example.com` is an account member.