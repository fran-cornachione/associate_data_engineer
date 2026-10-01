# Databricks notebook source
{
  "name": "John Doe", 
  "age": 35, 
  "address": {
    "city": "Anytown", 
    "state": "CA"
    }, 
  "children": [
    {
      "name": "Owen", 
      "age": 10
    }, 
    {
      "name": "Eva", 
      "age": 8
    }
  ]
}

# COMMAND ----------

# MAGIC %md
# MAGIC ### **1. STRING Data Type**
# MAGIC
# MAGIC Working with JSON can be done using different **column data types**
# MAGIC
# MAGIC - JSON can be stored as a simple STRING
# MAGIC - Can hold any JSON STRING without constraints
# MAGIC - Less performant
# MAGIC
# MAGIC Use :(colon) syntax to access subfields in JSON formatting strings

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT json_column:name -- John Doe

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT json_column:address:city -- Anytown

# COMMAND ----------

# MAGIC %md
# MAGIC ### **2. STRUCT Data Type**
# MAGIC
# MAGIC - You can parse JSON data into a STRUCT type, with a defined schema
# MAGIC - STRUCT enforces the JSON schema
# MAGIC - Is more efficient for querying than a JSON formatted STRING

# COMMAND ----------

# MAGIC %md
# MAGIC | JSON String Types | Databricks SQL Data Type |
# MAGIC |-------------------|---------------------------|
# MAGIC | String            | STRING                    |
# MAGIC | Number            | INT/FLOAT/DOUBLE          |
# MAGIC | Boolean           | BOOLEAN                   |
# MAGIC | Object            | STRUCT <>                 |
# MAGIC | Array             | ARRAY <>                  |

# COMMAND ----------

# MAGIC %sql
# MAGIC STRUCT <
# MAGIC   name: STRING,
# MAGIC   age: INT,
# MAGIC   address: STRUCT <
# MAGIC     city: STRING,
# MAGIC     state: STRING
# MAGIC   >,
# MAGIC   children: ARRAY <
# MAGIC       STRUCT <
# MAGIC         name: STRING,
# MAGIC         age: INT
# MAGIC       >
# MAGIC     >
# MAGIC   >

# COMMAND ----------

# MAGIC %md
# MAGIC ### **3. VARIANT Data Type**
# MAGIC
# MAGIC - Can store any type of data, including JSON, and is ideal form semi-structured data
# MAGIC - Highly flexible
# MAGIC - Improved performance over existing methods

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Derive Schema of JSON formatted STRING**
# MAGIC
# MAGIC ![](https://cdn5.dcbstatic.com/files/d/a/databricks_docebosaas_com/1768172400/ttuuTUd_-EMMj77VgU-qyA/authoring/675/675_full_slide130_1.jpg)

# COMMAND ----------

# MAGIC %md
# MAGIC The `from_json` function returns a **struct column** using the JSON string and specified schema

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT from_json(json_column, 'json-struct-schema') AS struct_column FROM TABLE

# COMMAND ----------

# DBTITLE 1,Practice: Parse JSON with from_json
# Practice: Parse the JSON STRING using from_json into a STRUCT
# The schema mirrors the JSON structure described in Cell 7

schema = """
    STRUCT <
        name: STRING,
        age: INT,
        address: STRUCT <
            city: STRING,
            state: STRING
        >,
        children: ARRAY <
            STRUCT <
                name: STRING,
                age: INT
            >
        >
    >
"""

display(spark.sql(f"""
    SELECT from_json(json_column, '{schema}') AS struct_column
    FROM new_year.practice.person_json
"""))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     json_column:name,
# MAGIC     json_column:age::int -- Use :: for casting data types
# MAGIC     FROM 
# MAGIC         new_year.practice.person_json;