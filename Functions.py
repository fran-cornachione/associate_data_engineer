# Databricks notebook source
# MAGIC %md
# MAGIC En Databricks SQL, las Functions (Funciones) son bloques de lógica reutilizables que encapsulan cálculos, transformaciones o reglas de negocio. En lugar de escribir la misma lógica compleja en diez consultas diferentes, creas una función y la llamas por su nombre.
# MAGIC
# MAGIC Desde la llegada de Unity Catalog, las funciones se han vuelto fundamentales porque ahora se pueden compartir entre diferentes usuarios, catálogos y notebooks de forma segura.

# COMMAND ----------

# MAGIC %md
# MAGIC **Tipos de Funciones**
# MAGIC
# MAGIC - **Scalar Functions (UDFs):** Toman uno o varios valores de entrada y devuelven un único valor. (Ej: Calcular el IVA de un precio).
# MAGIC
# MAGIC - **Table-Valued Functions (UDTFs):** Toman parámetros y devuelven una tabla completa. (Ej: Una función que genere una serie de fechas).
# MAGIC
# MAGIC - **Built-in Functions:** Las que ya vienen con Databricks (como `abs()`, `date_add()`, `substring()`).

# COMMAND ----------

# MAGIC %md
# MAGIC **1. Anatomía de una función (Scalar)**
# MAGIC
# MAGIC Para crear una función persistente en Unity Catalog, la sintaxis es muy clara:

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION catalog.schema.function_name(parameter1 TIPO, parameter2 TIPO)
# MAGIC RETURNS TIPO
# MAGIC [DETERMINISTIC | NOT DETERMINISTIC]
# MAGIC COMMENT 'Descripción de lo que hace'
# MAGIC RETURN lógica_de_la_función;

# COMMAND ----------

# MAGIC %md
# MAGIC - `DETERMINISTIC`: Significa que para la misma entrada, siempre dará la misma salida (importante para que Spark optimice la consulta).
# MAGIC
# MAGIC - `RETURN`: Aquí es donde vive la "magia" (la fórmula).

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Ejemplo:**
# MAGIC Imagina que tienes los precios de los tickets de vuelo en dólares, pero necesitas calcular el precio final con el Impuesto PAIS (30%) y la Retención (30%).

# COMMAND ----------

# DBTITLE 1,Crear función
# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION streaming.vuelos.calcular_precio_final(precio_usd DOUBLE)
# MAGIC RETURNS DOUBLE
# MAGIC DETERMINISTIC
# MAGIC COMMENT 'Calcula el precio final de un ticket en Argentina sumando impuestos (60% total)'
# MAGIC RETURN precio_usd * 1.60;

# COMMAND ----------

# DBTITLE 1,Usar función en una consulta
# MAGIC %sql
# MAGIC SELECT 
# MAGIC     id_vuelo, 
# MAGIC     precio_base, 
# MAGIC     streaming.vuelos.calcular_precio_final(precio_base) AS precio_con_impuestos
# MAGIC FROM streaming.vuelos.tickets;

# COMMAND ----------

# MAGIC %md
# MAGIC **Row and Column Filters**
# MAGIC
# MAGIC Row filters let you **control which rows a user can access in a table based on custom logic**. At query time, a row filter evaluates a condition and returns only the rows that meet it. This is commonly used to implement row-level security—for example, restricting users to records from a specific `region`, `department`, or `account`.
# MAGIC
# MAGIC Row filters are defined as SQL user-defined functions (UDFs), and can also incorporate Python or Scala logic when wrapped in a SQL UDF. You can apply row filters per table, or centrally through ABAC policies using governed tags.
# MAGIC
# MAGIC **Column Masks**
# MAGIC
# MAGIC Column masks **control what values users see in specific columns, depending on who they are**. At query time, the mask replaces each reference to a column with the result of a masking function. This allows sensitive data, such as SSNs or emails, to be redacted or transformed based on user identity or role.
# MAGIC
# MAGIC **Each column can have one mask.** The mask must be defined as a SQL UDF that returns a value of the same type as the column being masked. The SQL UDF can optionally [call Python or Scala UDFs](https://docs.databricks.com/aws/en/data-governance/unity-catalog/filters-and-masks/manually-apply#wrapper-example) to implement complex masking logic. Column masks can also take other columns as inputs, for example, to vary behavior based on multiple attributes.
# MAGIC
# MAGIC Like row filters, column masks can be applied per table or managed centrally through ABAC policies. They operate at query time and integrate seamlessly with standard SQL, notebooks, and dashboards.

# COMMAND ----------

# MAGIC %md
# MAGIC | Feature      | Applies to        | Managed using         | Naming impact              | Best used for...                               |
# MAGIC |--------------|-------------------|-----------------------|----------------------------|-----------------------------------------------|
# MAGIC | Dynamic views | Views            | SQL logic             | Creates a new object name  | Sharing filtered data or spanning multiple tables |
# MAGIC | Row filters   | Tables           | ABAC or mapping tables | Table name unchanged       | Row-level access control tied to user or data tags |
# MAGIC | Column masks  | Tables/columns   | ABAC or mapping tables | Table name unchanged       | Redacting sensitive column data based on identity |

# COMMAND ----------

# MAGIC %md
# MAGIC Queremos que `juan.perez@gmail.com` se convierta en `j***@gmail.com`.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION streaming.security.mask_email(email STRING)
# MAGIC RETURNS STRING
# MAGIC DETERMINISTIC
# MAGIC RETURN CONCAT(
# MAGIC     SUBSTRING(email, 1, 1), -- Toma la primera letra
# MAGIC     '***@', 
# MAGIC     SPLIT_PART(email, '@', 2) -- Toma todo lo que esté después del @
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC En lugar de dar acceso a la tabla original, le das acceso a los analistas a esta vista:

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VIEW streaming.vuelos.vw_pasajeros_seguro AS
# MAGIC SELECT 
# MAGIC     id_pasajero,
# MAGIC     streaming.security.mask_email(email) AS email_masked,
# MAGIC     nacionalidad
# MAGIC FROM streaming.vuelos.pasajeros;