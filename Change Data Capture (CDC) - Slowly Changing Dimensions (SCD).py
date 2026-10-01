# Databricks notebook source
# MAGIC %md
# MAGIC ### **Change Data Feed (CDF)**
# MAGIC
# MAGIC El Change Data Feed (o Registro de Cambios de Datos) es una característica que registra a nivel de fila y columna todos los cambios realizados en una tabla Delta, incluyendo inserciones (`INSERT`), eliminaciones (`DELETE`) y actualizaciones (`UPDATE`).
# MAGIC
# MAGIC En lugar de solo registrar la operación (como lo hace el Registro de Transacciones en la carpeta `_delta_log/`), el CDF registra qué filas exactas fueron modificadas y cómo cambiaron.
# MAGIC
# MAGIC **Analizando la Evolución**
# MAGIC
# MAGIC - **Registro de Transacciones (`_delta_log)`:** Te dice qué archivos Parquet son válidos en cada versión. Te permite hacer Time Travel (consultar el estado de la tabla en el pasado).
# MAGIC
# MAGIC - **Change Data Feed (CDF):** Te dice qué filas específicas y columnas cambiaron entre la Versión `N` y la Versión `N+1`. Te permite hacer streaming o procesamiento por lotes solo de los cambios.
# MAGIC
# MAGIC **Usos y Beneficios del CDF**
# MAGIC
# MAGIC El CDF resuelve el problema de tener que procesar la tabla completa solo para encontrar los cambios.
# MAGIC
# MAGIC **1. Replicación y Sincronización**
# MAGIC
# MAGIC Es el caso de uso más común. Permite construir pipelines que replican o sincronizan una tabla Delta grande (la Fuente) con otra tabla o sistema de destino (el Sink) de manera eficiente.
# MAGIC
# MAGIC - **Ejemplo:** Tienes una tabla Platinum (Agregada) y quieres actualizarla inmediatamente después de que la tabla Gold (Base de Datos Operacional) cambie. En lugar de procesar toda la tabla Gold, solo lees el CDF para ver las 100 filas que cambiaron y las aplicas al Platinum.
# MAGIC
# MAGIC **2. Auditoría y Cumplimiento**
# MAGIC
# MAGIC Permite auditar con precisión quién, cuándo y cómo cambió una fila específica, lo cual es fundamental para el cumplimiento normativo.
# MAGIC
# MAGIC **3. Propagación de Datos Eficiente**
# MAGIC
# MAGIC Esencial para la Arquitectura Medallion (Bronze → Silver → Gold) para crear pipelines incrementales que solo propagan los cambios:
# MAGIC
# MAGIC - **Sin CDF:** Para actualizar la capa Silver, tendrías que hacer un `MERGE` comparando toda la capa Bronze con la Silver.
# MAGIC
# MAGIC - **Con CDF:** Simplemente lees el CDF de la capa Bronze para obtener solo las filas que cambiaron y las aplicas a la capa Silver. Esto reduce drásticamente el tiempo de procesamiento y el costo.
# MAGIC
# MAGIC **Habilitación y Uso**
# MAGIC
# MAGIC **1. Habilitación**
# MAGIC
# MAGIC Para usar el CDF, primero debes habilitarlo en tu tabla Delta. Puedes hacerlo en la creación de la tabla o en una tabla existente:

# COMMAND ----------

# DBTITLE 1,Activar CDF en una tabla existente
# MAGIC %sql
# MAGIC -- Habilitar CDF en una tabla existente
# MAGIC ALTER TABLE my_table SET TBLPROPERTIES (
# MAGIC   'delta.enableChangeDataFeed' = 'true'
# MAGIC );

# COMMAND ----------

# DBTITLE 1,Crear tabla con CDF
# MAGIC %sql
# MAGIC -- Habilitar al crear una tabla
# MAGIC CREATE TABLE nombre_de_la_tabla (
# MAGIC   id INT,
# MAGIC   nombre STRING
# MAGIC )
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableChangeDataFeed' = 'true'
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC **Lectura de Cambios**
# MAGIC
# MAGIC Una vez habilitado, puedes leer el feed de cambios como si fuera una tabla normal, especificando el rango de versiones o marcas de tiempo:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Leer los cambios (INSERTs, UPDATEs, DELETEs) entre la versión 3 y la versión 5
# MAGIC SELECT * FROM table_changes('my_table')
# MAGIC WHERE _commit_version BETWEEN 3 AND 5;
# MAGIC
# MAGIC -- O leer los cambios desde un punto en el tiempo
# MAGIC SELECT * FROM table_changes('my_table')
# MAGIC WHERE _commit_timestamp > '2025-11-20 10:00:00';

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Slowly Changing Dimensions (SCD)**
# MAGIC
# MAGIC ### **SCD Type 0 (Retain Original)**
# MAGIC
# MAGIC Los datos nunca cambian. Una vez que se escriben, se quedan así para siempre.
# MAGIC
# MAGIC - **Ejemplo:** Fecha de nacimiento o el ID original de un contrato.

# COMMAND ----------

# MAGIC %md
# MAGIC ### **SCD Type 1 (Overwrite)**
# MAGIC
# MAGIC Cuando cambia una fila, el valor anterior es sobreescrito.
# MAGIC
# MAGIC - Aunque esta opción requiere almacenamiento mínimo, se pierde el tracking pasado.
# MAGIC
# MAGIC We have a `customers` table as our target. The table currently contains two customers:
# MAGIC
# MAGIC - `customer_id 1`, Peter 
# MAGIC - `customer_id 2`, Samarth
# MAGIC
# MAGIC We have an `updates` table as our source, this contains:
# MAGIC
# MAGIC - UPDATEs (Peter has had two updates on his address. One on 5/15 and the other on 5/20, `customer_id 1`)
# MAGIC - DELETEs (Samarth wants to be removed, `customer_id 2`)
# MAGIC - INSERTs (New customer Kostas, `customer_id 3`)
# MAGIC
# MAGIC Our goal is to update the `customers` table with the new customer information from the `updates` source table.
# MAGIC
# MAGIC ![](https://cdn5.dcbstatic.com/files/d/a/databricks_docebosaas_com/1768428000/4A17hB9gCkgWHmQbFFmZ7A/authoring/688/688_full_slide35_1.jpg)

# COMMAND ----------

# MAGIC %md
# MAGIC ### **SCD Type 2 (Add New Row)**
# MAGIC
# MAGIC Preserva el historial completo añadiendo una nueva fila cada vez que un atributo cambia.
# MAGIC
# MAGIC Para esto, añadimos columnas de control:
# MAGIC
# MAGIC - `is_current`: Un booleano (True/False).
# MAGIC
# MAGIC - `start_date` / `end_date`: Para saber el rango de validez.
# MAGIC
# MAGIC | `Customer_ID` | `Ciudad`       | `is_current` | `start_date` | `end_date`   |
# MAGIC |-------------|--------------|------------|------------|------------|
# MAGIC | 101         | Mendoza      | False      | 2024-01-01 | 2026-01-07 |
# MAGIC | 101         | Buenos Aires | True       | 2026-01-08 | NULL       |
# MAGIC
# MAGIC - Columns indicate indicate **active** and **inactive** rows 
# MAGIC
# MAGIC - **Null** values indicate the **current row**
# MAGIC
# MAGIC - **Non Null** values indicate **inactive (historic) rows**

# COMMAND ----------

# MAGIC %md
# MAGIC ![](https://images.squarespace-cdn.com/content/v1/65a7c5c379a13972f7c20d75/b9ce5f75-00c0-4843-9263-c982bd18a123/Managing+Data+Changes+with+SCDs+5.png?format=1500w)

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Applying Auto CDC on Lakeflow Declarative Pipelines**

# COMMAND ----------

# DBTITLE 1,Crear tablas para ejemplo
# MAGIC %sql
# MAGIC -- Customers (target table with all customers)
# MAGIC CREATE TABLE IF NOT EXISTS associate_data_engineer.default.customers (
# MAGIC   CustomerID INT,
# MAGIC   Name STRING,
# MAGIC   Address STRING,
# MAGIC   ProcessDate DATE
# MAGIC );
# MAGIC
# MAGIC -- Updates (source table with updates)
# MAGIC CREATE TABLE IF NOT EXISTS associate_data_engineer.default.updates (
# MAGIC   CustomerID INT,
# MAGIC   Name STRING,
# MAGIC   Address STRING,
# MAGIC   Operation STRING,
# MAGIC   ProcessDate DATE
# MAGIC );
# MAGIC
# MAGIC INSERT INTO associate_data_engineer.default.updates VALUES
# MAGIC   (1, 'Peter', '99 Sunny St.', 'UPDATE', '2025-05-15'), -- Peter cambia de dirección
# MAGIC   (1, 'Peter', '123 Main St.', 'UPDATE', '2025-05-20'),  -- Peter cambia de dirección otra vez
# MAGIC   (2, 'Samarth', '22 Front St', 'DELETE', '2025-05-20'), -- Eliminamos a Samarth
# MAGIC   (3, 'Kostas', '100 Athens Dr.', 'NEW', '2025-05-20'); -- Nuevo cliente
# MAGIC
# MAGIC INSERT INTO associate_data_engineer.default.customers VALUES
# MAGIC   (1, 'Peter', '1 Blue Rd.', '2025-05-01'),
# MAGIC   (2, 'Samarth', '22 Front St', '2025-05-01');

# COMMAND ----------

# DBTITLE 1,AUTO CDC
# MAGIC %sql
# MAGIC CREATE OR REFRESH STREAMING TABLE associate_data_engineer.default.customers; -- Create a streaming table, then use AUTO CDC to populate it
# MAGIC
# MAGIC CREATE FLOW scd_type_1_flow AS 
# MAGIC AUTO CDC INTO associate_data_engineer.default.customers -- Apply updates, inserts and deletes to the target table
# MAGIC   FROM STREAM associate_data_engineer.default.updates -- Source records to determine updates, deletes and inserts
# MAGIC   KEYS (CustomerID) -- The columns that uniquely identify a row in the source and target tables
# MAGIC APPLY AS DELETE WHEN operation = 'DELETE' -- Specifies when a CDC event should be treated as a DELETE rather than UPSERT
# MAGIC SEQUENCE BY ProcessDate -- Speficies the logical order of CDC events in the source data
# MAGIC COLUMNS * EXCEPT (operation) -- Specifies a subset of columns to include in the target
# MAGIC STORED AS SCD TYPE 1; -- Wheter to store records as SCD Type 1 (default) or Type 2