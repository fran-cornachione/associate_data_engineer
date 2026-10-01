# Databricks notebook source
# MAGIC %md
# MAGIC Imagina que tienes una biblioteca con 10 millones de libros tirados en una pila gigante en el suelo. Si te pido "todos los libros de 1995", tendrías que levantar y mirar cada uno de los 10 millones de libros para encontrar los correctos. Eso es una tabla sin particionar (Full Table Scan).
# MAGIC
# MAGIC Ahora, imagina que organizas esos libros en estantes separados por año. Si te pido "libros de 1995", vas directo al estante que dice "1995" e ignoras el resto de la biblioteca. Eso es Particionamiento.
# MAGIC
# MAGIC En el contexto de Big Data (Terabytes o Petabytes de datos), las particiones no son solo un "truco" de organización, son vitales para el rendimiento y el costo.

# COMMAND ----------

# MAGIC %md
# MAGIC ### **1. ¿Qué es exactamente una Partición?**
# MAGIC
# MAGIC Es una técnica para **_dividir una tabla lógica grande en piezas físicas más pequeñas y manejables_**. Aunque para el usuario sigue pareciendo una sola tabla, en el almacenamiento (disco/cloud storage), los datos están separados.
# MAGIC
# MAGIC En Big Data (como Spark, Hive, BigQuery, Snowflake), esto suele significar que **_los datos se guardan en carpetas diferentes._**
# MAGIC
# MAGIC **Ejemplo de almacenamiento físico:**
# MAGIC
# MAGIC - `/data/ventas/anio=2023/mes=01/archivo.parquet`
# MAGIC
# MAGIC - `/data/ventas/anio=2023/mes=02/archivo.parquet` 
# MAGIC
# MAGIC **2. El concepto clave: Partition Pruning (Poda)**
# MAGIC
# MAGIC Esta es la razón principal por la que particionamos. Cuando ejecutas una consulta SQL que filtra por la columna particionada, el motor de la base de datos es lo suficientemente inteligente como para **leer solo los archivos necesarios e ignorar el resto.**
# MAGIC
# MAGIC - **Sin Partición:** El motor lee 10 TB de datos para encontrar las ventas de ayer.
# MAGIC
# MAGIC - **Con Partición:** El motor lee solo 500 MB (la partición de ayer).
# MAGIC
# MAGIC **Impacto:**
# MAGIC
# MAGIC - **Velocidad:** La consulta es 100x más rápida.
# MAGIC
# MAGIC - **Costo:** En la nube (como Google BigQuery o AWS Athena), pagas por datos escaneados. Particionar reduce tu factura drásticamente.
# MAGIC
# MAGIC **3. Tipos comunes de Particionamiento**
# MAGIC
# MAGIC Dependiendo de cómo se distribuyan tus datos, elegirás una estrategia distinta:
# MAGIC
# MAGIC - **Por Rango (Range):** Ideal para fechas o números secuenciales. Es el estándar en Big Data.
# MAGIC
# MAGIC - **Ejemplo:** Particionar por `fecha_transaccion` (días, meses, años).
# MAGIC
# MAGIC - **Por Lista (List):** Para categorías con valores finitos y conocidos.
# MAGIC
# MAGIC **Ejemplo:** Particionar por pais o departamento.
# MAGIC
# MAGIC - **Por Hash:** Distribuye los datos aleatoriamente usando una función matemática sobre una clave (como el ID de usuario).
# MAGIC
# MAGIC - **Uso:** Se usa para evitar que una partición sea mucho más grande que otras (balanceo de carga), aunque es menos útil para filtros de rango.

# COMMAND ----------

# MAGIC %md
# MAGIC **4. Ejemplo en SQL**
# MAGIC
# MAGIC Supongamos que trabajamos con Spark SQL o Hive. Así se ve la creación de una tabla particionada:

# COMMAND ----------

# DBTITLE 1,Create Partitioned Table
# MAGIC %sql
# MAGIC CREATE TABLE logs_servidor (
# MAGIC     id_evento STRING,
# MAGIC     mensaje STRING,
# MAGIC     nivel_error STRING
# MAGIC )
# MAGIC PARTITIONED BY (fecha DATE, region STRING)
# MAGIC STORED AS PARQUET;

# COMMAND ----------

# MAGIC %md
# MAGIC Cómo consultarla para aprovechar la partición:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ✅ BIEN: Utiliza Partition Pruning
# MAGIC SELECT * FROM logs_servidor 
# MAGIC WHERE fecha = '2023-10-27' AND region = 'US-EAST';
# MAGIC -- El motor va directo a la carpeta /fecha=2023-10-27/region=US-EAST/
# MAGIC
# MAGIC -- ❌ MAL: Escaneo completo (muy lento y costoso)
# MAGIC SELECT * FROM logs_servidor 
# MAGIC WHERE mensaje LIKE '%Error crítico%';
# MAGIC -- El motor tiene que abrir TODAS las fechas y regiones.

# COMMAND ----------

# MAGIC %md
# MAGIC ### **5. Problemas comunes en Big Data (Lo que debes cuidar)**
# MAGIC
# MAGIC El particionamiento es poderoso, pero si se hace mal, puede destruir el rendimiento:
# MAGIC
# MAGIC **A. El problema de los "Small Files" (Archivos pequeños)**
# MAGIC
# MAGIC - Si particionas demasiado (ej. particionar por segundo o por `user_id` en una app con millones de usuarios), crearás millones de carpetas con archivos diminutos.
# MAGIC
# MAGIC - **Consecuencia:** El sistema de archivos (HDFS/S3) se satura gestionando metadatos en lugar de leer datos. Hadoop y Spark odian los archivos pequeños.
# MAGIC
# MAGIC **B. Data Skew (Sesgo de datos)**
# MAGIC
# MAGIC - Ocurre cuando una partición es gigante comparada con las otras.
# MAGIC
# MAGIC - **Ejemplo:** Particionar por pais y tener el 90% de los usuarios en "EE.UU." y el resto disperso.
# MAGIC
# MAGIC - **Consecuencia:** El procesamiento paralelo falla porque el "worker" que procesa EE.UU. tardará horas mientras los demás terminan en segundos.
# MAGIC
# MAGIC **C. High Cardinality (Alta Cardinalidad)**
# MAGIC
# MAGIC - Nunca particiones por una columna con valores únicos o casi únicos (como un DNI, ID de transacción o Email).