# Databricks notebook source
# MAGIC %md
# MAGIC # Structured Streaming en Spark
# MAGIC
# MAGIC El streaming en ingeniería de datos se refiere al procesamiento continuo y en tiempo real de datos que llegan en forma de flujos. A diferencia del procesamiento por lotes, donde los datos se procesan en bloques, el streaming permite la ingestión y transformación de datos de manera incremental y con baja latencia.
# MAGIC
# MAGIC ## Características del Streaming
# MAGIC
# MAGIC - **Procesamiento Continuo**: Los datos se procesan tan pronto como llegan, lo que permite respuestas rápidas y actualizaciones en tiempo real.
# MAGIC - **Baja Latencia**: Ideal para aplicaciones que requieren resultados inmediatos, como dashboards en vivo y sistemas de alerta.
# MAGIC - **Ingestión Incremental**: Solo se procesan los nuevos datos que llegan desde la última ejecución, optimizando el uso de recursos.
# MAGIC
# MAGIC ## Fuentes Comunes de Datos de Streaming
# MAGIC
# MAGIC - **Apache Kafka**: Un sistema de mensajería distribuido que permite la publicación y suscripción de flujos de datos.
# MAGIC - **Amazon Kinesis**: Un servicio de AWS para la ingestión y procesamiento de datos en tiempo real.
# MAGIC - **Sockets TCP**: Permiten la transmisión de datos en tiempo real a través de redes.
# MAGIC
# MAGIC ## Uso en Databricks
# MAGIC
# MAGIC En Databricks, el streaming se puede implementar utilizando tablas de streaming, que son tablas registradas en Unity Catalog con soporte adicional para el procesamiento incremental de datos. Estas tablas permiten la carga de datos desde fuentes como Kafka y almacenamiento en la nube, y se actualizan automáticamente en cada ejecución.
# MAGIC
# MAGIC ### Ventajas de las Tablas de Streaming
# MAGIC
# MAGIC - **Manejo de Grandes Volúmenes de Datos**: Ideal para flujos de datos que crecen rápidamente.
# MAGIC - **Transformaciones de Baja Latencia**: Permiten razonamientos sobre filas y ventanas de tiempo con alta eficiencia.
# MAGIC - **Actualizaciones Incrementales**: Cada actualización de la tabla de streaming lee la información cambiada en la fuente de streaming y agrega nueva información a la tabla.
# MAGIC
# MAGIC El streaming es una técnica poderosa en ingeniería de datos que permite la creación de sistemas reactivos y en tiempo real, optimizando el procesamiento y la respuesta a eventos de datos continuos.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # ¿Qué es el Structured Streaming?
# MAGIC
# MAGIC Structured Streaming es un motor de procesamiento en tiempo casi real que ofrece tolerancia a fallos de extremo a extremo con garantías de procesamiento exactamente una vez utilizando las API familiares de Spark. Permite expresar cálculos en datos de streaming de la misma manera que se expresan en datos estáticos por lotes. El motor de Structured Streaming realiza el cálculo de manera incremental y actualiza continuamente el resultado a medida que llegan los datos de streaming.
# MAGIC
# MAGIC ## Características Principales
# MAGIC
# MAGIC - **Procesamiento Incremental**: Los cálculos se realizan de manera incremental y continua.
# MAGIC - **Baja Latencia**: Puede lograr latencias de extremo a extremo tan bajas como 1 milisegundo en modo de procesamiento continuo.
# MAGIC - **Tolerancia a Fallos**: Garantiza exactamente una vez la tolerancia a fallos a través de checkpointing y Write-Ahead Logs.
# MAGIC - **API Familiar**: Utiliza las mismas API de DataFrame/Dataset que se usan para el procesamiento por lotes.

# COMMAND ----------

# MAGIC %md
# MAGIC # **Sintaxis**

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Read Stream**

# COMMAND ----------

df_stream = spark.readStream \
  .format("cloudFiles") \
  .option("cloudFiles.format", "csv") \
  .option("cloudFiles.schemaLocation", "my_path/checkpoints/autoloader_schema") \
  .load("my_path/raw_data/")

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Write Stream**

# COMMAND ----------

stream_query = df_transformado.writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("checkpointLocation", "my_path/checkpoints/delta_sink") \
  .trigger(processingTime='1 minute') \
  .toTable("nombre_esquema.tabla_destino")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Modos de Salida | Output Modes
# MAGIC
# MAGIC - **Complete Mode**: Se escribe toda la tabla de resultados actualizada en el almacenamiento externo.
# MAGIC - **Append Mode**: Solo se escriben las nuevas filas añadidas a la tabla de resultados desde el último disparador.
# MAGIC - **Update Mode**: Solo se escriben las filas que se actualizaron en la tabla de resultados desde el último disparador.
# MAGIC
# MAGIC Structured Streaming facilita la creación de aplicaciones de procesamiento de datos en tiempo real de manera eficiente y con garantías de consistencia.

# COMMAND ----------

# Append Mode
query = df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Complete Mode
query = df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

# Update Mode
query = df.writeStream \
    .outputMode("update") \
    .format("console") \
    .start()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Triggers
# MAGIC
# MAGIC La función `.trigger` en Spark Structured Streaming se utiliza para definir el momento en que se procesarán los datos de streaming. Especifica cómo y cuándo se deben generar los micro-lotes o si se debe utilizar el modo de procesamiento continuo. Aquí hay algunos modos comunes de disparadores (triggers):
# MAGIC
# MAGIC - **Default trigger**: Ejecuta micro-lotes tan pronto como sea posible. Espera a que termine un lote para empezar con otro.
# MAGIC
# MAGIC - **ProcessingTime trigger**: Ejecuta micro-lotes a intervalos de tiempo fijos.
# MAGIC
# MAGIC - **Available-now trigger**: Procesa todos los datos disponibles en múltiples micro-lotes y luego se detiene.
# MAGIC
# MAGIC - **Continuous trigger**: Ejecuta la consulta en modo de procesamiento continuo con baja latencia.
# MAGIC
# MAGIC - **Once trigger**: Ejecuta la consulta una sola vez y procesa todos los datos disponibles en ese momento.
# MAGIC
# MAGIC ## Ejemplos de Uso

# COMMAND ----------

# Default Trigger
df.writeStream \
  .format("console") \
  .start()


# ProcessingTime trigger con intervalo de dos segundos
df.writeStream \
  .format("console") \
  .trigger(processingTime='2 seconds') \
  .start()


# Available-now trigger
df.writeStream \
  .format("console") \
  .trigger(availableNow=True) \
  .start()


# Continuous trigger con intervalo de checkpoint de un segundo
df.writeStream \
  .format("console") \
  .trigger(continuous='1 second') \
  .start()


# Once trigger
df.writeStream \
  .format("console") \
  .trigger(once=True) \
  .start()

# COMMAND ----------

# MAGIC %md
# MAGIC # Carpetas
# MAGIC
# MAGIC ### Input / Source (opcional, depende de la fuente)
# MAGIC
# MAGIC - Si lees de un directorio de archivos (ej: CSV, JSON, Parquet en HDFS, S3, local), ese directorio es tu **input path**.
# MAGIC
# MAGIC - Spark va a estar mirando ese folder y cada archivo nuevo que caiga lo va a procesar.
# MAGIC
# MAGIC - Si la fuente es Kafka, Kinesis, etc., **no necesitás crear carpetas de input**, porque el stream viene directo del sistema.
# MAGIC
# MAGIC ### Checkpoint Location (OBLIGATORIO en la mayoría de casos)
# MAGIC
# MAGIC Es un directorio donde Spark guarda metadatos del stream:
# MAGIC
# MAGIC - Offset de cada fuente (por dónde va leyendo)
# MAGIC
# MAGIC - Estado de agregaciones con `withWatermark` o `groupBy`
# MAGIC
# MAGIC - Commits de micro-batch ya procesados
# MAGIC
# MAGIC Sirve para:
# MAGIC
# MAGIC - **Recuperación:** si el job se cae, Spark arranca desde el último estado consistente.
# MAGIC
# MAGIC - **Exactly-once:** evitar procesar dos veces el mismo batch.
# MAGIC
# MAGIC Sin esto, si el job se reinicia, Spark no sabe qué ya procesó → ***duplicados***.
# MAGIC
# MAGIC **Ejemplo:**

# COMMAND ----------

query = df.writeStream \
    .format("parquet") \
    .option("checkpointLocation", "s3://mi-bucket/checkpoints/ventas/") \
    .option("path", "s3://mi-bucket/output/ventas/") \
    .start()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Output / Sink
# MAGIC
# MAGIC - Es el directorio donde vas a persistir los resultados del stream si escribís a archivos.
# MAGIC
# MAGIC - Puede ser parquet, delta, csv, etc.
# MAGIC
# MAGIC - **Ojo:** en streaming Spark no sobreescribe, siempre appendea.
# MAGIC
# MAGIC ### Temp / Metadata interna (opcional)
# MAGIC
# MAGIC - Algunas veces Spark o el formato (por ejemplo Delta Lake) crean carpetas auxiliares:
# MAGIC
# MAGIC - `_spark_metadata`: info sobre qué archivos ya se procesaron.
# MAGIC
# MAGIC - `_delta_log`: si usás Delta Lake, guarda el transaction log.
# MAGIC
# MAGIC Estas se crean automáticamente, no hace falta armarlas vos.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Windowing
# MAGIC
# MAGIC En streaming, los datos son unbounded (no tienen fin).
# MAGIC Por lo tanto, no podés ejecutar agregaciones globales como en batch, porque el stream nunca termina:
# MAGIC
# MAGIC Windowing es una estrategia de time-based aggregation que transforma un stream infinito en subconjuntos finitos basados en una columna temporal (event-time).
# MAGIC
# MAGIC - **Event Time** → cuándo ocurrió el evento (columna del dataset).
# MAGIC
# MAGIC - **Processing Time** → cuándo Spark procesa el evento.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tipos de Ventana en Spark

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Tumbling Window**
# MAGIC
# MAGIC Ventanas fijas, no superpuestas.
# MAGIC
# MAGIC **Ejemplo:** 5 minutos.

# COMMAND ----------

# DBTITLE 1,Tumbling Window
from pyspark.sql.functions import window, col

df.groupBy(
    window(col("event_time"), "5 minutes")
).count()

# COMMAND ----------

# MAGIC %md
# MAGIC Si llegan eventos:
# MAGIC
# MAGIC 10:01
# MAGIC
# MAGIC 10:02
# MAGIC
# MAGIC 10:06
# MAGIC
# MAGIC 10:07
# MAGIC
# MAGIC Las ventanas serán:
# MAGIC
# MAGIC - [10:00–10:05)
# MAGIC
# MAGIC - [10:05–10:10)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Sliding Window
# MAGIC
# MAGIC Ventanas superpuestas.
# MAGIC
# MAGIC **Ejemplo:** Ventana de 10 minutos que se desliza cada 5 minutos.

# COMMAND ----------

# DBTITLE 1,Sliding Window - Example
df.groupBy(
    window(col("event_time"), "10 minutes", "5 minutes")
).count()

# COMMAND ----------

# MAGIC %md
# MAGIC Significa:
# MAGIC
# MAGIC - Cada ventana dura 10 min
# MAGIC - Se crea una nueva cada 5 min

# COMMAND ----------

# MAGIC %md
# MAGIC [10:00–10:10)
# MAGIC
# MAGIC [10:05–10:15)
# MAGIC
# MAGIC [10:10–10:20)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Session Window
# MAGIC
# MAGIC Basada en inactividad
# MAGIC
# MAGIC La ventana se mantiene abierta mientras no haya un gap mayor a 10 minutos.

# COMMAND ----------

# DBTITLE 1,Session Window - Example
from pyspark.sql.functions import session_window

df.groupBy(
    session_window(col("event_time"), "10 minutes"),
    col("user_id")
).count()