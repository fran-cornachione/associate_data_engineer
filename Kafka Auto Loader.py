# Databricks notebook source
# MAGIC %md
# MAGIC Kafka es una plataforma distribuida de mensajería que permite que un sistema envíe datos y otro los reciba en tiempo real, sin que ambos tengan que estar conectados directamente.
# MAGIC
# MAGIC **Conceptos**
# MAGIC
# MAGIC - **Producer:** El que envía el dato (ej. un microservicio, un sensor).
# MAGIC
# MAGIC - **Consumer:** El que lee el dato (ej. Spark Structured Streaming).
# MAGIC
# MAGIC - **Broker:** El servidor de Kafka. Un "Cluster" es un conjunto de Brokers.
# MAGIC
# MAGIC - **Topic:** La "categoría" o "tabla" donde se guardan los mensajes.
# MAGIC
# MAGIC - **Offset:** Un número único que indica la posición de un mensaje dentro de una partición. Es vital para saber qué datos ya leímos.

# COMMAND ----------

# MAGIC %md
# MAGIC ![](https://media.geeksforgeeks.org/wp-content/uploads/20251112175818469025/apache_kafka_architecture.webp)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Estructura de un Topic
# MAGIC
# MAGIC Kafka no guarda todo en una sola bolsa. Divide los Topics en Particiones.
# MAGIC
# MAGIC - **Paralelismo:** Las particiones permiten que Spark lea datos en paralelo (un Worker de Spark puede leer la Partición 1 mientras otro lee la Partición 2).
# MAGIC
# MAGIC - **Orden:** Kafka solo garantiza el orden de los mensajes dentro de una misma partición.
# MAGIC
# MAGIC ![](https://miro.medium.com/0*6FVLBloofcJp_RsF.png)

# COMMAND ----------

# DBTITLE 1,Read - Syntax
df = (spark.readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "<server:ip>")
  .option("subscribe", "<topic>")
  .option("startingOffsets", "latest")
  .load()
)

# COMMAND ----------

# DBTITLE 1,Read - Example
df = spark.readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "host1:9092,host2:9092") \
  .option("subscribe", "ventas_real_time") \
  .option("startingOffsets", "latest") \
  .load()

# COMMAND ----------

# DBTITLE 1,Batch Read
df = (spark
  .read
  .format("kafka")
  .option("kafka.bootstrap.servers", "<server:ip>")
  .option("subscribe", "<topic>")
  .option("startingOffsets", "earliest")
  .option("endingOffsets", "latest")
  .load()
)

# COMMAND ----------

# MAGIC %md
# MAGIC - `startingOffsets`: * `earliest`: Lee desde el principio del topic.
# MAGIC
# MAGIC - `latest`: Lee solo lo que llegue a partir de ahora.

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Schema**
# MAGIC
# MAGIC | Column        | Type   |
# MAGIC |---------------|--------|
# MAGIC | `key`           | binary |
# MAGIC | `value`         | binary |
# MAGIC | `topic`         | string |
# MAGIC | `partition`     | int    |
# MAGIC | `offset`        | long   |
# MAGIC | `timestamp`     | long   |
# MAGIC | `timestampType` | int    |
# MAGIC
# MAGIC **Tip de examen:** Como el `value` es binario, casi siempre debes convertirlo a String y luego procesar el JSON:
# MAGIC `df.selectExpr("CAST(value AS STRING)")`

# COMMAND ----------

# MAGIC %md
# MAGIC ### Configuration
# MAGIC
# MAGIC | Option           | Value                                             | Description                             |
# MAGIC |------------------|---------------------------------------------------|-----------------------------------------|
# MAGIC | `subscribe`        | A comma-separated list of topics.                 | The topic list to subscribe to.         |
# MAGIC | `subscribePattern` | Java regex string.                                | The pattern used to subscribe to topic(s). |
# MAGIC | `assign`           | JSON string `{"topicA":[0,1],"topic":[2,4]}.`      | Specific topicPartitions to consume.    |

# COMMAND ----------

# MAGIC %md
# MAGIC #### `failOnDataLoss`
# MAGIC
# MAGIC - Si es `true` (por defecto), el stream fallará si Kafka borra datos que Spark aún no ha leído (porque expiró el tiempo de retención).
# MAGIC
# MAGIC - Si es `false`, Spark ignorará los datos perdidos y seguirá procesando.
# MAGIC
# MAGIC #### `maxOffsetsPerTrigger`
# MAGIC
# MAGIC - Esto controla la velocidad. Le dice a Spark: "En cada micro-batch, no leas más de X mensajes".
# MAGIC
# MAGIC - Es vital para evitar que tu cluster de Databricks se sature si hay un pico de datos repentino en Kafka.

# COMMAND ----------

# DBTITLE 1,subscribePattern - Example
.option("subscribePattern", "ventas_*")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lakeflow Declarative Pipelines
# MAGIC
# MAGIC **Example:** Write to a streaming table from multiple Kafka topics
# MAGIC
# MAGIC The following examples creates a streaming table named kafka_target and writes to that streaming table from two Kafka topics:

# COMMAND ----------

# DBTITLE 1,LDP - Python Example
from pyspark import pipelines as dp

dp.create_streaming_table("kafka_target")

# Kafka stream from multiple topics
@dp.append_flow(target = "kafka_target")
def topic1():
  return (
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "host1:port1,...")
      .option("subscribe", "topic1")
      .load()
  )

@dp.append_flow(target = "kafka_target")
def topic2():
  return (
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "host1:port1,...")
      .option("subscribe", "topic2")
      .load()
  )

# COMMAND ----------

# DBTITLE 1,LDP - SQL Example
# MAGIC %sql
# MAGIC CREATE OR REFRESH STREAMING TABLE kafka_target;
# MAGIC
# MAGIC CREATE FLOW
# MAGIC   topic1
# MAGIC AS INSERT INTO
# MAGIC   kafka_target BY NAME
# MAGIC SELECT * FROM
# MAGIC   read_kafka(bootstrapServers => 'host1:port1,...', subscribe => 'topic1');
# MAGIC
# MAGIC CREATE FLOW
# MAGIC   topic2
# MAGIC AS INSERT INTO
# MAGIC   kafka_target BY NAME
# MAGIC SELECT * FROM
# MAGIC   read_kafka(bootstrapServers => 'host1:port1,...', subscribe => 'topic2');