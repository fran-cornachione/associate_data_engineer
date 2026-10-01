# Databricks notebook source
# MAGIC %md
# MAGIC Las **Lakeflow Declarative Pipelines** se refieren al enfoque moderno de ingeniería de datos en Databricks, implementado principalmente a través de **Delta Live Tables** (DLT), donde te centras en qué tablas quieres construir y cómo se relacionan, en lugar de en el paso a paso operacional.
# MAGIC
# MAGIC Es un cambio de paradigma de la programación tradicional imperativa a la declarativa.

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Declarativo vs. Imperativo:** El Cambio de Paradigma
# MAGIC
# MAGIC | Enfoque                 | Imperativo (Tradicional Spark/Python)                                 | Declarativo (Lakerflow / DLT)                               |
# MAGIC |-------------------------|------------------------------------------------------------------------|-------------------------------------------------------------|
# MAGIC | Enfoque                 | En la ejecución: Escribes código para cómo hacer las cosas (ej. read, transform, write y OPTIMIZE manualmente). | En el resultado: Defines la tabla de destino y su lógica de negocio. |
# MAGIC | Flujo de Control        | Manual. Debes gestionar la orquestación (Airflow, Jobs), la gestión de estados y el manejo de errores. | Automático. El motor DLT gestiona la orquestación, el encendido/pagado de clusters, y el stream o batch de la operación. |
# MAGIC | Creación de Tablas      | `spark.write.format("delto").saveAsTable(...)`                         | `CREATE LIVE TABLE` (SQL) o `@dlt.table` (Python).          |

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Delta Live Tables (DLT):** La Implementación del Lakeflow
# MAGIC DLT es el framework que te permite construir pipelines declarativos. El engineer se enfoca en la lógica de negocio, y DLT se encarga automáticamente de todo lo demás:
# MAGIC
# MAGIC **Orquestación y Dependencias Automáticas**
# MAGIC
# MAGIC Simplemente defines las tablas de origen y destino, y DLT deduce la secuencia de ejecución.
# MAGIC
# MAGIC **Ejemplo:** Si la Tabla B se define como `SELECT * FROM Tabla A`, DLT sabe que la Tabla A debe actualizarse antes que la Tabla B. DLT construye y gestiona el DAG (Grafo Acíclico Dirigido) por ti.
# MAGIC
# MAGIC **Inferencia y Gestión del Modo de Ejecución**
# MAGIC
# MAGIC - Al definir una tabla, tú especificas si es una vista (`VIEW`), una tabla de streaming (`STREAMING LIVE TABLE`) o una tabla materializada (`LIVE TABLE`).
# MAGIC
# MAGIC - El motor DLT se encarga de aplicar Structured Streaming si los datos se definen como un stream (ej. desde Auto Loader) o de ejecutar un batch si la fuente es estática.
# MAGIC
# MAGIC **Calidad de Datos (Expectations)**
# MAGIC
# MAGIC - DLT te permite integrar la validación de calidad de datos directamente en la definición de la tabla de forma declarativa, utilizando el concepto de Expectations (Expectativas).
# MAGIC
# MAGIC - **Comando:** En lugar de escribir un código de validación aparte, declaras reglas como: **_"El ID del cliente nunca debe ser nulo"._**

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Streaming Tables**
# MAGIC
# MAGIC En Lakeflow Declarative Pipelines, una streaming table es básicamente una tabla manejada por Unity Catalog que además sirve como destino de datos en streaming.
# MAGIC
# MAGIC - A una streaming table le podés escribir uno o varios streaming flows (ej: Append o AUTO CDC).
# MAGIC
# MAGIC - AUTO CDC solo funciona con streaming tables.
# MAGIC
# MAGIC Los flows se pueden definir de dos maneras:
# MAGIC
# MAGIC - Explícitamente (primero definís el flow y después lo conectás a la tabla).
# MAGIC
# MAGIC - Implícitamente (al definir la tabla, ya indicas el flow que la va a alimentar).

# COMMAND ----------

# DBTITLE 1,Streaming Tables Syntax
# MAGIC %sql
# MAGIC CREATE OR REFRESH STREAMING TABLE 
# MAGIC   catalog.bronze.streaming_table
# MAGIC AS SELECT
# MAGIC   *
# MAGIC FROM read_files(
# MAGIC   "/Volumes/dbacademy/ops/labuser/orders",
# MAGIC   format => "json"
# MAGIC );
# MAGIC
# MAGIC CREATE OR REFRESH STREAMING TABLE
# MAGIC   catalog.bronze.table
# MAGIC AS SELECT
# MAGIC   *
# MAGIC FROM STREAM
# MAGIC   catalog.bronze.streaming_table;

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Materialized Views**
# MAGIC
# MAGIC En Lakeflow Declarative Pipelines, una materialized view es **_también una tabla manejada por Unity Catalog, pero a diferencia de las streaming tables funciona como destino batch_** (no streaming).
# MAGIC
# MAGIC - Una materialized view recibe uno o varios materialized view flows.
# MAGIC
# MAGIC - La diferencia clave con las streaming tables es que **_los flows siempre se definen de forma implícita al crear la materialized view (no se pueden separar como en streaming)_**.
# MAGIC
# MAGIC - Su motor procesa solo los datos nuevos o modificados, evitando reprocesar todo.
# MAGIC
# MAGIC   - **Streaming table:** Destino en tiempo real, soporta flows de streaming (Append, AUTO CDC).
# MAGIC
# MAGIC   - **Materialized view:** Destino batch, actualiza solo con cambios, y los flows van siempre incluidos en la definición.
# MAGIC
# MAGIC Se usa Incremental Refresh para evitar reconstruir la view entera cada vez que llegan nuevos datos. Esto está disponible solo en entornos Serverless.

# COMMAND ----------

# MAGIC %md
# MAGIC ### **View**
# MAGIC
# MAGIC Construye una tabla virtual sin datos físicos basado en el resultado de una consulta en un pipeline.
# MAGIC
# MAGIC - Se registran como un objeto en Unity Catalog.
# MAGIC
# MAGIC ### **Temporary View**
# MAGIC
# MAGIC Las Temporary Views persisten solo durante el pipeline.
# MAGIC
# MAGIC - No se registran como un objeto en Unity Catalog
# MAGIC
# MAGIC - Se usan como queries intermedias

# COMMAND ----------

# DBTITLE 1,Views Syntax
# MAGIC %sql
# MAGIC -- View
# MAGIC CREATE VIEW
# MAGIC   catalog.silver.my_view
# MAGIC AS SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   catalog.bronze.table;
# MAGIC
# MAGIC -- Temporary View
# MAGIC CREATE TEMPORARY VIEW
# MAGIC   catalog.silver.my_view
# MAGIC AS SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   catalog.bronze.table;

# COMMAND ----------

# DBTITLE 1,Materialized View Syntax
# MAGIC %sql
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW 
# MAGIC   catalog.silver.materialized_view
# MAGIC AS SELECT
# MAGIC   *
# MAGIC FROM 
# MAGIC   catalog.bronze.table;

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Conceptos**
# MAGIC
# MAGIC ### **Flow**
# MAGIC
# MAGIC En Lakeflow Declarative Pipelines, un flow es la unidad básica de procesamiento de datos. Puede ser streaming o batch y siempre sigue el patrón: lee de una fuente → aplica lógica → escribe en un destino.
# MAGIC
# MAGIC - **Flows estándar:** Usa los mismos modos de Structured Streaming (Append, Update, Complete). Hoy solo está disponible Append.
# MAGIC
# MAGIC **Flows especiales de Lakeflow:**
# MAGIC
# MAGIC - **AUTO CDC (streaming):** maneja automáticamente eventos CDC fuera de orden y soporta SCD Tipo 1 y 2.
# MAGIC
# MAGIC - **Materialized View (batch):** Procesa solo datos nuevos o modificados en la fuente, evitando reprocesar todo.
# MAGIC
# MAGIC ### **Sink**
# MAGIC
# MAGIC Un sink es un destino de streaming.
# MAGIC
# MAGIC Tipos soportados hoy:
# MAGIC
# MAGIC - Delta tables
# MAGIC
# MAGIC - Apache Kafka topics
# MAGIC
# MAGIC - Azure EventHubs topics
# MAGIC
# MAGIC - Un sink puede recibir uno o varios streaming flows (Append).
# MAGIC
# MAGIC El sink es adonde van a parar los datos en tiempo real, ya sea a una tabla Delta o a un sistema de mensajería como Kafka o EventHubs.
# MAGIC
# MAGIC ### **Pipeline**
# MAGIC
# MAGIC Un pipeline es la unidad completa de desarrollo y ejecución.
# MAGIC
# MAGIC Un pipeline puede incluir:
# MAGIC
# MAGIC - Flows (streaming o batch)
# MAGIC
# MAGIC - Streaming tables
# MAGIC
# MAGIC - Materialized views
# MAGIC
# MAGIC - Sinks
# MAGIC
# MAGIC - **Para usarla:** Escribís todo en el código de la pipeline y luego la ejecutás.
# MAGIC
# MAGIC Mientras corre, Lakeflow:
# MAGIC
# MAGIC - Analiza dependencias entre flows, tablas, vistas y sinks.
# MAGIC
# MAGIC - Orquesta automáticamente el orden de ejecución.
# MAGIC
# MAGIC - Decide qué se puede ejecutar en paralelo para optimizar rendimiento.

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Beneficios de Lakeflow Declarative Pipelines**
# MAGIC
# MAGIC - **Orquestación automática:** No tenés que preocuparte por el orden ni por los fallos. La pipeline organiza sola los pasos, corre en paralelo lo que se pueda y, si algo falla, reintenta primero la tarea de Spark, después el flow completo y, si hace falta, toda la pipeline.
# MAGIC
# MAGIC - **Procesamiento declarativo:** En lugar de escribir cientos o miles de líneas en Spark o Structured Streaming, lo resolvés con unas pocas funciones declarativas. Por ejemplo, la API AUTO CDC ya viene preparada para manejar cambios en los datos (CDC) con soporte de SCD Tipo 1 y 2, y además resuelve sola problemas típicos como eventos fuera de orden o conceptos avanzados de streaming.
# MAGIC
# MAGIC - **Procesamiento incremental:** Cuando actualizás materialized views, solo procesa los datos nuevos o modificados. Así se evita volver a procesar todo desde cero y no necesitás código extra para manejar incrementos.
# MAGIC
# MAGIC ![](https://docs.databricks.com/aws/en/assets/images/dlt-core-concepts-6bc9894d3682035cadac19f3980875ae.png)

# COMMAND ----------

@dlt.expect("valido_id", "id_cliente IS NOT NULL")
@dlt.table
def clientes_limpios():
    # ... lógica de selección

# COMMAND ----------

# MAGIC %md
# MAGIC **Acción:** DLT puede configurar qué hacer cuando falla una expectativa:
# MAGIC
# MAGIC - **Alertar:** Solo registrar el error.
# MAGIC
# MAGIC - **Cuarentena:** Enviar la fila fallida a una tabla separada para revisión.
# MAGIC
# MAGIC - **Detener:** Fallar el pipeline si la violación es crítica.

# COMMAND ----------

# MAGIC %md
# MAGIC **Eficiencia y Mantenimiento (DLM Automático)**
# MAGIC
# MAGIC DLT es consciente de las operaciones de Data Lifecycle Management (DLM) y las aplica automáticamente:
# MAGIC
# MAGIC - **`OPTIMIZE` Automático:** DLT ejecuta automáticamente comandos de optimización (como `OPTIMIZE` y `ZORDER BY`) según la configuración de la tabla para mantener la eficiencia de lectura.
# MAGIC
# MAGIC - **Gestión de Clusters:** DLT gestiona el ciclo de vida de los Job Clusters.

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Ejemplo de Sintaxis Declarativa (Python)**
# MAGIC
# MAGIC Observa cómo defines la relación en lugar de las operaciones:

# COMMAND ----------

# 1. Tabla de Origen (Declaración)
@dlt.table
def datos_brutos_streaming():
    # DLT sabe que esto es un stream porque usamos readStream
    return spark.readStream.format("cloudFiles").load("/dataraw/")

# 2. Tabla de Limpieza (Declaración con Dependencia y Calidad)
@dlt.expect("pais_valido", "Pais IN ('USA', 'CAN', 'MEX')")
@dlt.table(comment="Tabla de clientes validados.")
def clientes_limpios():
    # La dependencia es implícita: selecciona de la tabla de arriba
    return (
        dlt.read_stream("datos_brutos_streaming")
        .select("id_cliente", "pais", "nombre")
        .where("id_cliente IS NOT NULL")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC En este ejemplo, tú declaras la estructura (`@dlt.table`) y las reglas (`@dlt.expect`), y el motor de DLT se encarga del imperativo: orquestar, ejecutar el stream, aplicar la lógica de calidad y optimizar la tabla Delta final.

# COMMAND ----------

# MAGIC %md
# MAGIC ## **UI**
# MAGIC
# MAGIC - **Pipeline Graph:** Es el DAG gráfico que define el orden de ejecución de las tareas.
# MAGIC
# MAGIC - **Dry Run:** Es una ejecución de prueba que valida la lógica de lo que vas a correr, pero sin ejecutar realmente las transformaciones ni escribir datos en el storage.
# MAGIC
# MAGIC - **Permission Settings:** Se asignan permisos que pueden tener los usuarios / grupos sobre un pipeline: 
# MAGIC
# MAGIC   - **Is owner:** Permisos de dueño
# MAGIC
# MAGIC   - **Can manage:** Puede manejar permisos en el pipeline
# MAGIC
# MAGIC   - **Can view:** Puede ver el pipeline
# MAGIC
# MAGIC   - **Can run:** Puede correr el pipeline
# MAGIC
# MAGIC - **Schedule:** Programar la ejecución del pipeline, con UI o CRON JOBS, y elegir si enviará un mail de `START`, `SUCCESS` o `FAIL`.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pipeline Configuration
# MAGIC
# MAGIC A pipeline's configuration is **a map of key value pairs** that can be used to parametrize your code:
# MAGIC
# MAGIC - Improve code readability / maintainability
# MAGIC
# MAGIC - Reuse common parameters in multiple pipelines files
# MAGIC
# MAGIC **Key:** source
# MAGIC
# MAGIC **Value:** "/Volumes/some/path"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE STREAMING TABLE
# MAGIC   bronze
# MAGIC AS SELECT
# MAGIC   *
# MAGIC FROM STREAM read_files(
# MAGIC   "${source}/orders",
# MAGIC   format=>'JSON'
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Streaming Join**
# MAGIC
# MAGIC Un Streaming Join es la capacidad de unir (hacer un `JOIN`) dos fuentes de datos donde, al menos, una de ellas fluye en tiempo real.
# MAGIC
# MAGIC En el mundo tradicional (Batch), un Join es fácil porque los datos son estáticos: Spark mira la Tabla A, mira la Tabla B y las junta. Pero en Streaming, los datos están llegando constantemente, lo que hace que el "matching" sea mucho más complejo.
# MAGIC
# MAGIC ![](https://miro.medium.com/v2/resize:fit:1400/1*v40tibW63tuEP8wB3ZxvkQ.png)
# MAGIC
# MAGIC #### **Stream-to-Static Join (El más común)**
# MAGIC
# MAGIC Es cuando unes un flujo en vivo (Streaming) con una tabla fija (Batch).
# MAGIC
# MAGIC - **Ejemplo:** Tienes un flujo de transacciones bancarias en tiempo real y quieres unirlo con una tabla de clientes (estática) para saber el nombre de quién hizo la compra.
# MAGIC
# MAGIC Este es un **Incremental JOIN**, lo que significa que la tabla estática (static table) no necesita ser reprocesada, solo los datos entrantes son evaluados.
# MAGIC
# MAGIC #### **Stream-to-Stream Join (El más complejo)**
# MAGIC
# MAGIC Es cuando unes dos flujos que están llegando en tiempo real al mismo tiempo.
# MAGIC
# MAGIC - **Ejemplo:** Un flujo de clics en una web y un flujo de compras. Quieres saber qué clic terminó en una compra.
# MAGIC
# MAGIC - **El gran reto:** ¿Qué pasa si el clic llega a las 10:00 AM pero la compra ocurre a las 10:05 AM? Spark tiene que "guardar" el clic en memoria (State) hasta que llegue la compra para poder unirlos.
# MAGIC
# MAGIC ### **3. El concepto de "State" y "Watermarking"**
# MAGIC
# MAGIC Como no puedes guardar datos en memoria para siempre (te quedarías sin RAM), en los Joins de streaming es obligatorio usar:
# MAGIC
# MAGIC - **Watermarking:** Para decirle a Spark: "Espera a la otra parte del Join máximo 10 minutos; si no llega, descarta el dato".
# MAGIC
# MAGIC - **Restricciones de tiempo:** Debes definir un intervalo en la condición del Join (ej. `compra.time BETWEEN clic.time AND clic.time + INTERVAL 1 HOUR`).

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Deploying a Pipeline to Production**
# MAGIC
# MAGIC ![](https://cdn5.dcbstatic.com/files/d/a/databricks_docebosaas_com/1768366800/JNn_rtv569Pn1Zntq9l1Vg/authoring/687/687_full_slide29_1.jpg)

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Pipeline Scheduling**
# MAGIC
# MAGIC #### **Triggered**
# MAGIC
# MAGIC - Can be triggered manually or run on a schedule
# MAGIC - Refreshes selected tables using data available at the start, then stops once the update is complete
# MAGIC
# MAGIC #### **Continuous**
# MAGIC
# MAGIC - Continuously processes new data to keep streaming tables and materialized tables up to date in near real time
# MAGIC - Monitors dependencies and **updates only when source data changes**

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Pipeline Event Log**
# MAGIC
# MAGIC ![](https://cdn5.dcbstatic.com/files/d/a/databricks_docebosaas_com/1768406400/e4N8EnHAYQkTqmOjchaa-w/authoring/687/687_full_slide36_1.jpg)

# COMMAND ----------

# MAGIC %md
# MAGIC The Event Log is written as a hidden Delta table
# MAGIC
# MAGIC - Located in the pipeline default catalog and schema
# MAGIC
# MAGIC **Querying the Declarative Pipeline Event Log**
# MAGIC
# MAGIC - Specify the table location 
# MAGIC - Define the table name

# COMMAND ----------

# DBTITLE 1,Event Log Syntax
# MAGIC %sql
# MAGIC SELECT * FROM <catalog>.<schema>.<event_log_table_name>