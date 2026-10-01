# Databricks notebook source
# MAGIC %md
# MAGIC ### Small Files
# MAGIC
# MAGIC ![](https://delta.io/_astro/image1.Cjq8K5tu_ev2TH.webp)
# MAGIC
# MAGIC Este problema ocurre cuando los datos se dividen en miles de archivos de pocos KB. Spark pierde más tiempo abriendo/cerrando metadatos que leyendo datos.
# MAGIC
# MAGIC **Síntomas**
# MAGIC
# MAGIC - Consultar una tabla lleva minutos solo en la fase de "listing files" (metadata overhead), aunque la cantidad total de datos sea pequeña.
# MAGIC
# MAGIC - Saturación del NameNode o metastore.
# MAGIC
# MAGIC **Soluciones:**
# MAGIC
# MAGIC - `OPTIMIZE`: Agrupa los archivos pequeños en archivos más grandes (idealmente de 1GB).
# MAGIC
# MAGIC - **Auto Optimize:** Habilitar `autoOptimize.optimizeWrite` y `autoOptimize.autoCompact` para que Databricks gestione el tamaño durante la escritura.
# MAGIC
# MAGIC - **Frecuencia de VACUUM**: No resuelve el rendimiento de lectura, pero elimina archivos antiguos que ya no son necesarios tras un OPTIMIZE.
# MAGIC
# MAGIC - **Usar `coalesce()` antes de escribir:** A diferencia de `repartition()`, `coalesce()` reduce el número de particiones sin provocar un Shuffle completo.

# COMMAND ----------

# Reduce a 4 particiones antes de guardar
df.coalesce(4).write.format("delta").save(...)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Skew
# MAGIC
# MAGIC Ocurre cuando la carga de datos no se distribuye equitativamente entre las particiones. En un `JOIN` o `GROUP BY`, una clave específica (por ejemplo, `user_id = NULL` o un valor genérico) concentra el 80% de los registros en un solo Task, mientras los demás ejecutores terminan en 5 segundos y se quedan esperando al ejecutor "pesado".
# MAGIC
# MAGIC - **Síntoma:** Durante un Join o Agregación, un solo "Stage" de Spark tarda mucho más que el resto, o un ejecutor se queda sin memoria (OOM) mientras los demás están inactivos.
# MAGIC
# MAGIC **Soluciones:**
# MAGIC
# MAGIC - **Salting (Técnica clásica):** Si haces un JOIN por una clave muy repetida, le agregas un sufijo aleatorio (un "salero", ej. 0 al 3) a la clave en la tabla izquierda y duplicas los registros en la tabla derecha con esos sufijos para repartir el peso entre múltiples particiones.
# MAGIC
# MAGIC - **AQE Skew Join (Automatizado en Spark 3+):** Habilita Adaptive Query Execution. Spark detecta las particiones sesgadas en tiempo de ejecución y las divide automáticamente en subparticiones más pequeñas.

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.sql.adaptive.enabled = true;
# MAGIC SET spark.sql.adaptive.skewJoin.enabled = true;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Shuffling
# MAGIC
# MAGIC Es el proceso de mover datos entre nodos del clúster a través de la red. Es la operación más cara en términos de tiempo.
# MAGIC
# MAGIC - **Síntoma:** Alto uso de red y lentitud en operaciones de `JOIN`, `GROUP BY` o `DISTINCT`.
# MAGIC
# MAGIC **Soluciones:**
# MAGIC
# MAGIC - **Broadcast Hash Join:** Si una de las tablas es pequeña (menor a 10MB por defecto), Spark la copia en todos los nodos, evitando el shuffle de la tabla grande.
# MAGIC
# MAGIC - **Z-Order Clustering:** Organiza los datos físicamente para que las columnas relacionadas estén en los mismos archivos, mejorando el "Data Skipping".
# MAGIC
# MAGIC - **Evitar particionado excesivo:** No particionar por columnas con alta cardinalidad (muchos valores únicos), ya que genera demasiadas carpetas y archivos pequeños.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Spill To Disk
# MAGIC
# MAGIC Ocurre cuando una partición o dataset intermedio es demasiado grande para caber en la memoria RAM asignada al ejecutor (`spark.memory.fraction`). Para no estrellarse con un OOM, Spark "derrama" (spills) los datos temporales al disco local del nodo (RAM → Disk) y luego los vuelve a leer.
# MAGIC
# MAGIC - **Síntoma:** En la UI de Spark verás una advertencia de "Spill (Memory)" y "Spill (Disk)". El rendimiento cae drásticamente (el disco es mucho más lento que la RAM).
# MAGIC
# MAGIC **Soluciones:**
# MAGIC
# MAGIC - **Filtrado temprano:** Aplicar `WHERE` y `SELECT` (solo las columnas necesarias) lo antes posible para reducir el volumen de datos en memoria.
# MAGIC
# MAGIC - **Aumentar tamaño de instancia:** Usar Workers con más memoria RAM.
# MAGIC
# MAGIC - **Reducir el tamaño de las particiones:** Aumentar `spark.sql.shuffle.partitions` para que cada tarea individual maneje menos datos.
# MAGIC
# MAGIC - **AQE Auto-Optimizing:** Permite que Spark redimensione las particiones dinámicamente:

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.sql.adaptive.coalescePartitions.enabled = true;

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Out of Memory (OOM) Errors**
# MAGIC
# MAGIC **¿Qué es?**
# MAGIC
# MAGIC El proceso de Spark colapsa porque se sobrepasa la memoria física disponible en el Driver o en los Executors.
# MAGIC
# MAGIC **Tipos y Soluciones**
# MAGIC
# MAGIC **A. Driver OOM (`java.lang.OutOfMemoryError`: Java heap space)**
# MAGIC - **Causa:** Traer demasiados datos al nodo central usando `.collect()`, `.toPandas()`, o plots locales sobre datasets masivos.
# MAGIC
# MAGIC - **Solución:** Evita `.collect()` en producción. Si necesitas exportar a pandas, filtra/agrega los datos en Spark primero. Si es indispensable, aumenta `spark.driver.memory`.
# MAGIC
# MAGIC **B. Executor OOM**
# MAGIC - **Causa:** Particiones gigantescas (relacionado con Skew o Spill), o uso de funciones UDFs en Python no optimizadas que saturan la memoria del proceso Python/JVM.
# MAGIC
# MAGIC - **Solución:** Usa Vectorized UDFs (PySpark Pandas UDFs) basadas en Apache Arrow en lugar de UDFs tradicionales de Python.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Metadata Bottlenecks
# MAGIC
# MAGIC Sucede cuando Spark tarda demasiado en listar los archivos en el almacenamiento de nube antes de empezar a procesar.
# MAGIC
# MAGIC - **Síntoma:** La consulta tarda varios segundos o minutos en "arrancar" antes de que veas progreso en los Jobs.
# MAGIC
# MAGIC **Soluciones:**
# MAGIC
# MAGIC - **Delta Lake:** Al usar tablas Delta, Spark lee el _delta_log en lugar de listar archivos en S3/ADLS, lo que elimina este problema.
# MAGIC
# MAGIC - **Partition Pruning:** Asegurarse de filtrar por la columna de partición en la cláusula `WHERE` para que Spark ignore carpetas enteras.