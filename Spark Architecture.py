# Databricks notebook source
# MAGIC %md
# MAGIC Apache Spark is a **unified computing engine** and a set of libraries for parallel data processing on **computer clusters**.

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Ecosistema de Spark**
# MAGIC
# MAGIC ![](https://sparkbyexamples.com/wp-content/uploads/2020/02/spark-components-1-768x432.jpg)
# MAGIC
# MAGIC ### **Componentes de Spark**
# MAGIC
# MAGIC ![](https://spark.apache.org/docs/latest/img/cluster-overview.png)
# MAGIC
# MAGIC ### **Driver Program** 
# MAGIC
# MAGIC Es el componente central de una aplicación de Spark. Es el proceso que corre la función `main()` de una aplicación de Spark, es el responsable de:
# MAGIC
# MAGIC - Coordina la ejecución del programa Spark
# MAGIC
# MAGIC - Convierte el código en un DAG (Directed Acyclic Graph) de tareas
# MAGIC
# MAGIC - Divide el DAG en stages y tasks
# MAGIC
# MAGIC - Programa las tareas en los executors
# MAGIC
# MAGIC - Mantiene información sobre los RDDs y su lineage
# MAGIC
# MAGIC - Recolecta resultados y métricas de ejecución
# MAGIC
# MAGIC ### **Cluster Manager** 
# MAGIC
# MAGIC Coordina y distribuye recursos (CPU, Memoria) en un cluster de máquinas de Spark.
# MAGIC
# MAGIC - Recibe el pedido del Driver
# MAGIC
# MAGIC - Decide qué nodos ejecutarán las tareas
# MAGIC
# MAGIC - Asigna máquinas (Workers)
# MAGIC
# MAGIC - Decide cuántos Executors va a tener un Job
# MAGIC
# MAGIC ### **Worker Nodes**
# MAGIC
# MAGIC Alberga y ejecuta los procesos de los "executors" de Spark, que son los responsables directos de procesar los datos.
# MAGIC
# MAGIC - Ejecutan los procesos de trabajo
# MAGIC
# MAGIC - Alojan los executors para procesamiento de datos
# MAGIC
# MAGIC - Reportan estado al Cluster Manager
# MAGIC
# MAGIC - Manejan almacenamiento en disco/memoria
# MAGIC
# MAGIC El Worker no ejecuta tareas directamente, solo aloja Executors que son los que ejecutan las tareas.
# MAGIC
# MAGIC ### **Executors**
# MAGIC
# MAGIC - Ejecutan las tareas asignadas por el Driver
# MAGIC
# MAGIC - Almacenan datos en memoria o disco (cache)
# MAGIC
# MAGIC - Procesan transformaciones y acciones sobre RDDs/DataFrames
# MAGIC
# MAGIC - Reportan progreso al Driver
# MAGIC
# MAGIC > **Nota:** Un executor de Spark dispone de múltiples slots para procesar varias tareas en paralelo. Spark admite una tarea por cada core de CPU virtual (vCPU) de forma predeterminada. Por ejemplo, si un executor tiene cuatro cores de CPU, puede ejecutar cuatro tareas simultáneas.
# MAGIC
# MAGIC ### **Tasks**
# MAGIC
# MAGIC Es la unidad mínima de trabajo que se ejecuta en paralelo para procesar una partición de datos en un executor.
# MAGIC
# MAGIC - **Ejecución en paralelo:** Las tareas se ejecutan en paralelo en diferentes executors (worker nodes) del clúster para procesar sus respectivas particiones de datos. El número de tareas que se ejecutan simultáneamente depende del número de núcleos de la CPU disponibles.

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Execution Plan**
# MAGIC
# MAGIC ### **Spark Application y Jobs**
# MAGIC
# MAGIC ![](https://docs.aws.amazon.com/images/prescriptive-guidance/latest/tuning-aws-glue-for-apache-spark/images/spark-execution-plan.png)
# MAGIC
# MAGIC - **Acción - Job:** En Spark, el trabajo real se desencadena solo cuando se llama a una Acción (`show()`, `write()`, o `count()`).
# MAGIC
# MAGIC - Cada vez que se ejecuta una Acción, el Driver Program de Spark crea un nuevo Job (Trabajo).
# MAGIC
# MAGIC En el diagrama, vemos tres acciones que resultan en:
# MAGIC
# MAGIC - `show()` genera **Job1**.
# MAGIC
# MAGIC - `write()` genera **Job2**.
# MAGIC
# MAGIC - `count()` genera **Job3**.
# MAGIC
# MAGIC ### **2. Stages y el DAG**
# MAGIC
# MAGIC El corazón del procesamiento distribuido está en cómo cada Job se descompone en Stages (Etapas).
# MAGIC
# MAGIC **A. Descomposición en Stages**
# MAGIC
# MAGIC El DAGScheduler toma la secuencia de transformaciones necesarias para completar un Job (ej. `join()`) y las divide en Stages.
# MAGIC
# MAGIC - **Límites de Stage:** La división entre Stages ocurre cuando hay una transformación que requiere mover datos entre los nodos del cluster. Esto se conoce como un Shuffle.
# MAGIC
# MAGIC - **Stage-A → Stage-B:** En el diagrama, el `join()` (una transformación "ancha" o wide transformation) es el responsable de dividir el Job en **Stage1** y **Stage2**.
# MAGIC
# MAGIC **B. El Papel del Shuffle**
# MAGIC
# MAGIC - **Shuffle (Intercambio de Datos):** Es la flecha que conecta **Stage1** con **Stage2**.
# MAGIC
# MAGIC - Una operación como `join()` o `groupBy()` requiere que todos los datos con la misma clave (por ejemplo, el ID de usuario en un `join()`) se muevan a la misma partición, incluso si estaban originalmente en diferentes nodos.
# MAGIC
# MAGIC - **Stage1** es la etapa que produce los datos que deben ser reagrupados, y **Stage2** es la etapa que consume esos datos reagrupados después del shuffle. El shuffle es la operación más costosa en Spark, ya que utiliza la red y el disco.
# MAGIC
# MAGIC ### **3. Tasks (Tareas) y Paralelismo**
# MAGIC
# MAGIC Finalmente, las Stages se dividen en las unidades de trabajo más pequeñas que Spark ejecuta: las Tasks.
# MAGIC
# MAGIC - **Partición → Task:** Cada Stage se divide en una o más Tasks. El número de Tasks en una Stage es igual al número de particiones de los datos de entrada/salida de esa Stage.
# MAGIC
# MAGIC - En el diagrama, tanto **Stage1** como **Stage2** tienen **Task1**, **Task2** y **Task3**. Esto implica que los datos se dividieron en tres particiones.
# MAGIC
# MAGIC - **Ejecución Paralela:** Estas Tasks se envían a los Executors (Ejecutores) distribuidos en los nodos de trabajo del cluster.
# MAGIC
# MAGIC - Las tres Tasks dentro de una Stage se ejecutan en paralelo en diferentes núcleos de CPU (o cores) para lograr la velocidad del procesamiento distribuido.
# MAGIC
# MAGIC Cada vez que se hace una transformación, Spark no ejecuta nada. En su lugar, simplemente agrega esa operación como un nuevo nodo en el DAG que se está construyendo en el Driver.
# MAGIC
# MAGIC **Ejemplo:**
# MAGIC
# MAGIC - `df.filter(...)` → Agrega un nodo Filter al DAG.
# MAGIC
# MAGIC - `df.groupBy(...)` → Agrega un nodo Aggregate al DAG.
# MAGIC
# MAGIC - `df.select(...)` → Agrega un nodo Project al DAG.

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Narrow vs Wide Transformations**
# MAGIC
# MAGIC ![](https://substackcdn.com/image/fetch/$s_!YIom!,w_1456,c_limit,f_webp,q_auto:good,fl_progressive:steep/https%3A%2F%2Fsubstack-post-media.s3.amazonaws.com%2Fpublic%2Fimages%2Fca5ee550-adcb-4f8d-b1d5-83f4ea19d2a2_1024x1024.png)
# MAGIC
# MAGIC ### **Narrow Transformations**
# MAGIC
# MAGIC Una transformación es Narrow (Estrecha) si todas las particiones de datos necesarias para calcular el resultado de una partición de salida **residen dentro de la misma partición de entrada** o, a lo sumo, en un número limitado de particiones fijas.
# MAGIC
# MAGIC El resultado es que no se requiere Shuffle (intercambio de datos a través de la red) entre los nodos.
# MAGIC
# MAGIC **Características Clave**
# MAGIC
# MAGIC - **Sin Shuffle:** El cálculo puede ocurrir localmente dentro de cada nodo del cluster.
# MAGIC
# MAGIC - **Encadenamiento (Pipelining):** Spark puede encadenar varias Narrow Transformations juntas dentro de la misma Stage (Etapa) sin escribir resultados intermedios a disco. Esto es muy rápido.
# MAGIC
# MAGIC - **Tolerancia a Fallos:** Si un nodo falla, solo se necesita volver a calcular la partición afectada, ya que los datos de entrada para esa partición están localizados.
# MAGIC
# MAGIC - `filter()`: La condición se evalúa en cada fila de la partición local.
# MAGIC
# MAGIC - `map()`: Se aplica una función a cada elemento de la partición local.
# MAGIC
# MAGIC - `union()`: Simplemente combina particiones, pero mantiene los datos de entrada originales separados.
# MAGIC
# MAGIC - `select()`: Reordenar o crear nuevas columnas a partir de columnas existentes en la misma fila.
# MAGIC
# MAGIC - `withColumn()`: Similar a `select`, opera fila por fila.
# MAGIC
# MAGIC ### **Wide Transformations**
# MAGIC
# MAGIC Una transformación es Wide (Ancha) si las particiones de datos necesarias para calcular el resultado de una partición de salida pueden provenir de múltiples (o todas) las particiones de entrada.
# MAGIC
# MAGIC Esto requiere que Spark mueva datos entre los Executors a través de la red, un proceso conocido como **Shuffle**.
# MAGIC
# MAGIC **Características Clave**
# MAGIC
# MAGIC - **Requiere Shuffle:** Los datos deben serializarse, transmitirse a través de la red y deserializarse en los nodos de destino. Esto es costoso en tiempo de CPU, disco y ancho de banda de red.
# MAGIC
# MAGIC - **Fuerza una Nueva Stage:** Las Wide Transformations actúan como un límite, forzando al DAGScheduler a terminar la Stage actual e iniciar una nueva Stage después del Shuffle.
# MAGIC
# MAGIC - **Checkpoints/Materialización:** Los resultados intermedios del Shuffle suelen escribirse a disco, lo que también ayuda en la recuperación de fallos, pero añade latencia.
# MAGIC
# MAGIC - `groupByKey()` / `groupBy()`: Todos los registros con la misma clave deben ir a la misma partición para la agregación, forzando un Shuffle.
# MAGIC
# MAGIC - `join()`: Si las tablas no están co-particionadas, Spark debe enviar filas con la misma clave a los mismos nodos para que la unión sea posible.
# MAGIC
# MAGIC - `repartition()`: Mover los datos a un número diferente de particiones o a un esquema de particionamiento diferente.
# MAGIC
# MAGIC - `orderBy()` / `sort()`: Para garantizar un orden global, todos los datos deben ser comparados, lo que a menudo implica un Shuffle.
# MAGIC
# MAGIC ### **Implicaciones de Rendimiento**
# MAGIC
# MAGIC La diferencia entre Narrow y Wide Transformations es la razón fundamental detrás de la optimización en Spark:
# MAGIC
# MAGIC - **Optimización del Código:** Un buen desarrollador Spark siempre busca minimizar el número de Wide Transformations (Shuffles) en su aplicación.
# MAGIC
# MAGIC - **Gestión de Memoria:** El Shuffle requiere memoria de buffer adicional y puede causar problemas de OOM (Out-Of-Memory) si los datos de una clave específica son demasiado grandes para caber en la memoria de un solo Executor.
# MAGIC
# MAGIC - **Configuración:** Los parámetros de configuración de Spark, como `spark.sql.shuffle.partitions`, están diseñados específicamente para optimizar el comportamiento de las Wide Transformations.

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Lazy Execution**
# MAGIC
# MAGIC Un buen ejemplo para explicar la Lazy Execution en Spark, es de la siguiente manera: 
# MAGIC
# MAGIC Supongamos que hacemos las siguientes transformaciones a un dataframe:

# COMMAND ----------

df = spark.read.format("csv").load(path)

df.groupBy("age")
df.join(df2, join_condition, join_type) # Wide Transformation
df.filter("age > 10")
df.show() # Action

# COMMAND ----------

# MAGIC %md
# MAGIC Read → Group By → Wide T. → Filter → Show
# MAGIC
# MAGIC Como el `Filter` (una operación Narrow) no se ejecuta hasta el `Show` (la Acción), Spark tiene la oportunidad de examinar todo el plan antes de empezar.
# MAGIC
# MAGIC Read → Filter → Group By → Wide T. → Show
# MAGIC
# MAGIC De esta manera, la operación de `Filter` se realiza inmediatamente después de leer los datos (o incluso mientras se leen, si la fuente lo soporta), minimizando la carga de trabajo para las operaciones distribuidas posteriores (`GroupBy`, `Wide T.`).
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **El Catalyst Optimizer y la Optimización**
# MAGIC
# MAGIC El componente que toma la decisión de reordenar las operaciones es el **Catalyst Optimizer.**
# MAGIC
# MAGIC El Catalyst opera en el plan lógico de la aplicación (el DAG) y aplica reglas de optimización para transformar el plan ineficiente en uno más eficiente, pero que produce el mismo resultado.
# MAGIC
# MAGIC **La Regla Aplicada: Predicate Pushdown**
# MAGIC
# MAGIC La optimización que estás viendo se llama Predicate Pushdown (o Filter Pushdown).
# MAGIC
# MAGIC - **Regla:** Es casi siempre más eficiente mover las operaciones de filtrado (`Filter`) y proyección (`Select`) lo más cerca posible de la fuente de datos (`Read`).
# MAGIC
# MAGIC **Motivo:**
# MAGIC
# MAGIC - **Menos Datos para el Shuffle:** Las operaciones como `GroupBy` o Wide T. a menudo requieren un costoso Shuffle (movimiento de datos en la red). Si aplicas el `Filter` antes del `GroupBy`, reduces drásticamente la cantidad de datos que necesitan ser movidos y procesados en la operación costosa.
# MAGIC
# MAGIC - **Menos Memoria/CPU:** Disminuir el volumen de datos en etapas tempranas significa que las etapas posteriores (como el `GroupBy` o Wide T.) **manejarán menos filas**, ahorrando memoria y tiempo de CPU.
# MAGIC
# MAGIC ![](https://docs.aws.amazon.com/images/prescriptive-guidance/latest/tuning-aws-glue-for-apache-spark/images/catalyst-optimizer.png)
# MAGIC
# MAGIC La función `explain()` muestra el plan de ejecución optimizado que usará Spark.
# MAGIC
# MAGIC **1. El Plan Lógico (Unresolved y Resolved)**
# MAGIC
# MAGIC `explain()` primero muestra cómo Spark interpreta tu código.
# MAGIC
# MAGIC - **Unresolved Logical Plan:** Cómo Spark lee tu código antes de verificar si las columnas o tablas existen.
# MAGIC
# MAGIC - **Resolved Logical Plan:** El plan lógico después de que Spark ha verificado y validado los nombres de columnas y tablas en el catálogo.
# MAGIC
# MAGIC **2. El Plan Lógico Optimizado**
# MAGIC
# MAGIC Esta es la clave de la "inteligencia" de Spark. Aquí es donde el Catalyst Optimizer aplica reglas de optimización (como el Predicate Pushdown que mencionamos antes) para reescribir el plan lógico ineficiente en uno más eficiente.
# MAGIC
# MAGIC Muestra el plan de transformaciones lógicas reordenadas y simplificadas.
# MAGIC
# MAGIC **3. El Plan Físico (Final)**
# MAGIC
# MAGIC Este es el plan que finalmente se traduce en **Stages** y **Tasks** y que se envía para su ejecución distribuida.
# MAGIC
# MAGIC - **Plan Físico:** Muestra los operadores físicos que se usarán (ej. `BroadcastHashJoin`, `SortMergeJoin`, `HashAggregate`).
# MAGIC
# MAGIC - **Paralelismo y Particiones:** Indica dónde ocurrirán los costosos Shuffles (por ejemplo, con un operador Exchange) y cuántas particiones se crearán.