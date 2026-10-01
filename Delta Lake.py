# Databricks notebook source
# MAGIC %md
# MAGIC # **Delta Lake**

# COMMAND ----------

# MAGIC %md
# MAGIC Delta Lake es una capa de almacenamiento optimizada que agrega la fiabilidad, seguridad y rendimiento de los data warehouses a los data lakes existentes. Aplica transacciones ACID (Atomicidad, Consistencia, Aislamiento y Durabilidad), gestión de metadatos escalables y un sistema de versionado que habilita características como Time Travel (viaje en el tiempo) y Schema Enforcement (aplicación de esquemas).

# COMMAND ----------

# MAGIC %md
# MAGIC ### **File Storage**
# MAGIC
# MAGIC Esta es la capa de almacenamiento físico donde residen los datos reales y la lógica de Delta Lake.
# MAGIC
# MAGIC **1.1. Los Archivos de Datos (e.g., `data0.parquet`)**
# MAGIC
# MAGIC - **Contenido:** Son los archivos que contienen los datos de tu tabla. En el ecosistema Delta Lake, el formato subyacente siempre es Parquet porque es un formato columnar optimizado para el análisis de alto rendimiento.
# MAGIC
# MAGIC - **Ubicación:** Residen dentro de la carpeta de la tabla (`your_table/`) en tu Data Lake (por ejemplo, AWS S3, Azure Data Lake Storage o Google Cloud Storage).
# MAGIC
# MAGIC **1.2. El Registro de Transacciones (`_delta_log/`)**
# MAGIC
# MAGIC - **Contenido:** Esta subcarpeta es el corazón de Delta Lake. Contiene una serie ordenada de archivos JSON (como `log0.json`, `log1.json`, etc.) que registran cada cambio (transacción) que se realiza en la tabla.
# MAGIC
# MAGIC - **Función:** Los archivos JSON no almacenan los datos en sí, **sino las acciones atómicas** (qué archivos Parquet se añadieron y cuáles se eliminaron) que definen el estado de la tabla en una versión específica.
# MAGIC
# MAGIC - **Fiabilidad:** Esto es lo que proporciona las características ACID y el Time Travel. Para saber el estado actual de la tabla, Spark solo necesita leer el último archivo de checkpoint y el registro de transacciones subsiguiente.
# MAGIC
# MAGIC ![](https://www.databricks.com/sites/default/files/2025-04/diving-into-delta-lake-unpacking-the-transaction-log-2x.png)
# MAGIC
# MAGIC ![](https://www.databricks.com/wp-content/uploads/2019/08/image3-6.png)
# MAGIC
# MAGIC ### **2. Meta Store (Almacén de Metadatos)**
# MAGIC
# MAGIC El Meta Store es el "directorio telefónico" del sistema. No almacena los datos, sino la información estructural y de gobernanza sobre los datos.
# MAGIC
# MAGIC **2.1. Definición y Función**
# MAGIC
# MAGIC - **Definición:** Es el contenedor de más alto nivel de objetos (tablas, vistas, bases de datos).
# MAGIC
# MAGIC - **Contenido:** Almacena metadatos clave como:
# MAGIC
# MAGIC - El nombre de la tabla (e.g., `your_table`).
# MAGIC
# MAGIC - La ubicación física de la tabla en el File Storage (e.g., `s3://my-bucket/your_table/`).
# MAGIC
# MAGIC - El esquema actual de la tabla (nombres y tipos de datos de las columnas).
# MAGIC
# MAGIC - Credenciales de acceso y políticas de seguridad (quién puede leer/escribir).
# MAGIC
# MAGIC Cuando ejecutas una consulta como `SELECT * FROM your_table`, el motor de Spark primero consulta el Meta Store para obtener la ubicación de la tabla en el File Storage. Luego, va a esa ubicación y usa el Registro de Transacciones para encontrar los archivos Parquet correctos para leer.
# MAGIC
# MAGIC **2.2. Tipos de Meta Stores**
# MAGIC
# MAGIC - **HIVE Metastore:** Es el estándar tradicional en el ecosistema Hadoop y Spark. Sigue siendo muy común.
# MAGIC
# MAGIC - **Unity Catalog (Desarrollo de Databricks):** Es la solución moderna de Databricks, diseñada para la arquitectura Lakehouse. Ofrece una gobernanza de datos unificada a través de la nube, gestionando el acceso de forma centralizada a nivel de datos, esquemas, tablas y archivos.
# MAGIC
# MAGIC - **Otros:** Incluye servicios nativos de las nubes como AWS Glue Data Catalog o Azure Purview/Microsoft Fabric Data Catalog, que cumplen la misma función de catalogar y gobernar los objetos de datos.

# COMMAND ----------

# MAGIC %md
# MAGIC Ver detalles de la tabla

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE table_1;

# COMMAND ----------

# MAGIC %md
# MAGIC Ver historial de operaciones en la tabla

# COMMAND ----------

# DBTITLE 1,DESCRIBE HISTORY
# MAGIC %sql
# MAGIC DESCRIBE HISTORY table_1;

# COMMAND ----------

# MAGIC %md
# MAGIC Los Delta logs se guardan en el directorio `_delta_log` que está junto a los datos de la tabla Delta en el almacenamiento (por ejemplo, en S3). Cada vez que se escribe en una tabla Delta, se crea una nueva versión en este directorio como archivos JSON y Parquet.

# COMMAND ----------

# MAGIC %md
# MAGIC Crear tabla desde archivo csv

# COMMAND ----------

# DBTITLE 1,Crear tabla desde archivo CSV
# MAGIC %sql
# MAGIC CREATE TABLE csv_table
# MAGIC AS
# MAGIC SELECT 
# MAGIC   *
# MAGIC FROM read_files(
# MAGIC   'my_csv_file.csv',
# MAGIC   format => 'csv',
# MAGIC   header => true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE 
# MAGIC   csv_table
# MAGIC USING CSV
# MAGIC OPTIONS (
# MAGIC   path = 'my_csv_file.csv',
# MAGIC   header = 'true'
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC Usando el comando `COPY`
# MAGIC
# MAGIC Es más rápido porque está optimizado para la carga masiva de datos. Utiliza procesamiento paralelo, infiere el esquema automáticamente y maneja errores de manera eficiente, lo que permite importar grandes volúmenes de datos desde archivos CSV a tablas Delta de forma mucho más eficiente que los métodos tradicionales como `CREATE TABLE AS SELECT` o `USING CSV`.

# COMMAND ----------

# MAGIC %sql
# MAGIC COPY INTO csv_table
# MAGIC FROM 'my_csv_file.csv'
# MAGIC FILEFORMAT = CSV
# MAGIC FORMAT_OPTIONS ('header' = 'true');

# COMMAND ----------

# MAGIC %md
# MAGIC ### **File Skipping**
# MAGIC
# MAGIC El File Skipping (salto de archivos) es una técnica de optimización de Delta Lake diseñada para reducir drásticamente la cantidad de datos que Apache Spark tiene que leer para satisfacer una consulta.
# MAGIC
# MAGIC ![](https://www.linuxfoundation.org/hs-fs/hubfs/Imported_Blog_Media/data-skipping-concept.png?width=800&height=291&name=data-skipping-concept.png)
# MAGIC
# MAGIC En lugar de escanear todos los archivos Parquet que componen una tabla, Delta Lake consulta la información de los metadatos almacenada en su Registro de Transacciones para identificar **solo los archivos que potencialmente contienen los datos relevantes para la consulta.**
# MAGIC
# MAGIC Cuando se escribe un archivo Parquet en una tabla Delta, se registran metadatos básicos sobre los datos que contiene. Esta información se almacena en el Registro de Transacciones (`_delta_log/`) junto con los detalles de qué archivos se agregaron.
# MAGIC
# MAGIC **1. Recolección de Metadatos (Statistics Collection)**
# MAGIC
# MAGIC Delta Lake recoge las siguientes estadísticas a nivel de archivo y las registra con cada nueva versión:

# COMMAND ----------

# MAGIC %md
# MAGIC | Estadística | Descripción | Uso en File Skipping |
# MAGIC |-------------|-------------|----------------------|
# MAGIC | `min` | Valor mínimo de la columna en el archivo. | Permite saltar un archivo si el valor mínimo es mayor que el valor máximo buscado en la consulta. |
# MAGIC | `max` | Valor máximo de la columna en el archivo. | Permite saltar un archivo si el valor máximo es menor que el valor mínimo buscado en la consulta. |
# MAGIC | `nullCount` | Número de valores nulos en la columna. | Útil en consultas que filtran por `IS NULL` o `IS NOT NULL`. |

# COMMAND ----------

# MAGIC %md
# MAGIC Cuando ejecutas una consulta con una cláusula `WHERE`, por ejemplo:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM my_table WHERE salary > 50000 AND state = 'CA';

# COMMAND ----------

# MAGIC %md
# MAGIC Spark, a través del conector Delta Lake, sigue estos pasos antes de leer el primer byte de datos:
# MAGIC
# MAGIC - **Lee el Registro de Transacciones:** Determina la versión más reciente de la tabla y la lista completa de archivos Parquet.
# MAGIC
# MAGIC - **Aplica los Predicados de Metadatos:** Analiza la cláusula `WHERE` y la compara con los valores `min` y `max` registrados para cada archivo en las columnas de filtro (`salary` y `state`).
# MAGIC
# MAGIC - **Filtrado por Rango:** Si un archivo Parquet tiene un valor máximo de `salary` de 40,000, ese archivo se salta (se excluye) inmediatamente _**porque la consulta busca valores mayores a 50,000.**_
# MAGIC
# MAGIC Si un archivo NO contiene ninguna fila donde `state` sea `'CA'`, también se salta.
# MAGIC
# MAGIC **Particionamiento (Partitioning) como Ayuda**
# MAGIC
# MAGIC Aunque el File Skipping funciona para columnas no particionadas, si la columna utilizada en el filtro es también la columna de partición (por ejemplo, `partitioned by (state)`), la optimización es aún más rápida.
# MAGIC
# MAGIC - **Partition Pruning (Poda de Particiones):** Si la tabla está particionada por `state`, la consulta `WHERE state = 'CA'` ni siquiera necesita leer el Registro de Transacciones para la columna `state`. Simplemente va directamente a la carpeta `/state=CA/`, ignorando todo lo demás. El File Skipping se aplica entonces a las columnas dentro de esa partición.

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Deletion Vectors**
# MAGIC
# MAGIC Los Deletion Vectors son un mecanismo diseñado para mejorar el rendimiento de las operaciones `DELETE`, `UPDATE` y `MERGE` al evitar la reescritura de archivos Parquet de datos inmutables que contienen registros eliminados.
# MAGIC
# MAGIC ![](https://www.databricks.com/sites/default/files/inline-images/image2_63.png?v=1763403418)
# MAGIC
# MAGIC Históricamente, debido a la naturaleza inmutable de los archivos Parquet en Delta Lake, cuando se eliminaba un solo registro de un archivo de 1 GB, el archivo entero debía ser reescrito sin ese registro. Los Deletion Vectors cambian esto.
# MAGIC
# MAGIC Un Deletion Vector es un mapa de bits (bitmap) separado que se adjunta lógicamente a un archivo de datos.
# MAGIC
# MAGIC - **Mapa de Bits:** Cada bit en el vector corresponde a una fila en el archivo de datos Parquet.
# MAGIC
# MAGIC **Valor del Bit:**
# MAGIC
# MAGIC - Si el bit está establecido en 0, la fila se considera válida.
# MAGIC
# MAGIC - Si el bit está establecido en 1, la fila está "marcada" para su eliminación y debe ser omitida durante la lectura.
# MAGIC
# MAGIC **¿Cómo Funcionan?**
# MAGIC
# MAGIC Cuando ejecutas una operación de eliminación (`DELETE`) o actualización (`UPDATE`), ocurre lo siguiente:
# MAGIC
# MAGIC En lugar de reescribir todo el archivo Parquet, Delta Lake simplemente **actualiza el Registro de Transacciones** para registrar que **un nuevo Deletion Vector se ha creado y adjuntado al archivo Parquet original**.
# MAGIC
# MAGIC - **Lectura Optimizada:** Cuando un motor de consulta (como Spark) lee la tabla, primero consulta el Registro de Transacciones. Al encontrar un archivo de datos que tiene un Deletion Vector adjunto, el motor lee tanto el archivo de datos como el vector de eliminación.
# MAGIC
# MAGIC - **Filtrado en Tiempo de Ejecución:** El motor utiliza el vector de eliminación para filtrar los registros eliminados después de leerlos del archivo, pero antes de que sean devueltos al usuario.
# MAGIC
# MAGIC Para habilitar esta función en Databricks y Delta Lake, necesitas configurar la propiedad de la tabla. Aunque es el estándar en versiones modernas de Databricks, el comando para habilitarlo manualmente es:

# COMMAND ----------

# DBTITLE 1,Activar Deletion Vectors
# MAGIC %sql
# MAGIC ALTER TABLE table_name SET TBLPROPERTIES ('delta.enableDeletionVectors' = true);

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Acid Transactions**
# MAGIC
# MAGIC #### **A: Atomicidad (Atomicity)**
# MAGIC
# MAGIC - **Concepto:** Una transacción se trata como una unidad indivisible. O todas las operaciones dentro de la transacción se completan (commit), o ninguna lo hace (rollback).
# MAGIC
# MAGIC - **En Delta Lake:** Una escritura o modificación (un `INSERT`, `UPDATE`, `DELETE` o `MERGE`) se considera una sola transacción. Si la operación falla a mitad de camino (por ejemplo, por un error de hardware o de red), el Registro de Transacciones NO se actualiza con la nueva versión incompleta. Los readers (lectores) siempre ven el estado de la tabla anterior a la transacción fallida.
# MAGIC
# MAGIC #### **C: Consistencia (Consistency)**
# MAGIC
# MAGIC - **Concepto:** Una transacción solo puede llevar la base de datos de un estado válido a otro. Cualquier dato escrito debe seguir las reglas y restricciones definidas (como el Schema Enforcement).
# MAGIC
# MAGIC - **En Delta Lake:** Esto se aplica a través de dos mecanismos principales:
# MAGIC
# MAGIC - **Schema Enforcement:** Delta Lake se asegura de que los datos que intentas escribir coincidan con el esquema de la tabla. Si intentas escribir datos con tipos incorrectos o columnas faltantes, la transacción es rechazada, manteniendo la consistencia de los datos.
# MAGIC
# MAGIC - **Registro de Transacciones:** Garantiza que todos los lectores vean una visión completa y consistente de la tabla, tal como fue definida por la última versión atómica registrada.
# MAGIC
# MAGIC #### **I: Aislamiento (Isolation)**
# MAGIC
# MAGIC - **Concepto:** Múltiples transacciones que se ejecutan concurrentemente no deben interferir entre sí. Para el usuario, parece que las transacciones se ejecutan secuencialmente.
# MAGIC
# MAGIC En Delta Lake: Esto es crucial y se implementa mediante:
# MAGIC
# MAGIC - **Control de Concurrencia Optimista (Optimistic Concurrency Control):** Delta Lake utiliza un sistema de bloqueo pesimista a nivel de archivo y lectura de snapshots.
# MAGIC
# MAGIC - **Readers (Lectores):** Los lectores siempre trabajan con la última snapshot (instantánea) de la tabla registrada en el `_delta_log` al comienzo de la consulta. Esto significa que nunca ven datos parciales o sucios de una transacción en curso.
# MAGIC
# MAGIC - **Writers (Escritores):** Cuando un escritor completa su transacción, intenta registrar la nueva versión en el `_delta_log`. Antes de hacerlo, verifica si algún otro escritor modificó los archivos que el escritor actual también estaba modificando. Si hay un conflicto, el segundo escritor falla y debe reintentar la operación, asegurando que las escrituras se mantengan aisladas.
# MAGIC
# MAGIC #### **D: Durabilidad (Durability)**
# MAGIC
# MAGIC - **Concepto:** Una vez que una transacción ha sido completada (committed), sus cambios son permanentes y persistirán incluso en caso de fallo del sistema (como un corte de energía o un crash del clúster).
# MAGIC
# MAGIC - **En Delta Lake:** Cuando una transacción finaliza exitosamente, Spark escribe el archivo JSON de la nueva versión en el almacenamiento en la nube (`_delta_log/`). Dado que el almacenamiento en la nube (S3, ADLS, GCS) está diseñado para alta disponibilidad y durabilidad, una vez que el archivo de commit (compromiso) es escrito, el cambio es permanente.

# COMMAND ----------

# MAGIC %md
# MAGIC | Propiedad  | Definición  | ¿Cómo lo logra Delta Lake? |
# MAGIC |------------|-------------|-----------------------------|
# MAGIC | **Atomicity** | **Atomicidad** | Una transacción debe ser tratada como una sola unidad indivisible: **todo o nada**. Si una parte falla (ej. el clúster se cae), toda la transacción se revierte. |
# MAGIC | **Consistency** | **Consistencia** | Una transacción solo lleva la base de datos de un estado válido a otro estado válido. Esto significa que **las reglas y esquemas de la tabla se mantienen**. |
# MAGIC | **Isolation** | **Aislamiento** | Múltiples transacciones concurrentes deben ejecutarse como si fueran secuenciales. Es decir, una transacción en curso **no debe ver los resultados intermedios de otra transacción**. |
# MAGIC | **Durability** | **Durabilidad** | Una vez que una transacción ha sido confirmada (*committed*), sus cambios son permanentes y **sobreviven a fallas del sistema, cortes de energía o reinicios**. |

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Schema Enforcement**
# MAGIC
# MAGIC El Schema Enforcement es un mecanismo de protección en Delta Lake que _**impide que se escriban datos en una tabla si su esquema (estructura) no coincide con el esquema actual de la tabla.**_
# MAGIC
# MAGIC El objetivo principal es prevenir la corrupción de datos al asegurar que solo se añadan datos limpios y estructurados a la tabla.
# MAGIC
# MAGIC ![](https://delta.io/_astro/thumbnail.BCgGUhaj_Z2b3JE0.webp)
# MAGIC
# MAGIC **Reglas de Validación**
# MAGIC
# MAGIC Cuando intentas escribir un nuevo DataFrame en una tabla Delta existente, Delta Lake verifica la compatibilidad de los esquemas aplicando las siguientes reglas de validación:
# MAGIC
# MAGIC - **Tipos de Datos:** Los tipos de datos de las columnas en el nuevo DataFrame deben coincidir con los tipos de datos de las columnas correspondientes en la tabla Delta. (**Ejemplo:** no puedes intentar escribir una string en una columna definida como integer).
# MAGIC
# MAGIC - **Nombres de Columnas:** Los nombres de las columnas en el nuevo DataFrame deben coincidir exactamente con los nombres de las columnas en la tabla Delta. La comprobación distingue entre mayúsculas y minúsculas (es case-sensitive).
# MAGIC
# MAGIC - **Columnas Adicionales:** El nuevo DataFrame NO debe contener columnas adicionales que no existan en la tabla Delta de destino. Si lo hace, la operación de escritura será rechazada.
# MAGIC
# MAGIC - **Columnas Faltantes:** El nuevo DataFrame debe contener todas las columnas que están definidas en la tabla Delta. Si faltan columnas, generalmente se requiere una configuración explícita para que se acepten (por ejemplo, rellenando con valores nulos).
# MAGIC
# MAGIC Si la operación de escritura viola cualquiera de estas reglas, Delta Lake lanza una excepción (un error) y **la transacción falla atómicamente.** El Registro de Transacciones no se actualiza, y la tabla se mantiene en su estado anterior, garantizando la durabilidad y consistencia.
# MAGIC
# MAGIC **Ejemplo:**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE `2026`.deltabase.example_table
# MAGIC (id INT,
# MAGIC valid BOOLEAN,
# MAGIC name STRING
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO `2026`.deltabase.example_table
# MAGIC VALUES 
# MAGIC (1, True, "OK");

# COMMAND ----------

# DBTITLE 1,Schema Mismatch
# MAGIC %sql
# MAGIC INSERT INTO `2026`.deltabase.example_table VALUES
# MAGIC (2, False, "OK", "Test"); -- Extra value

# COMMAND ----------

# DBTITLE 1,Activar MERGE SCHEMA en SQL (temporalmente)
# MAGIC %sql
# MAGIC SET spark.databricks.delta.schema.autoMerge.enabled = true;

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Schema Evolution**
# MAGIC
# MAGIC ![](https://delta.io/_astro/thumbnail.BzmzjtjT_Z2gbx9m.webp)
# MAGIC
# MAGIC A pesar de que el Schema Enforcement es estricto, Delta Lake también ofrece un mecanismo controlado para cambiar intencionalmente el esquema de una tabla. Esto se conoce como Schema Evolution.
# MAGIC
# MAGIC Si deseas añadir nuevas columnas al DataFrame y que estas se incorporen permanentemente a la tabla Delta (violando la Regla #3), debes indicar explícitamente tu intención utilizando una opción de escritura, típicamente:

# COMMAND ----------

# DBTITLE 1,Python - Schema Evolution
df.write.format("delta") \
  .mode("append") \
  .option("mergeSchema", "true") \ # Schema Evolution
  .save(path_to_delta_table)

# COMMAND ----------

# DBTITLE 1,SQL - Schema Evolution
# MAGIC %sql
# MAGIC MERGE WITH SCHEMA EVOLUTION INTO target_table
# MAGIC USING source_table
# MAGIC ON source_table.id = target_table.id
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT *

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Time Travel**
# MAGIC
# MAGIC El Time Travel te permite **consultar versiones anteriores de tu tabla Delta.** Esto es posible porque cada modificación (commit) en la tabla crea una nueva versión atómica registrada en el `_delta_log/`.
# MAGIC
# MAGIC Cuando realizas una consulta de Time Travel, le estás diciendo a Spark que ignore la última versión y que, en su lugar, lea el conjunto de archivos Parquet que se definió como válido en una versión anterior.
# MAGIC
# MAGIC #### `RESTORE`
# MAGIC
# MAGIC El comando `RESTORE` en Delta Lake permite regresar una tabla Delta a una versión anterior, restaurando su estado y datos tal como estaban en ese momento.

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Práctica**

# COMMAND ----------

# DBTITLE 1,Crear tabla Time Travel
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE `2026`.deltabase.time_travel
# MAGIC (id INT, text STRING);

# COMMAND ----------

# DBTITLE 1,Hacer operaciones (INSERT)
# MAGIC %sql
# MAGIC INSERT INTO `2026`.deltabase.time_travel VALUES (1, "Back");
# MAGIC INSERT INTO `2026`.deltabase.time_travel VALUES (2, "To the");
# MAGIC INSERT INTO `2026`.deltabase.time_travel VALUES (3, "Future");

# COMMAND ----------

# DBTITLE 1,Borrar datos de la tabla Time Travel
# MAGIC %sql
# MAGIC DELETE FROM `2026`.deltabase.time_travel;

# COMMAND ----------

# DBTITLE 1,Ver historial de cambios (SQL)
# MAGIC %sql
# MAGIC DESCRIBE HISTORY `2026`.deltabase.time_travel;

# COMMAND ----------

# MAGIC %md
# MAGIC **Time Travel con SQL**

# COMMAND ----------

# DBTITLE 1,Time Travel con SQL
# MAGIC %sql
# MAGIC -- Time Travel a la versión 3 (WRITE)
# MAGIC SELECT * FROM `2026`.deltabase.time_travel VERSION AS OF 3;

# COMMAND ----------

# MAGIC %md
# MAGIC **Time Travel con Python**

# COMMAND ----------

# DBTITLE 1,Time Travel con Python
spark.read.option("versionAsOf", 3) \
    .table("`2026`.deltabase.time_travel") \
    .display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Restaurar Tabla con SQL**

# COMMAND ----------

# DBTITLE 1,RESTORE TABLE - Syntax
# MAGIC %sql
# MAGIC RESTORE TABLE `2026`.deltabase.time_travel VERSION AS OF 3;

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Cloning**
# MAGIC
# MAGIC En Databricks, el **Deep Clone** y el **Shallow Clone** son comandos de Delta Lake que te permiten _**crear copias de tablas existentes**_ de forma eficiente. La diferencia clave radica en si se copian los datos o solo los metadatos.
# MAGIC
# MAGIC ![](https://cdn.hashnode.com/res/hashnode/image/upload/v1652205598283/H-I7ZCkNx.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Shallow Clone (Clonación superficial)**
# MAGIC
# MAGIC > Un shallow clone crea una copia de los metadatos de la tabla, pero no de los archivos de datos subyacentes. Es como crear un atajo a los datos originales.
# MAGIC
# MAGIC - **Qué se copia:** Solo el log de transacciones de Delta Lake (`_delta_log`). Este log contiene el historial de cambios y las referencias a los archivos de datos.
# MAGIC
# MAGIC - **Qué NO se copia:** Los archivos de datos Parquet. La tabla clonada simplemente apunta a los mismos archivos de datos que la tabla original.
# MAGIC
# MAGIC - **Velocidad y costo:** Es extremadamente rápido y barato, ya que no se duplica la información en tu almacenamiento.
# MAGIC
# MAGIC - **Independencia:** Las tablas **no son independientes**. Si eliminas o modificas un registro en la tabla original, la tabla clonada se verá afectada. Si borras la tabla original, la clonada ya no tendrá datos a los que apuntar.
# MAGIC
# MAGIC - **Caso de uso:** Es ideal para crear copias temporales para pruebas o para hacer rollbacks rápidos de datos, ya que el proceso es casi instantáneo.
# MAGIC

# COMMAND ----------

# DBTITLE 1,Sintaxis - Shallow Clone
# MAGIC %sql
# MAGIC CREATE TABLE nombre_tabla_clonada SHALLOW CLONE nombre_tabla_original

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ### **Deep Clone (Clonación Profunda)**
# MAGIC
# MAGIC > Un deep clone crea una **copia completa** y totalmente **independiente de la tabla original**, incluyendo los datos y los metadatos.
# MAGIC
# MAGIC - **Qué se copia:** Tanto el log de transacciones como todos los archivos de datos Parquet que componen la tabla en el momento de la clonación.
# MAGIC
# MAGIC - **Qué NO se copia:** Nada, es una duplicación completa.
# MAGIC
# MAGIC - **Velocidad y costo:** Es lento y costoso, ya que duplica todos los archivos en tu almacenamiento en la nube, lo que consume tiempo y recursos.
# MAGIC
# MAGIC - **Independencia:** Las tablas son completamente independientes. Los cambios en una tabla no afectan a la otra.
# MAGIC
# MAGIC - **Caso de uso:** Es la opción perfecta para crear copias de seguridad de una tabla de producción, ya que la copia clonada no se verá afectada por las operaciones en la tabla original.

# COMMAND ----------

# DBTITLE 1,Sintaxis - Deep Clone
# MAGIC %sql
# MAGIC CREATE TABLE nombre_tabla_clonada DEEP CLONE nombre_tabla_original

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Change Data Feed (CDF)**
# MAGIC
# MAGIC El Change Data Feed (o Registro de Cambios de Datos) es una característica que registra a nivel de fila y columna todos los cambios realizados en una tabla Delta, incluyendo inserciones, eliminaciones y actualizaciones.
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

# DBTITLE 1,Activar CDF en Tabla Existente
# MAGIC %sql
# MAGIC -- Habilitar CDF en una tabla existente
# MAGIC ALTER TABLE my_table SET TBLPROPERTIES (
# MAGIC   'delta.enableChangeDataFeed' = 'true'
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC Si esta opción no está activada, no se podrán consultar los cambios.
# MAGIC
# MAGIC Error getting change data for range [0 , 34] as change data was not
# MAGIC recorded for version [0]. If you've enabled change data feed on this table,
# MAGIC use `DESCRIBE HISTORY` to see when it was first enabled.
# MAGIC Otherwise, to start recording change data, use `ALTER TABLE table_name SET TBLPROPERTIES
# MAGIC (delta.enableChangeDataFeed=true)`. SQLSTATE: KD002

# COMMAND ----------

# DBTITLE 1,Crear Tabla con CDF
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
# MAGIC **Lectura de Cambios**
# MAGIC
# MAGIC Una vez habilitado, puedes leer el feed de cambios como si fuera una tabla normal, especificando el rango de versiones o marcas de tiempo:

# COMMAND ----------

# DBTITLE 1,Leer cambios de CDF
# MAGIC %sql
# MAGIC -- Leer los cambios (inserts, updates, deletes) entre la versión 3 y la versión 5
# MAGIC SELECT * FROM table_changes('salesforce.bronze.opportunity', 3, 5);
# MAGIC
# MAGIC -- O leer los cambios desde un punto en el tiempo
# MAGIC SELECT * FROM table_changes('salesforce.bronze.opportunity', '2025-11-20 10:00:00');

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Optimize Data Layout**

# COMMAND ----------

# MAGIC %md
# MAGIC ### **`OPTIMIZE`**
# MAGIC
# MAGIC El comando `OPTIMIZE` está diseñado para mejorar el rendimiento de lectura de tu tabla Delta. Resuelve el problema de la fragmentación de archivos.
# MAGIC
# MAGIC **¿Qué problema resuelve?**
# MAGIC
# MAGIC Cuando se escriben datos en una tabla Delta de forma continua (especialmente en streaming o merges frecuentes), se crean muchos archivos Parquet muy pequeños. Leer muchos archivos pequeños es ineficiente y lento porque Apache Spark gasta demasiado tiempo en la sobrecarga de abrir y escanear cada archivo.
# MAGIC
# MAGIC **¿Cómo funciona?**
# MAGIC
# MAGIC `OPTIMIZE` ejecuta un proceso de compactación de archivos:
# MAGIC
# MAGIC - Consolida los archivos pequeños de la tabla.
# MAGIC
# MAGIC - Combina esos archivos en un número menor de archivos grandes (típicamente de 1 GB cada uno).
# MAGIC
# MAGIC - Actualiza el Registro de Transacciones de Delta Lake para referenciar solo los nuevos archivos grandes, haciendo que el set de datos sea más eficiente para la lectura.
# MAGIC
# MAGIC ![](https://delta.io/_astro/image1.B_cvKRuY_1JzoRa.webp)
# MAGIC
# MAGIC ![](https://delta.io/_astro/image1.Cjq8K5tu_ev2TH.webp)

# COMMAND ----------

# MAGIC %md
# MAGIC **Z-Ordering (La Optimización de Alto Nivel)**
# MAGIC
# MAGIC - La mejor forma de usar `OPTIMIZE` es combinándolo con `Z-Ordering`. 
# MAGIC
# MAGIC `Z-Ordering` es una técnica que organiza físicamente los datos dentro de los archivos de tal manera que los datos relacionados se almacenan cerca. Es como crear un índice multidimensional de alto rendimiento.
# MAGIC
# MAGIC - **Uso:** Debes especificar las columnas que usas con más frecuencia en tus filtros (`WHERE` clauses).
# MAGIC
# MAGIC - **Beneficio:** Permite al motor de consultas de Databricks saltarse grandes cantidades de datos (data skipping), leyendo solo los archivos relevantes para tu filtro, lo que acelera enormemente la consulta.
# MAGIC
# MAGIC ![](https://delta.io/_astro/image1.CM9-3R6A_1m0v86.webp)

# COMMAND ----------

# DBTITLE 1,Z-Ordering
# MAGIC %sql
# MAGIC -- Compactación simple de archivos
# MAGIC OPTIMIZE mi_tabla;
# MAGIC
# MAGIC -- Compactación y Z-Ordering por las columnas más filtradas
# MAGIC OPTIMIZE mi_tabla
# MAGIC ZORDER BY (customer_id, product_category);

# COMMAND ----------

# MAGIC %md
# MAGIC ### **`VACUUM`**
# MAGIC
# MAGIC El comando `VACUUM` es la herramienta de limpieza de Delta Lake y se utiliza para **eliminar físicamente los archivos de datos que ya no son referenciados por el Registro de Transacciones.**
# MAGIC
# MAGIC **¿Qué problema resuelve?**
# MAGIC
# MAGIC Delta Lake mantiene versiones históricas de tus datos (Time Travel). Esto significa que, después de un `UPDATE`, `DELETE` o `MERGE`, los archivos de datos viejos permanecen en el almacenamiento por un tiempo para permitir el Time Travel y las recuperaciones. _**Si no se limpian, acumulan costos de almacenamiento innecesarios.**_
# MAGIC
# MAGIC **¿Cómo funciona?**
# MAGIC
# MAGIC - **Define un Umbral de Retención:** Por defecto, `VACUUM` solo elimina los archivos que tienen más de 7 días de antigüedad para proteger las operaciones de Time Travel y las ejecuciones de streaming largas.
# MAGIC
# MAGIC - **Escaneo Seguro:** Escanea el Registro de Transacciones y **_solo elimina los archivos que no son parte de ninguna versión válida dentro del umbral de retención._**
# MAGIC
# MAGIC - **Eliminación Física:** Borra los archivos viejos del almacenamiento en la nube (S3, ADLS), liberando espacio y reduciendo costos.
# MAGIC
# MAGIC **Advertencia de Seguridad**
# MAGIC
# MAGIC Ejecutar `VACUUM` con un umbral menor al predeterminado de 7 días (por ejemplo, `RETAIN 0 HOURS`) es extremadamente peligroso para la integridad de los datos, ya que podría eliminar archivos que otros procesos (como un stream activo o una operación de Time Travel) aún necesitan.

# COMMAND ----------

# DBTITLE 1,Vacuum
# MAGIC %sql
# MAGIC -- Limpia todos los archivos no referenciados con más de 7 días de antigüedad (valor por defecto)
# MAGIC VACUUM mi_tabla;
# MAGIC
# MAGIC -- Limpia todos los archivos no referenciados con más de 24 horas de antigüedad
# MAGIC VACUUM mi_tabla RETAIN 24 HOURS;
# MAGIC
# MAGIC -- Permite simular la eliminación de los archivos sin eliminarlos realmente.
# MAGIC VACUUM mi_tabla DRY RUN;

# COMMAND ----------

# MAGIC %md
# MAGIC ![](https://miro.medium.com/v2/resize:fit:805/1*cWYfqBTxMblIJ_aIEKMMqA.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Partitioning**
# MAGIC
# MAGIC El Particionamiento es una técnica tradicional de organización de datos heredada de Apache Hive y utilizada por Apache Spark. Su objetivo principal es **mejorar la eficiencia de las consultas y gestionar el almacenamiento de datos al organizar los archivos de forma física en el File Storage (Data Lake).**
# MAGIC
# MAGIC **1. Concepto Fundamental**
# MAGIC
# MAGIC El particionamiento consiste en crear una estructura de directorios anidados basada en los valores de una o más columnas de partición. Cada valor único de la columna de partición se convierte en un subdirectorio.
# MAGIC
# MAGIC **Ejemplo de Estructura de Particionamiento:**
# MAGIC
# MAGIC Si particionas una tabla de `ventas` por `year` y luego por `country`:

# COMMAND ----------

/ventas/
├── year=2024/
│   ├── country=USA/
│   │   ├── parte-001.parquet
│   │   └── parte-002.parquet
│   ├── country=CAN/
│   │   └── parte-003.parquet
└── year=2023/
    └── country=MEX/
        └── parte-004.parquet

# COMMAND ----------

# MAGIC %md
# MAGIC **2. Mecanismo de Optimización: Partition Pruning (Poda de Particiones)**
# MAGIC
# MAGIC La principal ventaja del particionamiento es el Partition Pruning (Poda de Particiones).
# MAGIC
# MAGIC Cuando ejecutas una consulta que filtra por una columna de partición, el motor de Spark:
# MAGIC
# MAGIC - **Identifica el Filtro:** Reconoce la cláusula `WHERE` (ejemplo: `WHERE year = 2024`).
# MAGIC
# MAGIC - **Omite Directorios:** Simplemente omite (poda) todos los directorios que no coincidan con ese filtro (por ejemplo, ignora el directorio `year=2023`).
# MAGIC
# MAGIC - **Lee Mínimamente:** Solo lee los archivos de datos que están físicamente contenidos en el(los) subdirectorio(s) coincidente(s).
# MAGIC
# MAGIC Esto reduce drásticamente la cantidad de archivos que el motor necesita escanear y los metadatos que debe procesar, lo que acelera la consulta.
# MAGIC
# MAGIC **Consideraciones Críticas (Desventajas)**
# MAGIC
# MAGIC Aunque es eficaz, el particionamiento tiene riesgos y desafíos importantes en entornos de Data Lake:
# MAGIC
# MAGIC - **Granulidad Inadecuada (Demasiadas Particiones):** Si eliges una columna con demasiados valores únicos (por ejemplo, `user_id` o `timestamp`), terminarás con miles o millones de directorios que contienen muy pocos archivos pequeños. Esto lleva a un problema conocido como Small Files Problem, que sobrecarga el sistema de archivos del cloud storage y ralentiza el procesamiento de metadatos de Spark.
# MAGIC
# MAGIC - **Particionamiento Inadecuado (Muy Pocas Particiones):** Si eliges una columna con muy pocos valores únicos (por ejemplo, `gender`), las particiones serán demasiado grandes, y el Partition Pruning no será efectivo.
# MAGIC
# MAGIC - **Rigidez:** El mayor inconveniente es la rigidez. Si tu patrón de consulta cambia (por ejemplo, de consultar por `date` a consultar por `region`), la única forma de optimizar la tabla para el nuevo patrón es reescribir completamente todos los datos en la nueva estructura de directorios, lo que es costoso y consume mucho tiempo.

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Partitioning vs Liquid Clustering**
# MAGIC
# MAGIC ![](https://delta.io/_astro/image1.Bio3QTFO_Z1JChrd.webp)
# MAGIC
# MAGIC **Caso 1: Consulta por Partición (Óptimo) ✅**
# MAGIC
# MAGIC - **Consulta:** `SELECT * FROM sales_data WHERE region = 'North';`
# MAGIC
# MAGIC - **Comportamiento:** El motor de consulta (Query Engine) sabe que solo debe escanear el subdirectorio `region=North`.
# MAGIC
# MAGIC - **Resultado:** Solo lee los archivos de ese directorio (`part-00001.snappy.parquet`, etc.). Esto es extremadamente rápido y eficiente porque se aplica el Partition Pruning (poda de particiones).
# MAGIC
# MAGIC **Caso 2: Cambios de Esquema (Problemático) ❌**
# MAGIC
# MAGIC - **Consulta original (antes del cambio):** La tabla estaba optimizada para la columna region.
# MAGIC
# MAGIC - **Nuevo requerimiento/Consulta:** `SELECT * FROM sales_data WHERE year = 2024;`
# MAGIC
# MAGIC - **Comportamiento:** Dado que la tabla no está particionada por `year`, el Query Engine no puede aplicar la poda de particiones. Debe escanear todos los subdirectorios (`region=East`, `region=North`, etc.) y revisar los metadatos de todos los archivos para encontrar los datos de 2024.
# MAGIC
# MAGIC - **Resultado:** Si la necesidad de consulta principal cambia (por ejemplo, ahora quieres consultar por year), tienes que realizar una reescritura completa (complete rewrite required) de toda la tabla para cambiar la clave de partición. Esto es costoso, lento y arriesgado.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Liquid Clustering**
# MAGIC
# MAGIC Liquid Clustering es una técnica de Databricks que aplica una organización de datos flexible a nivel de archivo dentro de la tabla Delta, en lugar de a nivel de directorio.
# MAGIC
# MAGIC **Caso 1: Consulta por Clave de Clustering (Óptimo) ✅**
# MAGIC
# MAGIC - **Consulta:** `SELECT * FROM sales_data WHERE region = 'North';`
# MAGIC
# MAGIC - **Comportamiento:** Asumiendo que `region` es una clave de clustering. Liquid Clustering asegura que las filas con valores similares de `region` (por ejemplo, `'North'`) estén físicamente ubicadas cerca unas de otras dentro de los archivos Parquet.
# MAGIC
# MAGIC - **Resultado:** El motor de consulta usa los metadatos de los archivos (similar al File Skipping) para identificar y leer _solo los archivos que contienen datos de `'North'`_, logrando el mismo rendimiento que el particionamiento sin la estructura de carpetas rígida.
# MAGIC
# MAGIC **Caso 2: Cambios de Esquema (Flexible) ✅**
# MAGIC
# MAGIC - **Consulta:** `SELECT * FROM sales_data WHERE year = 2024;`
# MAGIC
# MAGIC - **Comportamiento:** Si la necesidad cambia y ahora necesitas consultar por `year`, puedes simplemente cambiar la clave de clustering de la tabla para que ahora incluya o se centre en `year` (o incluso tener múltiples claves).
# MAGIC
# MAGIC - **Resultado:** El motor reescribirá la tabla de manera incremental y eficiente para organizar los datos por `year` sin la costosa reescritura completa que requiere el particionamiento. La gran ventaja es que, dado que el clustering se aplica dentro de la tabla, puedes tener hasta 4 claves de clustering simultáneas que optimizan para diferentes patrones de consulta.
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Crear una tabla con Liquid Clustering
# MAGIC %sql
# MAGIC CREATE TABLE nombre_tabla (
# MAGIC     id INT,
# MAGIC     nombre STRING,
# MAGIC     pais STRING
# MAGIC )
# MAGIC CLUSTER BY (pais); -- Aquí se define la columna de clustering

# COMMAND ----------

# MAGIC %md
# MAGIC ### ¿Qué columnas elegir para Liquid Clustering?
# MAGIC
# MAGIC Elegir las columnas correctas para el Liquid Clustering es la clave para obtener un buen rendimiento. La regla principal es simple: elige las columnas que usas con más frecuencia en las cláusulas `WHERE` (para filtrar tus consultas).
# MAGIC
# MAGIC **Por qué esta es la regla**
# MAGIC
# MAGIC El objetivo de Liquid Clustering es organizar tus datos en el almacenamiento para que el motor de consultas de Databricks tenga que **escanear la menor cantidad de archivos posible.** Al agrupar los datos por las columnas que usas para filtrar, logras este objetivo.
# MAGIC
# MAGIC Imagina que tienes una tabla de `ventas` con millones de registros. Si agrupas los datos por `product_id` y ejecutas la consulta `SELECT * FROM ventas WHERE product_id = 'XYZ'`, Databricks sabrá que solo necesita leer los pocos archivos que contienen los datos para ese `product_id` específico, en lugar de escanear toda la tabla.
# MAGIC
# MAGIC **Guía para Elegir Columnas**
# MAGIC
# MAGIC - **Prioriza columnas de alta cardinalidad:** Las columnas con muchos valores únicos son las mejores candidatas. Por ejemplo, `order_id`, `customer_id`, o `session_id`. Estas columnas permiten que Liquid Clustering cree agrupaciones muy específicas y eficientes.
# MAGIC
# MAGIC - **Combina columnas si es necesario:** Si tus consultas suelen filtrar por una combinación de columnas (por ejemplo, `WHERE cliente_id = 123 AND fecha = '2025-09-15'`), Liquid Clustering te permite agrupar los datos por ambas.
# MAGIC
# MAGIC - **Evita columnas de muy baja cardinalidad:** No elijas columnas con pocos valores únicos, como `true/false`, `on/off` o `gender`. Esto no ayuda a crear agrupaciones significativas y puede llevar a un rendimiento pobre. La excepción es si se usa en combinación con otras columnas.
# MAGIC
# MAGIC - **No elijas demasiadas columnas:** Se recomienda no usar más de cuatro columnas para el clustering. Añadir demasiadas puede generar un exceso de archivos pequeños y, paradójicamente, empeorar el rendimiento.

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Bloom Filters**
# MAGIC
# MAGIC Un Bloom Filter es una estructura de datos probabilística y eficiente en espacio utilizada **para probar si un elemento es o no miembro de un conjunto.**
# MAGIC
# MAGIC En Delta Lake, los Bloom Filters se utilizan para crear un índice a nivel de archivo en columnas específicas. Este índice se utiliza durante la lectura para determinar rápidamente si un archivo Parquet no contiene definitivamente el valor que se está buscando.
# MAGIC
# MAGIC **1. El Mecanismo Probabilístico**
# MAGIC
# MAGIC Los Bloom Filters funcionan con dos posibles resultados:
# MAGIC
# MAGIC - **"Definitivamente no está allí":** Si el filtro dice que el valor no está en el archivo, puedes estar 100% seguro de que no lo está. El archivo es omitido.
# MAGIC
# MAGIC - **"Podría estar allí":** Si el filtro dice que el valor podría estar, existe una pequeña probabilidad de que sea un falso positivo (el filtro dice que está, pero no lo está). El archivo debe ser leído (aunque sea innecesariamente).
# MAGIC
# MAGIC Debido a su naturaleza probabilística, son muy rápidos y utilizan muy poco espacio de almacenamiento.
# MAGIC
# MAGIC **2. ¿Cómo Complementan al File Skipping?**
# MAGIC
# MAGIC - **File Skipping:** Utiliza las estadísticas `min` / `max` a nivel de archivo. Es excelente para filtros de rango (ej: `WHERE salary > 50000`).
# MAGIC
# MAGIC - **Bloom Filters:** Se centran en la pertenencia a un conjunto de valores discretos. Son excelentes para filtros de igualdad y pertenencia a conjuntos (ej: `WHERE user_id = 'XYZ'`) o para acelerar las condiciones de `join`.
# MAGIC
# MAGIC Cuando ejecutas una consulta, el proceso de optimización ocurre así:
# MAGIC
# MAGIC - **Partition Pruning:** ¿La consulta filtra por una clave de partición? Si es así, se omiten directorios completos.
# MAGIC
# MAGIC - **File Skipping:** ¿Los valores de búsqueda caen fuera del rango `min` / `max` de los archivos restantes? Si es así, se omiten esos archivos.
# MAGIC
# MAGIC - **Bloom Filter:** Para los archivos que quedan, si la columna tiene un Bloom Filter, este se consulta. Si el filtro dice que el valor está definitivamente ausente, el archivo se omite, **incluso si sus valores `min` / `max` caían en el rango.**
# MAGIC
# MAGIC Para utilizar los Bloom Filters, debes crearlos explícitamente en columnas que se consultan frecuentemente usando filtros de igualdad:
# MAGIC
# MAGIC **1. Creación**
# MAGIC
# MAGIC Debes especificar en qué columnas deseas crear el índice de Bloom Filter:

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE BLOOMFILTER INDEX ON TABLE
# MAGIC   table_name
# MAGIC FOR COLUMNS
# MAGIC   user_id,
# MAGIC   user_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ## `CONVERT TO DELTA`
# MAGIC
# MAGIC CONVERT TO DELTA es una herramienta fundamental en Databricks cuando estás migrando datos desde formatos tradicionales (como Parquet) al formato Delta Lake.
# MAGIC
# MAGIC Básicamente, este comando transforma una tabla de Parquet existente en una tabla Delta **sin mover los datos ni reescribirlos**, lo que lo hace extremadamente rápido y eficiente.
# MAGIC
# MAGIC #### **Funcionamiento**
# MAGIC
# MAGIC En lugar de copiar los archivos, el comando escanea el directorio de la tabla y crea un Delta Log (la carpeta `_delta_log`). Este log registra todos los archivos de datos existentes para que, a partir de ese momento, Databricks los maneje con todas las ventajas de Delta: transacciones ACID, "Time Travel" y cumplimiento de esquemas.
# MAGIC
# MAGIC Para que funcione en un directorio entero, los archivos deben tener el mismo esquema (mismas columnas y tipos de datos).
# MAGIC
# MAGIC - **Si el directorio no es particionado:** Todos los archivos deben estar directamente en la carpeta raíz que indiques.
# MAGIC
# MAGIC - **Si el directorio es particionado:** Los archivos deben estar organizados en subcarpetas siguiendo el estándar de Hive (ej. `/año=2024/mes=01/archivo.parquet`). En este caso, debes usar la cláusula `PARTITIONED BY` para que la conversión sea exitosa.
# MAGIC
# MAGIC Si tienes una estructura en tu Data Lake así:

# COMMAND ----------

# MAGIC %%bash
# MAGIC /mnt/data/ventas_historial/
# MAGIC     ├── part-00001.parquet
# MAGIC     ├── part-00002.parquet
# MAGIC     └── ...

# COMMAND ----------

# MAGIC %md
# MAGIC El comando sería

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA parquet.`/mnt/data/ventas_historial/`

# COMMAND ----------

# MAGIC %md
# MAGIC > Actualmente, el comando oficial está diseñado para archivos Parquet. Si tienes CSV o JSON, lo ideal es leerlos y guardarlos como Delta usando `df.write.format("delta").save()`.

# COMMAND ----------

# DBTITLE 1,Convert to Delta - Syntax
# MAGIC %sql
# MAGIC CONVERT TO DELTA parquet.`/ruta/a/la/tabla`

# COMMAND ----------

# MAGIC %sql
# MAGIC -- From S3
# MAGIC CONVERT TO DELTA parquet.`s3://my-bucket/path/to/table`

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Isolation Levels**
# MAGIC
# MAGIC Los Isolation Levels (Niveles de Aislamiento) son una parte crítica del estándar ACID en bases de datos. Definen cómo se separan las transacciones entre sí cuando ocurren al mismo tiempo.
# MAGIC
# MAGIC En el contexto de Databricks y Delta Lake, esto es vital porque permite que un Data Engineer esté cargando datos (escritura) mientras un Analyst está consultando (lectura) sin que los resultados sean inconsistentes.
# MAGIC
# MAGIC #### **El problema: Los fenómenos de lectura**
# MAGIC
# MAGIC Antes de entender los niveles, debemos saber qué intentan evitar:
# MAGIC
# MAGIC - **Dirty Read (Lectura sucia):** Leer datos de una transacción que aún no se ha confirmado (COMMIT). Si esa transacción falla y hace rollback, habrás leído datos que "nunca existieron".
# MAGIC
# MAGIC - **Non-repeatable Read:** Lees una fila, otra transacción la modifica y, al volver a leerla, el valor ha cambiado.
# MAGIC
# MAGIC - **Phantom Read (Lectura fantasma):** Haces una consulta con un filtro (ej. `precio > 10`), otra transacción inserta nuevas filas que cumplen ese criterio, y al repetir tu consulta aparecen filas "fantasma" que no estaban antes.
# MAGIC
# MAGIC #### **Niveles de Aislamiento en Delta Lake**
# MAGIC
# MAGIC A diferencia de las bases de datos tradicionales (como MySQL o Postgres) que ofrecen 4 niveles, Delta Lake en Databricks se centra principalmente en dos para garantizar el alto rendimiento en Big Data:
# MAGIC
# MAGIC **1. WriteSerializable (Por defecto)**
# MAGIC
# MAGIC Es el nivel estándar. Garantiza que las operaciones de escritura se vean como si hubieran ocurrido una tras otra (secuencialmente).
# MAGIC
# MAGIC - **Lo que permite:** Lecturas rápidas y consistentes.
# MAGIC
# MAGIC - **El riesgo:** Permite ciertos "fantasmas" durante la lectura si hay inserciones simultáneas, pero asegura que el orden de las operaciones de escritura sea lógico.
# MAGIC
# MAGIC **2. Serializable**
# MAGIC
# MAGIC Es el nivel más restrictivo y seguro.
# MAGIC
# MAGIC - **Cómo funciona:** Las transacciones se ejecutan de tal manera que el resultado es exactamente el mismo que si se hubieran ejecutado una por una, sin solapamiento.
# MAGIC
# MAGIC - **Uso:** Se usa cuando la precisión absoluta es requerida y no puedes permitir que una inserción simultánea afecte el resultado de una operación compleja.

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Managed Table**
# MAGIC
# MAGIC Con una tabla **managed**, Databricks tiene el control total sobre los datos y los metadatos.
# MAGIC
# MAGIC - **¿Dónde están los datos?** Databricks almacena los archivos de datos en una ubicación de almacenamiento que gestiona por ti (la "raíz" de tu metastore o de tu Unity Catalog). **El usuario no necesita especificar la ruta de los archivos.**
# MAGIC
# MAGIC - **Gestión del ciclo de vida:** Cuando eliminas una tabla **managed** (usando `DROP TABLE`), Databricks no solo borra los metadatos de la tabla, sino que también elimina permanentemente los archivos de datos subyacentes del almacenamiento.
# MAGIC
# MAGIC - **Caso de uso:** Son ideales para datos de "producción" o para conjuntos de datos intermedios que no necesitas conservar fuera del entorno de Databricks. **Son el tipo de tabla predeterminado.**
# MAGIC
# MAGIC _Las tablas MANAGED se crean por defecto_

# COMMAND ----------

# MAGIC %md
# MAGIC ## **External Tables**
# MAGIC
# MAGIC > Con una tabla **external**, Databricks *solo gestiona los metadatos*. La vida útil de los datos es independiente de la tabla.
# MAGIC
# MAGIC - **¿Dónde están los datos?** Los datos se almacenan en una ubicación que tú especificas explícitamente (por ejemplo, un bucket de S3 o un contenedor de ADLS). Eres tú quien controla el almacenamiento de los archivos.
# MAGIC
# MAGIC - **Gestión del ciclo de vida:** Cuando eliminas una tabla **external**, Databricks solo borra los metadatos de la tabla en el metastore. Los archivos de datos subyacentes permanecen intactos en tu almacenamiento.
# MAGIC
# MAGIC - **Caso de uso:** Son perfectas para cuando necesitas compartir datos con otras herramientas que no son de Databricks, o para cuando quieres que los datos persistan incluso si la definición de la tabla se elimina. También se usan para acceder a datos que ya existen en tu data lake.
# MAGIC
# MAGIC **Ejemplo:**

# COMMAND ----------

# DBTITLE 1,EXTERNAL TABLE - Syntax
# MAGIC %sql
# MAGIC CREATE EXTERNAL TABLE mi_tabla_external 
# MAGIC (
# MAGIC id INT, 
# MAGIC nombre STRING
# MAGIC ) USING DELTA LOCATION 's3://mi-bucket/datos/mi_tabla_external'

# COMMAND ----------

# MAGIC %md
# MAGIC ![](https://cdn.prod.website-files.com/66954b344e907bd91f1c8027/6835e83fff2e3b864fa1941a_AD_4nXfDKyo3WXqPNPYDnNkIb_Br1SR8ueQoQWA5JvZzP5GHlB_zeNGEdUQNjpdJqBb447OzLQgeygdcKR4ygZVA_1-heWgDOq447fioCXnMTK62rXLM4szHwDGyJvvPxYWjFiXp_bStzw.png)
# MAGIC
# MAGIC | Característica | **Tabla Managed** (Gestionada) | **Tabla External** (Externa) |
# MAGIC | :--- | :--- | :--- |
# MAGIC | **Control** | Databricks controla tanto los datos como los metadatos | Databricks controla solo los metadatos |
# MAGIC | **Ubicación de datos** | Ubicación predeterminada gestionada por Databricks | Ubicación de almacenamiento especificada por el usuario (`LOCATION`) |
# MAGIC | **Eliminación** | `DROP TABLE` elimina **datos y metadatos** | `DROP TABLE` elimina solo los **metadatos**; los datos persisten |
# MAGIC | **Creación** | Opción predeterminada (`CREATE TABLE ...`) | Requiere la palabra clave `EXTERNAL` y `LOCATION` |

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Delta Sharing**
# MAGIC
# MAGIC ![](https://www.databricks.com/sites/default/files/styles/max_1000x1000/public/2025-10/top-10-delta-sharing-questions-answered-part-1-og-image.png?itok=agR2IEsl&v=1760460897)
# MAGIC
# MAGIC Delta Sharing es un protocolo abierto y seguro desarrollado por Databricks que te **_permite compartir datos (tablas Delta) almacenados en tu cloud con usuarios o sistemas fuera de tu organización_**, incluso si no usan Databricks o no están en la misma nube (AWS, Azure, GCP).
# MAGIC
# MAGIC El objetivo es eliminar la necesidad de copiar datos. En lugar de enviar archivos masivos o configurar ETLs complejos entre empresas, solo compartes un enlace seguro para que el receptor pueda leer los datos.

# COMMAND ----------

# MAGIC %md
# MAGIC **¿Cómo Funciona?**
# MAGIC
# MAGIC ![](https://www.databricks.com/sites/default/files/inline-images/top-10-delta-sharing-questions-answered-part-1-image-4.png)
# MAGIC
# MAGIC Delta Sharing se apoya en el concepto de compartición segura de metadatos. El proceso es simple:
# MAGIC
# MAGIC - **El Proveedor (Tú):** Compartes una o más tablas Delta a través de un servidor de Delta Sharing.
# MAGIC
# MAGIC - **El Servidor:** Genera un archivo de credenciales (`.share`) que contiene un token seguro y la URL del servidor de Delta Sharing.
# MAGIC
# MAGIC - **El Receptor (Tu Cliente/Socio):** Usa el archivo de credenciales para conectarse al servidor de Delta Sharing y puede leer los datos directamente desde tu cloud usando cualquier herramienta que soporte el protocolo (Databricks, Spark, Pandas, Power BI, etc.).
# MAGIC
# MAGIC - Lo importante es que el receptor solo lee los archivos de datos que están en tu almacenamiento en la nube, sin tener acceso a tu cuenta de cloud, tus credenciales, o tu infraestructura.

# COMMAND ----------

# MAGIC %md
# MAGIC **Ventajas Clave**
# MAGIC
# MAGIC - **Es Abierto:** El receptor no necesita usar Databricks. Pueden usar Apache Spark, Pandas, lenguajes como R y Python, o herramientas de BI como Power BI y Tableau, siempre que tengan un conector de Delta Sharing.
# MAGIC
# MAGIC - **Seguridad y Gobernanza:** Al integrarse con Unity Catalog, puedes gestionar permisos a nivel de columna o fila, y revocar el acceso instantáneamente si es necesario.
# MAGIC
# MAGIC - **No Copia de Datos:** Elimina los costos, la latencia y la complejidad de duplicar los datos. El consumidor siempre ve la última versión de la tabla (o la versión que se decida compartir) sin esperar por la sincronización.
# MAGIC
# MAGIC - **Soporte Multi-Cloud:** Puedes compartir datos de AWS con un socio en Azure, o viceversa, sin tener que mover los datos.
# MAGIC
# MAGIC **Limitaciones de Delta Sharing**
# MAGIC
# MAGIC - **Costo de Egreso (Egress Costs):** Esta es la "letra chica". Aunque no pagas por mover los datos, si tu receptor está en otra región de la nube o en otra nube distinta (ej: tú en Azure y ellos en AWS), tu nube te cobrará por la transferencia de datos que ellos descarguen.
# MAGIC
# MAGIC - **Solo lectura (Read-Only):** Delta Sharing está diseñado para compartir, no para colaborar. El receptor puede leer y descargar los datos, pero no puede insertar o borrar filas en tu tabla original.
# MAGIC
# MAGIC - **Restricciones de Delta Lake:** No todas las funciones avanzadas de Delta funcionan siempre a través de Sharing. Por ejemplo, los Deletion Vectors que mencionamos antes o el Column Mapping requieren que el receptor tenga una versión muy reciente del cliente de Delta Sharing para poder leerlos correctamente.
# MAGIC
# MAGIC - **Gestión de Tokens:** Si usas el modelo de "Open Sharing" (con receptores que no tienen Databricks), tienes que gestionar archivos de credenciales (`.share`) de forma manual y segura. Si el token expira o se pierde, el cliente pierde el acceso.