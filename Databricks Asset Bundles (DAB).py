# Databricks notebook source
# MAGIC %md
# MAGIC #### **1. ¿Qué son los DABs?**
# MAGIC
# MAGIC Es un framework basado en Infraestructura como Código (IaC). En lugar de ir a la interfaz de Databricks y crear un Job o un Pipeline a mano (haciendo clics), escribes un archivo de configuración (YAML) que describe todo lo que necesitas. Luego, con un comando de terminal (`databricks bundle deploy`), Databricks construye todo por ti.

# COMMAND ----------

# MAGIC %md
# MAGIC #### **2. Estructura de un Asset Bundle**
# MAGIC
# MAGIC La guía del examen menciona específicamente que debes identificar su estructura. Un bundle típico se compone de:
# MAGIC
# MAGIC - **Archivo `databricks.yml`:** Es el corazón del bundle. Aquí defines el nombre del proyecto y las configuraciones globales.
# MAGIC
# MAGIC - **Recursos (Resources):** Aquí defines qué vas a desplegar, como por ejemplo:
# MAGIC
# MAGIC - **Jobs:** La configuración de tus tareas programadas.
# MAGIC
# MAGIC - **Delta Live Tables (DLT):** Tus pipelines declarativos.
# MAGIC
# MAGIC - **Targets (Entornos):** Permite definir diferentes configuraciones para dev (desarrollo), staging y prod (producción). Por ejemplo, en "dev" puedes usar un cluster pequeño y en "prod" uno con auto-escalado.
# MAGIC
# MAGIC - **Archivos de código:** Tus notebooks (`.py`, `.sql`) o archivos de Python (`.py`) que contienen la lógica del negocio.

# COMMAND ----------

# MAGIC %md
# MAGIC #### **3. Diferencia con métodos tradicionales**
# MAGIC
# MAGIC | Característica               | Método Tradicional (Manual/UI)                                  | Databricks Asset Bundles (DABs)                                                                 |
# MAGIC |-------------------------------|-----------------------------------------------------------------|-------------------------------------------------------------------------------------------------|
# MAGIC | Consistencia                  | Difícil de replicar exactamente en otro entorno.                | Idéntico en cada despliegue gracias al código.                                                  |
# MAGIC | Control de Versiones          | Los cambios en la UI no se guardan en Git fácilmente.           | Todo el pipeline vive en Git, permitiendo ver quién cambió qué.                                 |
# MAGIC | CI/CD                         | Requiere procesos manuales o scripts complejos.                 | Se integra nativamente con herramientas como Azure DevOps, GitHub Actions o GitLab.             |
# MAGIC | Escalabilidad                 | Tedioso de manejar si tienes cientos de Jobs.                   | Puedes gestionar proyectos masivos con un solo archivo de configuración.                        |

# COMMAND ----------

# MAGIC %md
# MAGIC #### **4. ¿Cómo funciona en la práctica?**
# MAGIC
# MAGIC Para tu meta de trabajar como Data Engineer, este será tu flujo diario:
# MAGIC
# MAGIC - Desarrollas tu lógica en un IDE (como VS Code) o en Databricks Connect.
# MAGIC
# MAGIC - Defines la infraestructura en el archivo YAML.
# MAGIC
# MAGIC - Validas que no haya errores de sintaxis con `databricks bundle validate`.
# MAGIC
# MAGIC - Despliegas a producción con un solo comando.

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Práctica**
# MAGIC
# MAGIC Imagina que queremos automatizar el proceso de ingesta de clientes que estuvimos practicando.

# COMMAND ----------

# MAGIC %md
# MAGIC #### **1. Estructura de archivos del Bundle**
# MAGIC
# MAGIC Esta es la organización que verías en tu computadora (por ejemplo, usando VS Code):

# COMMAND ----------

# MAGIC %%bash
# MAGIC mi_proyecto_bundle/
# MAGIC ├── databricks.yml          # Configuración principal del bundle
# MAGIC ├── bundle.dlt.yml          # (Opcional) Definición de pipelines de DLT
# MAGIC ├── src/
# MAGIC │   └── ingesta_clientes.py # Tu código de lógica (Notebook o Python)
# MAGIC └── resources/
# MAGIC     └── jobs_config.yml     # Definición de los Jobs (tareas programadas)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **2. El archivo de configuración (`databricks.yml`)**
# MAGIC
# MAGIC Este archivo le dice a Databricks quién eres y a dónde quieres enviar el código. Es el corazón del "Asset Bundle".

# COMMAND ----------

# MAGIC %%bash
# MAGIC bundle:
# MAGIC   name: ingesta-clientes-bundle
# MAGIC
# MAGIC # Definimos los entornos (Targets)
# MAGIC targets:
# MAGIC   dev:
# MAGIC     workspace:
# MAGIC       host: https://adb-12345.azuredatabricks.net
# MAGIC     mode: development # Permite despliegues rápidos para pruebas
# MAGIC
# MAGIC   prod:
# MAGIC     workspace:
# MAGIC       host: https://adb-12345.azuredatabricks.net
# MAGIC     mode: production # Aplica restricciones de seguridad y logs

# COMMAND ----------

# MAGIC %md
# MAGIC #### **3. Definición de Recursos (`resources/jobs_config.yml`)**
# MAGIC
# MAGIC Aquí es donde reemplazas los "clics" de la interfaz por código. Defines el Job que ejecutará tu tarea.

# COMMAND ----------

# MAGIC %%bash
# MAGIC resources:
# MAGIC   jobs:
# MAGIC     job_ingesta_diaria:
# MAGIC       name: Job_Ingesta_Clientes_Diario
# MAGIC       tasks:
# MAGIC         - task_key: procesar_bronce
# MAGIC           notebook_task:
# MAGIC             notebook_path: ../src/ingesta_clientes.py
# MAGIC           new_cluster: # El bundle crea el cluster automáticamente
# MAGIC             spark_version: "15.4.x-scala2.12"
# MAGIC             node_type_id: "Standard_DS3_v2"
# MAGIC             num_workers: 1

# COMMAND ----------

# MAGIC %md
# MAGIC #### **4. Código de lógica (`src/ingesta_clientes.py`)**
# MAGIC
# MAGIC Este es el archivo que ya sabes escribir, el que usa Auto Loader .

# COMMAND ----------

# logic en src/ingesta_clientes.py
import dlt

@dlt.table
def customers_raw():
  return (
    spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "parquet")
      .load("/Volumes/associate_data_engineer/default/raw_data/customers/")
  )

# COMMAND ----------

# MAGIC %md
# MAGIC #### **5. ¿Cómo se despliega? (DAB vs Tradicional)**
# MAGIC
# MAGIC En el examen te preguntarán por la diferencia entre este método y el tradicional.
# MAGIC
# MAGIC - **Método Tradicional:** Tendrías que subir el archivo manualmente, ir a la pestaña "Workflows", crear el Job, configurar el cluster a mano, etc.
# MAGIC
# MAGIC - **Con DABs:** Simplemente abres tu terminal y escribes:

# COMMAND ----------

# MAGIC %%bash
# MAGIC databricks bundle deploy -t dev

# COMMAND ----------

# MAGIC %md
# MAGIC Databricks leerá tus archivos YAML y creará automáticamente el Job, el Cluster y subirá el código en un solo paso.

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Databricks CLI**