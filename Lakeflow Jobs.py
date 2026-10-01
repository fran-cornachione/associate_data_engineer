# Databricks notebook source
# MAGIC %md
# MAGIC **A Job is the primary resource for:**
# MAGIC - Scheduling
# MAGIC - Coordinating
# MAGIC - Running operations such as data processing, ETL, analytics, and machine learning workloads within the Databricks environment.
# MAGIC - A task is a single unit of work within a job that executes a specific workload such as a notebook, script, query and more.
# MAGIC - Each job consists of one or more tasks, which are the individual units of work that make up the job.
# MAGIC
# MAGIC Jobs consist of one or more tasks, and there are many different task types available. You can use Databricks Notebooks in any supported language, Python Scripts, Python Wheels for packaged code, SQL Files and Queries for data transformations, DLT (Declarative Pipelines), dbt for data transformation, Java JAR files, Spark Submit jobs for legacy Spark applications, AI/BI Dashboards for visualization, and even Power BI integration.
# MAGIC
# MAGIC This variety ensures that you can orchestrate virtually any type of workload within your job.
# MAGIC
# MAGIC When you create a job, you can set specific configurations for each particular task. The options available depend on the task type you select. Common configuration options include defining the path to your code, adding libraries, setting parameters, enabling notifications, and configuring retry policies.
# MAGIC
# MAGIC These configurations allow you to better orchestrate each particular task according to your specific requirements.

# COMMAND ----------

# MAGIC %md
# MAGIC ![](https://miro.medium.com/v2/resize:fit:1400/1*8FZWMkgMAy-tPKvRbcK7lw.png)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Notebook Tasks**
# MAGIC
# MAGIC - Source
# MAGIC - Path
# MAGIC - Compute Options
# MAGIC
# MAGIC #### **SQL Tasks**
# MAGIC
# MAGIC - Task name
# MAGIC - SQL Query
# MAGIC - SQL Warehouse

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Control Flows**
# MAGIC
# MAGIC #### Sequential Flow
# MAGIC
# MAGIC Tasks are executed one after another in a specific order.
# MAGIC
# MAGIC - **Mechanism:** It is defined by setting Dependencies ("Depends on"). A downstream task will not start until its parent task  completes successfully.
# MAGIC
# MAGIC #### Parallel Execution
# MAGIC
# MAGIC Databricks can execute multiple tasks simultaneously to reduce the total "wall-clock" time of the job.
# MAGIC
# MAGIC - **Mechanism:** Tasks that share the same parent (or have no parent at all) and don't depend on each other will run in parallel.
# MAGIC
# MAGIC - **Use Case:** Ingesting 10 independent source tables at once. Instead of waiting for each one to finish, you trigger all of them at the start of the job.
# MAGIC
# MAGIC #### Conditionals (If / Else Logic)
# MAGIC
# MAGIC Conditionals allow your workflow to branch based on specific logic or the outcome of a previous task.
# MAGIC
# MAGIC - **The "If/Else" Task:** You can add a task type called **Condition**. It evaluates a boolean expression using job parameters or task values (e.g., `{{job.parameters.environment}} == 'PROD'`).
# MAGIC
# MAGIC - **Execution Paths:** If the condition is true, it follows the "True" branch; otherwise, it follows the "False" branch.
# MAGIC
# MAGIC - **Use Case:** Running a data validation check. If the data quality is high (True), proceed to the Gold layer; if it fails (False), send an email notification and stop.
# MAGIC
# MAGIC #### Run Job (Modular Task)
# MAGIC
# MAGIC This allows you to **trigger another job as a task within your current workflow**. It promotes the "Don't Repeat Yourself" (DRY) principle.
# MAGIC
# MAGIC - **Modularity:** You can create small, specialized jobs (e.g., a "Data Vacuum" job or a "Slack Notification" job).
# MAGIC
# MAGIC - **Integration:** The parent job waits for the child job to complete before moving to the next task.
# MAGIC
# MAGIC - **Use Case:** A large enterprise pipeline that calls a pre-existing "Shared Security Audit" job used by multiple teams.
# MAGIC
# MAGIC #### For Each Loop
# MAGIC
# MAGIC The For Each task allows you to **iterate over a collection** (like a list of files, tables, or dates) and execute a task for each item.
# MAGIC
# MAGIC - **Parameterization:** You pass a JSON-formatted list to the loop, and it assigns each element to a variable (e.g., {{item}}) for the nested task.
# MAGIC
# MAGIC - **Concurrency:** You can control how many iterations run in parallel (e.g., processing 5 partitions at a time).
# MAGIC
# MAGIC - **Use Case:** You have a list of 20 regions (UK, USA, Spain, etc.). Instead of creating 20 separate tasks, you use one For Each loop to run the same processing notebook 20 times with a different "Region" parameter.

# COMMAND ----------

# MAGIC %md
# MAGIC ![](https://cdn5.dcbstatic.com/files/d/a/databricks_docebosaas_com/1767819600/VyoFcUu7LxPchoEE_q4uyQ/authoring/927/927_full_slide9_1.jpg)

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Serverless Performance Mode**

# COMMAND ----------

# MAGIC %md
# MAGIC ![](https://cdn5.dcbstatic.com/files/d/a/databricks_docebosaas_com/1767819600/VyoFcUu7LxPchoEE_q4uyQ/authoring/927/927_full_slide10_1.jpg)

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Selecting Compute**
# MAGIC
# MAGIC ![](https://cdn5.dcbstatic.com/files/d/a/databricks_docebosaas_com/1767819600/VyoFcUu7LxPchoEE_q4uyQ/authoring/927/927_full_slide11_1.jpg)

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Modular Design in Jobs**
# MAGIC
# MAGIC ![](https://cdn5.dcbstatic.com/files/d/a/databricks_docebosaas_com/1767826800/SMM7QRrvRcK9Iv-RZ4mPcg/authoring/934/934_full_slide8_1.jpg)