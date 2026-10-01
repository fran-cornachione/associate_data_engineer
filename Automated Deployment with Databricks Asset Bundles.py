# Databricks notebook source
# MAGIC %md
# MAGIC ### **DevOps Review**
# MAGIC
# MAGIC DevOps is a culture and set of practices that combines Software Engineering Best Practices with IT Operations to deliver software more rapidly, efficiently, and with higher quality.  
# MAGIC It’s all about fostering collaboration between development and operations teams to automate your workflows, streamline the processes, and ensure continuous delivery of applications.  
# MAGIC
# MAGIC Key benefits of DevOps include:  
# MAGIC
# MAGIC - Faster deployment cycles  
# MAGIC - Improved collaboration between teams  
# MAGIC - Enhanced system reliability  
# MAGIC - Better scalability and efficiency  
# MAGIC
# MAGIC In short, DevOps is a way to build and deliver software quickly and reliably by bridging the gap between development and operations.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC We can apply DevOps principles to Data Engineering and Machine Learning. Remember, DevOps is all about automating processes, improving collaboration, testing, and speeding up delivery.
# MAGIC
# MAGIC - **DataOps** is a subset of DevOps and applies DevOps to data engineering. It automates the management of data pipelines, ensuring smooth, reliable data flows from collection to processing. This means fewer bottlenecks and faster insights.
# MAGIC
# MAGIC - **MLOps** is about applying DevOps to machine learning. It streamlines the process of deploying and managing ML models, ensuring that models move from development to production quickly and are monitored for performance.

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Continuous Integration and Countinuous Deployment/Delivery (CI/CD)**
# MAGIC
# MAGIC CI/CD is a key subset of DevOps practices that focuses on **automating code integration, testing, and delivery.** 
# MAGIC
# MAGIC - Within the DevOps lifecycle, **continuous integration (CI)** emphasizes planning, development, environment management, and testing of the pipelines.
# MAGIC
# MAGIC - On the other hand, **continuous deployment/delivery (CD)** focuses on automating release processes, deployment, operation, and monitoring of these pipelines.

# COMMAND ----------

# MAGIC %md
# MAGIC #### **1. Unit Tests (La base: Pruebas Unitarias)**
# MAGIC
# MAGIC - Son las pruebas más rápidas, baratas y frecuentes. Se centran en piezas de código aisladas.
# MAGIC
# MAGIC - **Qué prueban:** Funciones o métodos individuales de Python/PySpark en aislamiento.
# MAGIC
# MAGIC - **Ejemplo en Databricks:** Si escribes una función personalizada para limpiar strings o calcular una métrica, haces un Unit Test para asegurar que con una entrada X, siempre de la salida Y.
# MAGIC
# MAGIC - **Características:** Son automatizadas y proporcionan la mayor cobertura de código.
# MAGIC
# MAGIC #### **2. Integration Tests (Nivel medio: Pruebas de Integración)**
# MAGIC
# MAGIC Aquí verificas que los distintos componentes que ya probaste por separado funcionen bien cuando se conectan.
# MAGIC
# MAGIC - **Qué prueban:** La interacción entre diferentes sistemas o componentes (por ejemplo, entre un Notebook y una tabla Delta).
# MAGIC
# MAGIC - **Ejemplo en Databricks:** Probar que un Notebook de transformación lee correctamente de la capa Bronze y escribe bien en la Silver, o validar que un Lakeflow Job conecta bien con sus dependencias.
# MAGIC
# MAGIC - **Características:** Son más lentas y costosas que las unitarias, pero dan más seguridad sobre el flujo del dato.
# MAGIC
# MAGIC #### **3. System Tests (La cima: Pruebas de Sistema / End-to-End)**
# MAGIC
# MAGIC Es la prueba final donde se valida todo el ecosistema como un conjunto único.
# MAGIC
# MAGIC - **Qué prueban:** La aplicación completa de principio a fin en un escenario lo más parecido posible a la realidad (producción).
# MAGIC
# MAGIC - **Ejemplo en Databricks:** Ejecutar un Workflow completo que abarca desde la ingesta con Auto Loader hasta el refresco de un dashboard en Gold, asegurando que los resultados finales sean los esperados.
# MAGIC
# MAGIC - **Características:** Son las más lentas y costosas, por lo que se ejecutan con menos frecuencia que las anteriores.

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Continuous Delivery**

# COMMAND ----------

# MAGIC %md
# MAGIC ![](https://cdn5.dcbstatic.com/files/d/a/databricks_docebosaas_com/1767834000/bnwrGmAkQGXuOxIR0Tg4lg/authoring/847/847_full_slide6_1.jpg)

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Continuous Deployment**
# MAGIC
# MAGIC ![](https://cdn5.dcbstatic.com/files/d/a/databricks_docebosaas_com/1767834000/bnwrGmAkQGXuOxIR0Tg4lg/authoring/847/847_full_slide7_1.jpg)

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Setting up your data for CI/CD**
# MAGIC
# MAGIC ![](https://cdn5.dcbstatic.com/files/d/a/databricks_docebosaas_com/1767884400/MT-CCK1bPiC9sJYbe91shQ/authoring/847/847_full_slide10_1.jpg)