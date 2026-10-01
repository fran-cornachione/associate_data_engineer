# Databricks notebook source
# MAGIC %md
# MAGIC ### **¿Qué es Unity Catalog?**
# MAGIC
# MAGIC Imagina Unity Catalog como una capa de control maestra que se sitúa por encima de todos tus workspaces de Databricks y tu almacenamiento en la nube (S3, ADLS, GCS).
# MAGIC
# MAGIC - **Antes (Hive Metastore):** Los permisos y tablas vivían dentro de un Workspace. Si tenías 3 workspaces (Dev, QA, Prod), tenías que crear los usuarios y permisos 3 veces.
# MAGIC
# MAGIC - **Ahora (Unity Catalog):** Defines los datos y los permisos una sola vez a nivel de cuenta. Múltiples workspaces pueden acceder a los mismos datos según los permisos que tú definas centralmente.
# MAGIC
# MAGIC ### **El Modelo de Objetos (La Jerarquía de 3 Niveles)**
# MAGIC
# MAGIC Para entender UC, debes entender cómo organiza los datos. Utiliza un espacio de nombres de tres niveles (`catalog`.`schema`.`table`) que reemplaza al antiguo `database`.`table`.
# MAGIC
# MAGIC - **Metastore:** El contenedor de nivel superior (generalmente uno por región/nube).
# MAGIC
# MAGIC - **Catálogo (Catalog):** El primer nivel de agrupación de datos. Ejemplo: `prod_catalog`, `rrhh_catalog`. Es la frontera más dura para el aislamiento de datos.
# MAGIC
# MAGIC - **Esquema (Schema):** (Antes llamado Base de Datos). Contiene las tablas, vistas, volúmenes y modelos. Ejemplo: `finance_schema`.
# MAGIC
# MAGIC ![](https://docs.databricks.com/aws/en/assets/images/object-model-40d730065eefed283b936a8664f1b247.png)
# MAGIC
# MAGIC **Activos (Assets):**
# MAGIC
# MAGIC - **Tablas:** Datos estructurados (Delta Tables).
# MAGIC
# MAGIC - **Vistas:** Consultas guardadas.
# MAGIC
# MAGIC - **Volúmenes:** Datos no estructurados (PDFs, imágenes, CSVs, JSONs brutos).
# MAGIC
# MAGIC - **Modelos:** Modelos de IA/ML registrados en MLflow.
# MAGIC
# MAGIC - **Funciones:** User Defined Functions (UDFs).

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Pilares del Gobierno de Datos en Unity Catalog**
# MAGIC
# MAGIC El verdadero valor de UC reside en sus capacidades de gobierno. Aquí están los detalles técnicos:
# MAGIC
# MAGIC **A. Gestión de Identidad y Acceso (IAM Unificado)**
# MAGIC
# MAGIC UC separa la identidad del workspace.
# MAGIC
# MAGIC - **Sincronización:** Se integra con tu proveedor de identidad (Azure Entra ID, Okta, AWS IAM).
# MAGIC
# MAGIC - **Service Principals:** Soporte nativo para cuentas de servicio (automatización/jobs) que no dependen de un usuario humano.
# MAGIC
# MAGIC **B. Control de Acceso (ACLs) Estándar SQL**
# MAGIC
# MAGIC Olvídate de gestionar permisos con archivos JSON complejos o políticas de IAM de la nube difíciles de mantener. UC usa SQL estándar ANSI para todo:

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Privilegios**

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW GRANTS `<email>` ON CATALOG workspace;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## **Tipos de Privilegio por Objeto**

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Metastore**
# MAGIC
# MAGIC `CREATE CATALOG`, `CREATE CLEAN ROOM`, `CREATE CONNECTION`, `CREATE EXTERNAL LOCATION`, `CREATE EXTERNAL METADATA`, `CREATE PROVIDER`, `CREATE RECIPIENT`, `CREATE SHARE`,`CREATE SERVICE CREDENTIAL`, `CREATE STORAGE CREDENTIAL`, `SET SHARE PERMISSION`, `USE MARKETPLACE ASSETS`, `USE PROVIDER`, `USE RECIPIENT`, `USE SHARE`

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Catalog**
# MAGIC
# MAGIC `ALL PRIVILEGES`, `APPLY TAG`, `BROWSE`, `CREATE SCHEMA`, `USE CATALOG`
# MAGIC
# MAGIC All users have `USE CATALOG` on the main catalog by default.
# MAGIC
# MAGIC The following privilege types apply to securable objects in a catalog. You can grant these privileges at the catalog level to apply them to current and future objects in the catalog.
# MAGIC
# MAGIC `CREATE FUNCTION`, `CREATE TABLE`, `CREATE MATERIALIZED VIEW`, `CREATE MODEL`, `CREATE VOLUME`, `EXTERNAL USE SCHEMA`, `READ VOLUME`, `REFRESH`, `WRITE VOLUME`, `EXECUTE`, `MANAGE`, `MODIFY`, `SELECT`, `USE SCHEMA`

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Schema**
# MAGIC
# MAGIC `ALL PRIVILEGES`, `APPLY TAG`, `CREATE FUNCTION`, `CREATE TABLE`, `CREATE MODEL`, `CREATE VOLUME`, `CREATE MATERIALIZED VIEW`, `MANAGE`, `EXTERNAL USE SCHEMA`, `USE SCHEMA`
# MAGIC
# MAGIC The following privilege types apply to securable objects within a schema. You can grant these privileges at the schema level to apply them to current and future objects within the schema.
# MAGIC
# MAGIC `EXECUTE`, `MODIFY`, `READ VOLUME`, `REFRESH`, `SELECT`, `WRITE VOLUME`

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Table**
# MAGIC
# MAGIC `ALL PRIVILEGES`, `APPLY TAG`, `MANAGE`, `MODIFY`, `SELECT`

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Materialized View**
# MAGIC
# MAGIC `ALL PRIVILEGES`, `APPLY TAG`, `MANAGE`, `REFRESH`, `SELECT`

# COMMAND ----------

# MAGIC %md
# MAGIC #### **View**
# MAGIC
# MAGIC `ALL PRIVILEGES`, `APPLY TAG`, `MANAGE`, `SELECT`

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Volume**
# MAGIC
# MAGIC `ALL PRIVILEGES`, `MANAGE`, `READ VOLUME`, `WRITE VOLUME`

# COMMAND ----------

# MAGIC %md
# MAGIC #### **External Location**
# MAGIC
# MAGIC `ALL PRIVILEGES`, `BROWSE`, `CREATE EXTERNAL TABLE`, `CREATE EXTERNAL VOLUME`, `CREATE FOREIGN SECURABLE`, `CREATE MANAGED STORAGE`, `EXTERNAL USE LOCATION`, `MANAGE`, `READ FILES`, `WRITE FILES`

# COMMAND ----------

# MAGIC %md
# MAGIC #### **External Metadata**
# MAGIC
# MAGIC `ALL PRIVILEGES`, `BROWSE`, `MANAGE`, `MODIFY`

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Service Credential**
# MAGIC
# MAGIC `ALL PRIVILEGES`, `ACCESS`, `CREATE CONNECTION`, `MANAGE`

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Storage Credential**
# MAGIC
# MAGIC `ALL PRIVILEGES`, `CREATE EXTERNAL LOCATION`, `CREATE EXTERNAL TABLE`, `MANAGE`, `READ FILES`, `WRITE FILES`

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Connection**
# MAGIC
# MAGIC `ALL PRIVILEGES`, `CREATE FOREIGN CATALOG`, `MANAGE`, `USE CONNECTION`

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Function**
# MAGIC
# MAGIC `ALL PRIVILEGES,` `APPLY TAG` (models only), `CREATE MODEL VERSION` (models only), `EXECUTE`, `MANAGE`

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Tipos de Privilegio**

# COMMAND ----------

# MAGIC %md
# MAGIC ### **`ALL PRIVILEGES`**
# MAGIC
# MAGIC **Tipos de objetos aplicables:**
# MAGIC
# MAGIC `CATALOG`, `EXTERNAL LOCATION`, `EXTERNAL METADATA`, `SERVICE CREDENTIAL`, `STORAGE CREDENTIAL`, `SCHEMA`, `FUNCTION` (incluyendo modelos), `PROCEDURE`, `TABLE`, `MATERIALIZED VIEW`, `VIEW`, `VOLUME`.
# MAGIC
# MAGIC Se utiliza para conceder o revocar _**todos los privilegios aplicables al objeto securizable y a sus objetos hijos**_, sin necesidad de especificarlos de manera explícita.
# MAGIC
# MAGIC Cuando se concede `ALL PRIVILEGES` sobre un objeto, no se otorgan individualmente al usuario cada uno de los privilegios aplicables en ese momento. En cambio, se expande a todos los privilegios disponibles en el momento en que se realizan las verificaciones de permisos. Esto significa que, a medida que Databricks introduce nuevos privilegios y nuevos objetos securizables, un `ALL PRIVILEGES` existente incluye automáticamente cualquier privilegio nuevo aplicable al objeto, a sus objetos hijos existentes y a cualquier nuevo objeto hijo.
# MAGIC
# MAGIC Para evitar una exfiltración accidental de datos o una escalada de privilegios, `ALL PRIVILEGES` no incluye los privilegios `EXTERNAL USE SCHEMA`, `EXTERNAL USE LOCATION` ni `MANAGE`.
# MAGIC
# MAGIC Cuando se **revoca** `ALL PRIVILEGES`, tanto la concesión de `ALL PRIVILEGES` como todos los privilegios individuales que implicaba se eliminan. Los privilegios que no forman parte de `ALL PRIVILEGES` —como `MANAGE`, `EXTERNAL USE LOCATION` y `EXTERNAL USE SCHEMA`— no se ven afectados.

# COMMAND ----------

# MAGIC %md
# MAGIC ### **CREATE CATALOG**
# MAGIC
# MAGIC **Tipos de objetos aplicables:** Unity Catalog metastore
# MAGIC
# MAGIC Permite que un usuario cree un catálogo dentro de un metastore de Unity Catalog.
# MAGIC
# MAGIC Para crear un catálogo externo (foreign catalog), también se debe tener el privilegio `CREATE FOREIGN CATALOG` sobre la conexión que contiene ese catálogo externo o sobre el propio metastore.

# COMMAND ----------

# MAGIC %md
# MAGIC ### **CREATE SCHEMA**
# MAGIC
# MAGIC **Tipos de objetos aplicables:** `CATALOG`
# MAGIC
# MAGIC Permite que un usuario cree un esquema (schema).
# MAGIC
# MAGIC - El usuario también debe tener el privilegio `USE CATALOG` sobre el catálogo en el que desea crear el esquema.

# COMMAND ----------

# MAGIC %md
# MAGIC ### **CREATE VOLUME**
# MAGIC
# MAGIC - **Tipos de objetos aplicables:** `SCHEMA`
# MAGIC
# MAGIC Permite que un usuario cree un volumen dentro de un esquema.
# MAGIC
# MAGIC Dado que los privilegios se heredan, `CREATE VOLUME` también puede otorgarse sobre un catálogo, lo que permite al usuario crear volúmenes en cualquier esquema existente o futuro dentro del catálogo.
# MAGIC
# MAGIC El usuario también debe tener el privilegio `USE CATALOG` sobre el catálogo padre y `USE SCHEMA` sobre el esquema padre.

# COMMAND ----------

# MAGIC %md
# MAGIC ### **CREATE TABLE**
# MAGIC
# MAGIC **Tipos de objetos aplicables:** `SCHEMA`
# MAGIC
# MAGIC Permite que un usuario cree una tabla o vista dentro de un esquema.
# MAGIC
# MAGIC Dado que los privilegios se heredan, `CREATE TABLE` también puede otorgarse sobre un catálogo, lo que permite al usuario crear tablas o vistas en cualquier esquema existente o futuro dentro del catálogo.
# MAGIC
# MAGIC - El usuario también debe tener el privilegio `USE CATALOG` sobre el catálogo padre y `USE SCHEMA` sobre el esquema padre.

# COMMAND ----------

# MAGIC %md
# MAGIC ### **CREATE CONNECTION**
# MAGIC
# MAGIC **Applicable object types:** Unity Catalog metastore, `SERVICE CREDENTIAL`
# MAGIC
# MAGIC Permite que un usuario cree una conexión a una base de datos externa en un escenario de Lakehouse Federation.
# MAGIC
# MAGIC Para usar una credencial de servicio al momento de crear la conexión, el usuario debe tener este privilegio tanto en el metastore como en la propia credencial de servicio.

# COMMAND ----------

# MAGIC %md
# MAGIC ### **ACCESS**
# MAGIC
# MAGIC **Tipos de objetos aplicables:** `SERVICE CREDENTIAL`
# MAGIC
# MAGIC Permite que un usuario utilice una credencial de servicio para acceder a un servicio externo o a varios servicios externos.

# COMMAND ----------

# MAGIC %md
# MAGIC ### **APPLY TAG**
# MAGIC
# MAGIC **Tipos de objetos aplicables:** `CATALOG`, `SCHEMA`, `TABLE`, `VOLUME`, `MATERIALIZED VIEW`, `VIEW`, y modelos registrados como `FUNCTION`.
# MAGIC
# MAGIC Permite que un usuario agregue y edite tags en un objeto. Conceder `APPLY TAG` sobre una tabla o vista también habilita el tagging de columnas. Conceder `APPLY TAG` a un modelo registrado también habilita el tagging de versiones del modelo.
# MAGIC
# MAGIC El usuario también debe tener el privilegio `USE CATALOG` en el catálogo padre y `USE SCHEMA` en el esquema padre.
# MAGIC
# MAGIC Para aplicar un governed tag a objetos securizables de Unity Catalog, también es necesario contar con el permiso `ASSIGN` sobre dicho governed tag.

# COMMAND ----------

# MAGIC %md
# MAGIC ### **CREATE EXTERNAL LOCATION**
# MAGIC
# MAGIC - **Tipos de objetos aplicables:** Unity Catalog METASTORE, `STORAGE CREDENTIAL`
# MAGIC
# MAGIC Para crear una ubicación externa (external location), el usuario debe tener este privilegio tanto en el metastore como en la credencial de almacenamiento (storage credential) que se referencia en dicha ubicación externa.

# COMMAND ----------

# MAGIC %md
# MAGIC ## **`GRANT`**
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Show Privileges - Sintax
# MAGIC %sql
# MAGIC SHOW GRANTS [ principal ] ON securable_object;

# COMMAND ----------

# DBTITLE 1,Grant Privileges - Sintax
# MAGIC %sql
# MAGIC GRANT privilege(s) ON securable_object TO principal;

# COMMAND ----------

# DBTITLE 1,Grant Privileges  - Example
# MAGIC %sql
# MAGIC -- Dar permiso de lectura a un grupo
# MAGIC GRANT SELECT ON TABLE catalogo.esquema.tabla TO `grupo_analistas`;
# MAGIC
# MAGIC -- Permitir crear tablas en un esquema
# MAGIC GRANT CREATE TABLE ON SCHEMA catalogo.esquema TO `data_engineers`;

# COMMAND ----------

# MAGIC %md
# MAGIC ## **`REVOKE`**

# COMMAND ----------

# DBTITLE 1,Revoke Privileges - Sintax
# MAGIC %sql
# MAGIC REVOKE privilege_types ON securable_object FROM principal;

# COMMAND ----------

# DBTITLE 1,Revoke Privileges - Example
# MAGIC %sql
# MAGIC REVOKE SELECT ON TABLE t FROM aliens;

# COMMAND ----------

# MAGIC %md
# MAGIC ## **GROPUS**

# COMMAND ----------

# MAGIC %md
# MAGIC ### **CREATE GROUP**
# MAGIC
# MAGIC Crea un grupo local del área de trabajo con el nombre especificado, **opcionalmente incluyendo una lista de usuarios y grupos**. Los grupos locales del área de trabajo no se sincronizan con la cuenta de Databricks y no son compatibles con Unity Catalog. Para obtener más información, consulta Administrar grupos locales del área de trabajo (heredado).
# MAGIC
# MAGIC - Para ejecutar este comando debes ser **administrador**.

# COMMAND ----------

# DBTITLE 1,Create Group - Sintax
# MAGIC %sql
# MAGIC -- Create an empty group.
# MAGIC CREATE GROUP humans;
# MAGIC
# MAGIC -- Create tv_aliens with Alf and Thor as members.
# MAGIC CREATE GROUP tv_aliens WITH USER `alf@melmak.et`, `thor@asgaard.et`;
# MAGIC
# MAGIC -- Create aliens with Hilo and tv_aliens as members.
# MAGIC CREATE GROUP aliens WITH USER `hilo@jannus.et` GROUP tv_aliens;

# COMMAND ----------

# MAGIC %md
# MAGIC ## **DROP GROUP**
# MAGIC
# MAGIC Elimina un grupo local del área de trabajo. Se lanzará una excepción si el grupo no existe en el sistema.
# MAGIC
# MAGIC Los grupos locales del área de trabajo no se sincronizan con la cuenta de Databricks y no son compatibles con Unity Catalog. Para obtener más información, consulta Administrar grupos locales del área de trabajo (heredado).
# MAGIC
# MAGIC - Para ejecutar este comando debes ser **administrador**.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP GROUP group_principal;

# COMMAND ----------

# MAGIC %md
# MAGIC # **Gestión del Almacenamiento: External vs Managed**
# MAGIC
# MAGIC Unity Catalog cambia cómo Databricks interactúa con tu almacenamiento en la nube (S3/ADLS).
# MAGIC
# MAGIC **Credenciales de Almacenamiento y Ubicaciones Externas**
# MAGIC
# MAGIC Ya no se deben hardcodear claves de acceso (Access Keys) en los notebooks.
# MAGIC
# MAGIC - **Storage Credential:** Un objeto seguro en UC que encapsula un rol de IAM (AWS) o Service Principal (Azure).
# MAGIC
# MAGIC - **External Location:** Una ruta de almacenamiento (ej. `s3://my-bucket/finance/`) autorizada para usar una Credencial específica.
# MAGIC
# MAGIC **Tipos de Tablas**
# MAGIC
# MAGIC - **Managed Tables (Tablas Gestionadas):** Databricks gestiona tanto los metadatos como los archivos físicos en el almacenamiento raíz del metastore. Si borras la tabla, se borran los datos. Recomendado para la mayoría de los casos.
# MAGIC
# MAGIC - **External Tables (Tablas Externas):** Databricks gestiona los metadatos, pero tú controlas la ubicación física de los archivos. Si borras la tabla en UC, los archivos en S3/ADLS permanecen. Útil si otras herramientas fuera de Databricks necesitan leer esos archivos directamente.
# MAGIC
# MAGIC ![](https://cdn.prod.website-files.com/66954b344e907bd91f1c8027/6835e83fff2e3b864fa1941a_AD_4nXfDKyo3WXqPNPYDnNkIb_Br1SR8ueQoQWA5JvZzP5GHlB_zeNGEdUQNjpdJqBb447OzLQgeygdcKR4ygZVA_1-heWgDOq447fioCXnMTK62rXLM4szHwDGyJvvPxYWjFiXp_bStzw.png)
# MAGIC
# MAGIC | Característica | **Tabla Managed** (Gestionada) | **Tabla External** (Externa) |
# MAGIC | :--- | :--- | :--- |
# MAGIC | **Control** | Databricks controla tanto los datos como los metadatos | Databricks controla solo los metadatos |
# MAGIC | **Ubicación de datos** | Ubicación predeterminada gestionada por Databricks | Ubicación de almacenamiento especificada por el usuario (`LOCATION`) |
# MAGIC | **Eliminación** | `DROP TABLE` elimina **datos y metadatos** | `DROP TABLE` elimina solo los **metadatos**; los datos persisten |
# MAGIC | **Creación** | Opción predeterminada (`CREATE TABLE ...`) | Requiere la palabra clave `EXTERNAL` y `LOCATION` |

# COMMAND ----------

# DBTITLE 1,Crear External Table
# MAGIC %sql
# MAGIC CREATE EXTERNAL TABLE 
# MAGIC   mi_tabla_external 
# MAGIC (
# MAGIC id INT, 
# MAGIC nombre STRING
# MAGIC ) 
# MAGIC LOCATION 
# MAGIC   's3://mi-bucket/datos/mi_tabla_external'