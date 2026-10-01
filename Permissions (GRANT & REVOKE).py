# Databricks notebook source
# MAGIC %md
# MAGIC # **Mostrar privilegios**

# COMMAND ----------

# MAGIC %md
# MAGIC **Sintaxis**

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW GRANTS [ principal ] ON securable_object

# COMMAND ----------

# MAGIC %md
# MAGIC **Ejemplo**

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW GRANTS `cornachofrance@gmail.com` ON CATALOG workspace

# COMMAND ----------

# MAGIC %md
# MAGIC # **Tipos de Privilegio por Objeto en Unity Catalog**

# COMMAND ----------

# MAGIC %md
# MAGIC [_Fuente_](https://docs.databricks.com/aws/en/data-governance/unity-catalog/manage-privileges/privileges)

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Metastore**
# MAGIC
# MAGIC `CREATE CATALOG`, `CREATE CLEAN ROOM`, `CREATE CONNECTION`, `CREATE EXTERNAL LOCATION`, `CREATE EXTERNAL METADATA`, `CREATE PROVIDER`, `CREATE RECIPIENT`, `CREATE SHARE`,`CREATE SERVICE CREDENTIAL`, `CREATE STORAGE CREDENTIAL`, `SET SHARE PERMISSION`, `USE MARKETPLACE ASSETS`, `USE PROVIDER`, `USE RECIPIENT`, `USE SHARE`

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Catalog**
# MAGIC
# MAGIC 	
# MAGIC `ALL PRIVILEGES`, `APPLY TAG`, `BROWSE`, `CREATE SCHEMA`, `USE CATALOG`
# MAGIC
# MAGIC All users have `USE CATALOG` on the `main` catalog by default.
# MAGIC
# MAGIC The following privilege types apply to securable objects in a catalog. You can grant these privileges at the catalog level to apply them to current and future objects in the catalog.
# MAGIC
# MAGIC `CREATE FUNCTION`, `CREATE TABLE`, `CREATE MATERIALIZED VIEW`, `CREATE MODEL`, `CREATE VOLUME`, `EXTERNAL USE SCHEMA`, `READ VOLUME`, `REFRESH`, `WRITE VOLUME`, `EXECUTE`, `MANAGE`, `MODIFY`, `SELECT`, `USE SCHEMA`

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Schema**
# MAGIC
# MAGIC `ALL PRIVILEGES`, `APPLY TAG`, `CREATE FUNCTION`, `CREATE TABLE`, `CREATE MODEL`, `CREATE VOLUME`, `CREATE MATERIALIZED VIEW`, `MANAGE`, `EXTERNAL USE SCHEMA`, `USE SCHEMA`
# MAGIC
# MAGIC The following privilege types apply to securable objects within a schema. You can grant these privileges at the schema level to apply them to current and future objects within the schema.
# MAGIC
# MAGIC `EXECUTE`, `MODIFY`, `READ VOLUME`, `REFRESH`, `SELECT`, `WRITE VOLUME`

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Table**
# MAGIC
# MAGIC `ALL PRIVILEGES`, `APPLY TAG`, `MANAGE`, `MODIFY`, `SELECT`

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Materialized View**
# MAGIC
# MAGIC `ALL PRIVILEGES`, `APPLY TAG`, `MANAGE`, `REFRESH`, `SELECT`

# COMMAND ----------

# MAGIC %md
# MAGIC ### **View**
# MAGIC
# MAGIC `ALL PRIVILEGES`, `APPLY TAG`, `MANAGE`, `SELECT`

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Volume**
# MAGIC
# MAGIC `ALL PRIVILEGES`, `MANAGE`, `READ VOLUME`, `WRITE VOLUME`

# COMMAND ----------

# MAGIC %md
# MAGIC ### **External Location**
# MAGIC
# MAGIC `ALL PRIVILEGES`, `BROWSE`, `CREATE EXTERNAL TABLE`, `CREATE EXTERNAL VOLUME`, `CREATE FOREIGN SECURABLE`, `CREATE MANAGED STORAGE`, `EXTERNAL USE LOCATION`, `MANAGE`, `READ FILES`, `WRITE FILES`

# COMMAND ----------

# MAGIC %md
# MAGIC ### **External Metadata**
# MAGIC
# MAGIC `ALL PRIVILEGES`, `BROWSE`, `MANAGE`, `MODIFY`

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Service Credential**
# MAGIC
# MAGIC `ALL PRIVILEGES`, `ACCESS`, `CREATE CONNECTION`, `MANAGE`

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Storage Credential**
# MAGIC
# MAGIC `ALL PRIVILEGES`, `CREATE EXTERNAL LOCATION`, `CREATE EXTERNAL TABLE`, `MANAGE`, `READ FILES`, `WRITE FILES`

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Connection**
# MAGIC
# MAGIC `ALL PRIVILEGES`, `CREATE FOREIGN CATALOG`, `MANAGE`, `USE CONNECTION`

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Function**
# MAGIC 	
# MAGIC `ALL PRIVILEGES`, `APPLY TAG (models only)`, `CREATE MODEL VERSION (models only)`, `EXECUTE`, `MANAGE`

# COMMAND ----------

# MAGIC %md
# MAGIC # **Tipos de Privilegio**

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
# MAGIC **Tipos de objetos aplicables:** `SCHEMA`
# MAGIC
# MAGIC Permite que un usuario cree un volumen dentro de un esquema.
# MAGIC
# MAGIC Dado que los privilegios se heredan, `CREATE VOLUME` también puede otorgarse sobre un catálogo, lo que permite al usuario **crear volúmenes en cualquier esquema existente o futuro _dentro del catálogo_.**
# MAGIC
# MAGIC - El usuario también debe tener el privilegio `USE CATALOG` sobre el catálogo padre y `USE SCHEMA` sobre el esquema padre.

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
# MAGIC **Tipos de objetos aplicables:** Unity Catalog metastore, `STORAGE CREDENTIAL`
# MAGIC
# MAGIC Para crear una ubicación externa (external location), el usuario debe tener este privilegio tanto en el metastore como en la credencial de almacenamiento (storage credential) que se referencia en dicha ubicación externa.

# COMMAND ----------

# MAGIC %md
# MAGIC # **Otorgar privilegios: `GRANT` `REVOKE`**

# COMMAND ----------

# MAGIC %md
# MAGIC ## **GRANT**

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Sintaxis**

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Sintaxis
# MAGIC GRANT privilege_types ON securable_object TO principal
# MAGIC
# MAGIC privilege_types
# MAGIC   { ALL PRIVILEGES |
# MAGIC     privilege_type [, ...] }

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Parámetros**
# MAGIC
# MAGIC - **privilege type**
# MAGIC
# MAGIC Un privilegio específico que se otorgará sobre el securable_object al principal.
# MAGIC
# MAGIC - **securable_object**
# MAGIC
# MAGIC El objeto sobre el cual se otorgan los privilegios al principal.
# MAGIC
# MAGIC - **principal**
# MAGIC
# MAGIC Un usuario, entidad de servicio o grupo al que se le otorgan los privilegios. Debes encerrar los nombres de usuarios, entidades de servicio y grupos que tengan caracteres especiales entre comillas invertidas (` `).

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Catalog | Metastore**

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT USE CATALOG ON CATALOG catalog_name TO group_name;
# MAGIC GRANT CREATE CATALOG ON METASTORE TO group_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Schema**

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT USE SCHEMA ON SCHEMA catalog_name.schema_name TO group_name;
# MAGIC GRANT CREATE TABLE ON SCHEMA catalog_name.schema_name TO group_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Table | View** 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- table/view
# MAGIC GRANT SELECT ON TABLE catalog_name.schema_name.table_name TO principal;
# MAGIC GRANT MODIFY ON TABLE catalog_name.schema_name.table_name TO principal;

# COMMAND ----------

# MAGIC %md
# MAGIC ## **REVOKE**
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Sintaxis**

# COMMAND ----------

# MAGIC %sql
# MAGIC REVOKE privilege_types ON securable_object FROM principal
# MAGIC
# MAGIC privilege_types
# MAGIC   { ALL PRIVILEGES |
# MAGIC     privilege_type [, ...] }

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Parámetros**
# MAGIC
# MAGIC - **privilege_types**
# MAGIC
# MAGIC Identifica uno o más privilegios que serán revocados del principal.
# MAGIC
# MAGIC - **ALL PRIVILEGES**
# MAGIC
# MAGIC Revoca todos los privilegios aplicables al securable_object. En Unity Catalog, cuando se revoca ALL PRIVILEGES, se revocan tanto la concesión ALL PRIVILEGES como cualquier privilegio individual implícito en ella. Los privilegios que no forman parte de ALL PRIVILEGES, como MANAGE, EXTERNAL USE LOCATION y EXTERNAL USE SCHEMA, no se ven afectados.
# MAGIC
# MAGIC - **privilege_type**
# MAGIC
# MAGIC El privilegio específico que será revocado en el securable_object del principal.
# MAGIC
# MAGIC - **securable_object**
# MAGIC
# MAGIC El objeto sobre el cual los privilegios están concedidos al principal.
# MAGIC
# MAGIC - **principal**
# MAGIC
# MAGIC Un usuario, entidad de servicio o grupo del cual se revocan los privilegios. Debes encerrar los nombres de usuarios, entidades de servicio y grupos que contengan caracteres especiales entre comillas invertidas (` `).

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Ejemplos**

# COMMAND ----------

# MAGIC %sql
# MAGIC REVOKE ALL PRIVILEGES ON SCHEMA default FROM `alf@melmak.et`;
# MAGIC
# MAGIC REVOKE SELECT ON TABLE t FROM aliens;

# COMMAND ----------

# MAGIC %md
# MAGIC # **GROUPS**

# COMMAND ----------

# MAGIC %md
# MAGIC ## **CREATE GROUP**

# COMMAND ----------

# MAGIC %md
# MAGIC Crea un grupo local del área de trabajo con el nombre especificado, **opcionalmente incluyendo una lista de usuarios y grupos**. Los grupos locales del área de trabajo no se sincronizan con la cuenta de Databricks y no son compatibles con Unity Catalog. Para obtener más información, consulta Administrar grupos locales del área de trabajo (heredado).
# MAGIC
# MAGIC - Para ejecutar este comando debes ser **administrador**.

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Sintaxis**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE GROUP group_principal
# MAGIC   [ WITH
# MAGIC     [ USER user_principal [, ...] ]
# MAGIC     [ GROUP subgroup_principal [, ...] ]
# MAGIC   ]

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Parámetros**

# COMMAND ----------

# MAGIC %md
# MAGIC - **group_principal**
# MAGIC
# MAGIC El nombre del grupo local del área de trabajo que se va a crear.
# MAGIC
# MAGIC - **user_principal**
# MAGIC
# MAGIC Un usuario para incluir como miembro del grupo.
# MAGIC
# MAGIC - **subgroup_principal**
# MAGIC
# MAGIC Un subgrupo local del área de trabajo para incluir como miembro del grupo.

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Ejemplos**

# COMMAND ----------

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

# COMMAND ----------

# MAGIC %md
# MAGIC Elimina un grupo local del área de trabajo. Se lanzará una excepción si el grupo no existe en el sistema.
# MAGIC
# MAGIC Los grupos locales del área de trabajo no se sincronizan con la cuenta de Databricks y no son compatibles con Unity Catalog. Para obtener más información, consulta Administrar grupos locales del área de trabajo (heredado).
# MAGIC
# MAGIC - Para ejecutar este comando debes ser **administrador**.

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Sintaxis**

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP GROUP group_principal

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Parámetros**

# COMMAND ----------

# MAGIC %md
# MAGIC - **group_principal**
# MAGIC
# MAGIC El nombre del grupo local existente a eliminar.

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Ejemplos**

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create `aliens` Group
# MAGIC CREATE GROUP aliens WITH GROUP tv_aliens;
# MAGIC
# MAGIC -- Drop `aliens` group
# MAGIC DROP GROUP aliens;