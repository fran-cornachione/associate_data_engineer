# Databricks notebook source
# MAGIC %md
# MAGIC ### **Layers of Access Control**
# MAGIC
# MAGIC Access control in Unity Catalog is built on the following complementary models:
# MAGIC
# MAGIC - **Workspace-level restrictions** control where users can access data, by limiting objects to specific workspaces.
# MAGIC
# MAGIC - **Privileges and ownership control** who can access what, using grants on securable objects.
# MAGIC
# MAGIC - **Attribute-based policies (ABAC)** control what data users can access, using governed tags and centralized policies.
# MAGIC
# MAGIC - **Table-level filtering and masking** control what data users can see within tables using table-specific filters and views.
# MAGIC
# MAGIC These models work together to enforce secure, fine-grained access across your data environment.
# MAGIC
# MAGIC | Layer                          | Purpose                                                                                    | Mechanisms                                      |
# MAGIC |--------------------------------|--------------------------------------------------------------------------------------------|-------------------------------------------------|
# MAGIC | Workspace-level restrictions   | Limit which workspaces can access specific catalogs, external locations, and storage credentials | Workspace-level bindings                         |
# MAGIC | Privileges and ownership       | Control access to catalogs, schemas, tables, and other objects                             | Privilege grants to users and groups, object ownership |
# MAGIC | Attribute-based policies       | Use tags and policies to dynamically apply filters and masks                               | ABAC policies and governed tags                  |
# MAGIC | Table-level filtering and masking | Control what data users can see within tables                                           | Row filters, column masks, dynamic views        |

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Privileges and ownership**
# MAGIC
# MAGIC Access in Unity Catalog is primarily governed by privileges and object ownership. This model defines who can access or manage data and metadata by assigning admin roles and by granting privileges and managing ownership across securable objects. This section describes how privileges are granted, how ownership works, and which admin roles can manage access across different scopes.
# MAGIC
# MAGIC #### **Admin roles**
# MAGIC
# MAGIC Unity Catalog supports multiple admin roles:
# MAGIC
# MAGIC - **Account admin:** Can create metastores, manage identities, assign metastore admins, and manage account-level features like Delta Sharing and system tables.
# MAGIC
# MAGIC - **Metastore admin:** An optional but powerful role that can manage all objects in the metastore, transfer ownership, and assign top-level privileges like `CREATE CATALOG`, `CREATE EXTERNAL LOCATION`, and more.
# MAGIC
# MAGIC - **Workspace admin:** Manages identities, workspace-level settings, and the workspace catalog.
# MAGIC For details, see [Admin privileges in Unity Catalog.](https://docs.databricks.com/aws/en/data-governance/unity-catalog/manage-privileges/admin-privileges)
# MAGIC
# MAGIC ### **Object ownership**
# MAGIC
# MAGIC Every securable object, such as a catalog, schema, or table, in Unity Catalog has an owner. Ownership grants full control over that object, including the ability to:
# MAGIC
# MAGIC - Read or modify the object and its metadata
# MAGIC
# MAGIC - Grant privileges to other users
# MAGIC
# MAGIC - Transfer ownership to another principal
# MAGIC
# MAGIC Unity Catalog also supports a `MANAGE` privilege, which allows users to grant access and modify objects without making them the owner.