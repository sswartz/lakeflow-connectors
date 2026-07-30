# Databricks notebook source
# MAGIC %md
# MAGIC # Maxio (SaaSOptics) — Load credentials into a secret scope
# MAGIC
# MAGIC One-time setup. Stores three secrets the Lakeflow community connector for Maxio
# MAGIC needs:
# MAGIC
# MAGIC - `server_subdomain` — e.g. `s12`
# MAGIC - `account_name`     — e.g. `qbdv10_lucid`
# MAGIC - `api_key`          — SaaSOptics API token (Admin → API Tokens)
# MAGIC
# MAGIC Fill in the widgets, then **Run all**. The notebook prints only the secret
# MAGIC *keys* it wrote — never the values.

# COMMAND ----------
# MAGIC %md ## 1. Inputs

# COMMAND ----------

dbutils.widgets.text("scope_name", "acuity-maxio-dev", "Secret scope name")
dbutils.widgets.text("server_subdomain", "", "server_subdomain (e.g. s12)")
dbutils.widgets.text("account_name", "", "account_name (e.g. qbdv10_lucid)")
dbutils.widgets.text("api_key", "", "api_key (SaaSOptics API token)")
dbutils.widgets.text("reader_principal", "", "Optional: user/group email to grant READ")

scope_name        = dbutils.widgets.get("scope_name").strip()
server_subdomain  = dbutils.widgets.get("server_subdomain").strip()
account_name      = dbutils.widgets.get("account_name").strip()
api_key           = dbutils.widgets.get("api_key").strip()
reader_principal  = dbutils.widgets.get("reader_principal").strip()

missing = [n for n, v in [
    ("scope_name", scope_name),
    ("server_subdomain", server_subdomain),
    ("account_name", account_name),
    ("api_key", api_key),
] if not v]
if missing:
    raise ValueError(f"Missing required widget values: {missing}")

print(f"Target scope: {scope_name}")
print(f"Will write keys: server_subdomain, account_name, api_key")
if reader_principal:
    print(f"Will grant READ to: {reader_principal}")

# COMMAND ----------
# MAGIC %md ## 2. Create scope (idempotent)

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import ResourceAlreadyExists
from databricks.sdk.service.workspace import AclPermission

w = WorkspaceClient()

existing_scopes = {s.name for s in w.secrets.list_scopes()}
if scope_name in existing_scopes:
    print(f"Scope '{scope_name}' already exists — reusing.")
else:
    try:
        w.secrets.create_scope(scope=scope_name)
        print(f"Created scope '{scope_name}'.")
    except ResourceAlreadyExists:
        print(f"Scope '{scope_name}' already exists — reusing.")

# COMMAND ----------
# MAGIC %md ## 3. Write the three secrets

# COMMAND ----------

for key, value in [
    ("server_subdomain", server_subdomain),
    ("account_name",     account_name),
    ("api_key",          api_key),
]:
    w.secrets.put_secret(scope=scope_name, key=key, string_value=value)
    print(f"  wrote {scope_name}/{key}")

# COMMAND ----------
# MAGIC %md ## 4. (Optional) Grant READ to a reader principal

# COMMAND ----------

if reader_principal:
    w.secrets.put_acl(
        scope=scope_name,
        principal=reader_principal,
        permission=AclPermission.READ,
    )
    print(f"Granted READ on '{scope_name}' to {reader_principal}.")
else:
    print("No reader_principal provided — skipping ACL grant.")

# COMMAND ----------
# MAGIC %md ## 5. Verify (keys only — never values)

# COMMAND ----------

keys = sorted(s.key for s in w.secrets.list_secrets(scope=scope_name))
print(f"Keys in scope '{scope_name}':")
for k in keys:
    print(f"  - {k}")

acls = w.secrets.list_acls(scope=scope_name)
print(f"\nACLs on '{scope_name}':")
for a in acls:
    print(f"  - {a.principal}: {a.permission.value}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Done
# MAGIC
# MAGIC Share with the connector developer:
# MAGIC - **Workspace URL**
# MAGIC - **Scope name** (above)
# MAGIC - The three key names: `server_subdomain`, `account_name`, `api_key`
