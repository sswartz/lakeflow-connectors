# Databricks notebook source
# MAGIC %md
# MAGIC # Maxio (SaaSOptics) — Test ingestion into Databricks
# MAGIC
# MAGIC End-to-end smoke test of the Maxio Lakeflow community connector **without** a
# MAGIC pipeline or a UC connection. It loads the self-contained connector source,
# MAGIC registers the `lakeflow_connect` Spark data source, reads a Maxio table over
# MAGIC the live API, and (optionally) writes the result to a Delta table.
# MAGIC
# MAGIC **Why a single source file, not a wheel:** the repo's wheel is framework-only
# MAGIC — it deliberately excludes `...sources.*`. The deployable artifact for a
# MAGIC source is its merged `_generated_<source>_python_source.py`, which is fully
# MAGIC self-contained (only pyspark / stdlib / requests) and exposes
# MAGIC `register_lakeflow_source(spark)`. It defines every data-source class as a
# MAGIC nested closure, so Spark cloudpickles it to executors — nothing needs to be
# MAGIC importable on the workers.
# MAGIC
# MAGIC **Prerequisites**
# MAGIC - `load_maxio_secrets` has been run (scope holds `server_subdomain`,
# MAGIC   `account_name`, `api_key`). (Already done.)
# MAGIC - Cluster / serverless has internet egress to `*.saasoptics.com`.
# MAGIC - `maxio_source.py` (patched for Spark 4.1) sits next to this notebook.
# MAGIC
# MAGIC > Batch-only: does **not** exercise CDC watermarking or SCD merge — those run
# MAGIC > under the SDP pipeline on the PREVIEW channel. Each `.load()` is a full read.

# COMMAND ----------
# MAGIC %md ## 1. Inputs

# COMMAND ----------

dbutils.widgets.text("scope_name", "acuity-maxio-dev", "Secret scope name")
dbutils.widgets.text("table_name", "customers", "Maxio table to read")
dbutils.widgets.text("max_records_per_batch", "500", "max_records_per_batch (caps read size)")
dbutils.widgets.text("write_table", "", "Optional: catalog.schema.table to write results to")
dbutils.widgets.text(
    "source_dir",
    "/Workspace/Users/szs69@acuitysso.com/maxio",
    "Dir containing maxio_source.py",
)

scope_name             = dbutils.widgets.get("scope_name").strip()
table_name             = dbutils.widgets.get("table_name").strip()
max_records_per_batch  = dbutils.widgets.get("max_records_per_batch").strip()
write_table            = dbutils.widgets.get("write_table").strip()
source_dir             = dbutils.widgets.get("source_dir").strip()

# COMMAND ----------
# MAGIC %md ## 2. Load the connector source and register it
# MAGIC Put the source dir on `sys.path`, import the self-contained module, and call
# MAGIC its `register_lakeflow_source(spark)` — this registers the `lakeflow_connect`
# MAGIC format. No wheel, no package install.

# COMMAND ----------

import sys

if source_dir not in sys.path:
    sys.path.insert(0, source_dir)

import maxio_source  # the uploaded _generated_maxio_python_source.py, renamed

maxio_source.register_lakeflow_source(spark)
print("Registered 'lakeflow_connect' from maxio_source.py")

# COMMAND ----------
# MAGIC %md ## 3. Pull credentials from the secret scope
# MAGIC Read the three secrets — never printed.

# COMMAND ----------

creds = {
    "server_subdomain": dbutils.secrets.get(scope_name, "server_subdomain"),
    "account_name":     dbutils.secrets.get(scope_name, "account_name"),
    "api_key":          dbutils.secrets.get(scope_name, "api_key"),
}
print(f"Credentials loaded from scope '{scope_name}'.")

# COMMAND ----------
# MAGIC %md ## 4. Read the table
# MAGIC Start with a small table (`customers` or `items`) to validate auth and schema
# MAGIC before the high-volume tables (`revenue_entries`).

# COMMAND ----------

reader = (
    spark.read.format("lakeflow_connect")
    .option("tableName", table_name)
    .options(**creds)
)
if max_records_per_batch:
    reader = reader.option("max_records_per_batch", max_records_per_batch)

df = reader.load()

df.printSchema()

# COMMAND ----------

display(df.limit(20))

# COMMAND ----------

print(f"Row count for '{table_name}': {df.count()}")

# COMMAND ----------
# MAGIC %md ## 5. (Optional) Write to Delta
# MAGIC Set the `write_table` widget to a `catalog.schema.table` you can write to.

# COMMAND ----------

if write_table:
    (df.write.mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(write_table))
    print(f"Wrote {df.count()} rows to {write_table}")
else:
    print("No write_table provided — skipping Delta write.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Tables to try
# MAGIC | table | notes |
# MAGIC |---|---|
# MAGIC | `customers` | small — validate auth/schema first |
# MAGIC | `items` | small |
# MAGIC | `invoices` | header-only (nested `line_items` dropped) |
# MAGIC | `invoices_line_items` | exploded invoice lines |
# MAGIC | `contracts` | customer contracts |
# MAGIC | `transactions` | per-contract transactions |
# MAGIC | `revenue_entries` | highest volume — keep `max_records_per_batch` small for a quick test |
