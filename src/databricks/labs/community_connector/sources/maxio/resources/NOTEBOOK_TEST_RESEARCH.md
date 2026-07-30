# Maxio connector — notebook testing on abl-sandbox: what we did & the best-practice path

**Date:** 2026-07-30
**Author:** agent session (Stuart)
**Workspace:** abl-sandbox (`szs69@acuitysso.com`)
**Branch:** `feat/connector-maxio`

---

## TL;DR

We tried to smoke-test the Maxio Lakeflow community connector by reading data into
Databricks from a **notebook** on abl-sandbox, deliberately avoiding the full
SDP pipeline + UC connection. We hit a chain of environment mismatches — the
crux is that **the connector requires PySpark 4.2's `pyspark.sql.streaming.datasource`
module, which does not exist on interactive Serverless v5 (Spark 4.1)**. That
module only ships on the **SDP pipeline runtime (PREVIEW channel)**, which is the
environment the framework is actually designed for.

Everything we did to work around it (wheel builds, `--no-deps`, a Spark-4.1 import
shim, switching from the wheel to the self-contained generated source file) was a
series of hacks to force a batch read onto an unsupported runtime. They can be made
to work for a **driver-side batch peek**, but none of it is the intended path.

**Best practice, with admin buy-in: deploy the real pipeline via `community-connector
create_pipeline` (PREVIEW channel, serverless) with a UC connection whose credential
values are secret-scope references.** That is the only environment where the connector
loads unmodified and where CDC/SCD behavior is actually exercised.

---

## What we were trying to do

Goal: prove Maxio → Databricks ingestion works end-to-end, from a notebook, without
standing up the pipeline or a UC connection — a quick credibility check before the
heavier deployment.

Constraints in play:
- The `databricks-ai-dev-kit` MCP (our standard for Databricks ops) was **not connected**
  this session; only `databricks-v2` was. Per standing rules we escalated and got a
  one-time OK to use the **Databricks CLI** with `--profile abl-sandbox`.
- Credentials were loaded into a **secret scope** (`acuity-maxio-dev`) via the shipped
  `load_maxio_secrets` notebook — not a UC connection.

## What we built and uploaded to `/Users/szs69@acuitysso.com/maxio/`

| Object | Role | Status |
|---|---|---|
| `load_maxio_secrets` | Credential loader → secret scope `acuity-maxio-dev` | Run ✅ |
| `lakeflow_community_connectors-0.1.0-py3-none-any.whl` | Framework-only wheel (built from branch) | **Dead end** — see below |
| `maxio_source.py` | Self-contained generated connector source (Spark-4.1 shimmed) | Uploaded |
| `test_maxio_ingest` | Test notebook: load source → register → read → optional Delta write | Uploaded |

---

## The failure chain (what we learned, in order)

### 1. CLI/env bootstrapping
- No `pip`/`python` on PATH; only `python3` and `uv`. Installed the
  `community-connector` CLI via `uv tool install -e .`.
- `databricks-ai-dev-kit` MCP absent → escalated → one-time CLI override approved.

### 2. `%pip install <wheel>` → dependency conflict
```
lakeflow-community-connectors 0.1.0 depends on pyspark>=4.2.0.dev0
The user requested (constraint) pyspark==4.1.0
```
**Cause:** the wheel pins `pyspark>=4.2.0.dev0` (repo intentionally tracks pyspark
prereleases); the DBR/serverless runtime pins `pyspark==4.1.0`. **Workaround:** `--no-deps`.
This *installs*, but silences the very warning that mattered.

### 3. `register(...)` → `ModuleNotFoundError: No module named 'pyspark.sql.streaming.datasource'`
**Root cause (the real one).** The connector imports, at module-load time:
```python
from pyspark.sql.streaming.datasource import ReadAllAvailable, SupportsTriggerAvailableNow
```
That module ships in **pyspark 4.2.0.dev0+**. Interactive **Serverless v5 = Spark 4.1**,
which lacks it. The import dies before any read runs. This requirement is **not
maxio-specific** — every connector in the repo shares it.

Where does 4.2 come from? **Not** interactive serverless. It's:
- the repo's local dev/test env (`uv.lock` resolves `pyspark==4.2.0.dev5`), and
- the **SDP pipeline runtime on the PREVIEW channel** — the CLI's `default_config.yaml`
  deploys pipelines with `channel: PREVIEW`, `serverless: true`. That runtime carries
  the newer Spark. **This is why the framework requires a pipeline, not a notebook.**

The two symbols are used **only** by the streaming reader classes
(`LakeflowStreamReader`, `LakeflowPartitionedStreamReader`). The **batch**
`DataSourceReader` path (`spark.read.format("lakeflow_connect").load()`) never touches
them — the import just fails eagerly at load time.

### 4. Shim to unblock batch on Spark 4.1
Wrapped both import sites (`sparkpds/lakeflow_datasource.py` and the generated maxio
source) in `try/except ModuleNotFoundError` with inert stand-in classes, baked into the
built artifact. Repo tree restored to clean (`git restore`) — the patch lives only in
the uploaded artifact, **not committed**.

### 5. `register("maxio")` → `Source 'maxio' not found`
**Cause:** the wheel **deliberately excludes the sources**:
```
# pyproject.toml
[tool.setuptools.packages.find]
exclude = ["databricks.labs.community_connector.sources*"]
```
The wheel is **framework-only by design**; sources ship via Git checkout / the merge
script, not the wheel. So `register("maxio")` had no maxio package to find.

### 6. Pivot to the self-contained source file
The merge script produces `_generated_maxio_python_source.py` — fully self-contained
(only pyspark / stdlib / requests), exposing `register_lakeflow_source(spark)`, with
every data-source class defined as a **nested closure** (so Spark cloudpickles it to
executors — nothing needs to be importable on workers). We uploaded a shimmed copy as
`maxio_source.py` and rewrote the notebook to `import maxio_source` +
`register_lakeflow_source(spark)`. **This is where the batch path can actually run on
Serverless v5**, with the caveats below.

### Why `%run` was not the answer
`%run` merges a notebook's globals into the driver session; it does **not** put an
importable package on `sys.path`, and a `%run`-defined class has `__module__ ==
"__main__"`, which Spark can't re-import by FQN on executors. The self-contained file +
`sys.path` import (or a proper wheel/`%pip`) is required for a distributed data source.

---

## Current state

- Batch read of Maxio from a notebook on Serverless v5 is **plausible** via
  `maxio_source.py` + `register_lakeflow_source(spark)` (the shim makes the import
  survive Spark 4.1; the closure design covers executors).
- This is **batch-only** and **hack-based**: streaming classes are inert stubs, the shim
  is uncommitted, and it runs on a runtime the connector doesn't officially target.
- **Not exercised at all:** CDC watermarking, SCD_TYPE_1/2 merge, delete flows, the
  metadata/virtual tables — all of which only run under the SDP pipeline.

---

## Best-practice path (assuming admin buy-in)

The intended, faithful deployment — no shims, no `--no-deps`, no uncommitted patches:

1. **Deploy the real pipeline** with the CLI:
   ```bash
   community-connector create_pipeline maxio maxio_ingest \
     -n <uc_connection> --catalog <cat> --target <schema> \
     --use-local-source   # until the branch is merged to remote master
   ```
   This provisions a Git repo + SDP pipeline on **`channel: PREVIEW`, `serverless: true`**
   — the runtime that has `pyspark.sql.streaming.datasource`. The connector loads
   **unmodified**.

2. **Credentials via a UC connection whose values are secret-scope references.** The
   framework injects options through a UC connection
   (`spark.databricks.unityCatalog.connectionDfOptionInjection.enabled`), so a connection
   is required — but its values can be
   `{{secrets/acuity-maxio-dev/api_key}}` etc., so nothing sensitive is typed inline.
   Reuses the scope we already loaded. Needs admin rights to create UC connections.

3. **Run it** (`run_pipeline`), starting with the smallest tables (`customers`, `items`)
   to validate auth/schema before `revenue_entries`.

### Admin asks
- Permission to **create a UC connection** in abl-sandbox (or have an admin create one
  pointing at the `acuity-maxio-dev` scope).
- Confirm **serverless SDP / PREVIEW channel** is enabled for the workspace.
- Get the **`databricks-ai-dev-kit` MCP** connected so future ops follow the standard
  path instead of CLI overrides.
- Longer term: **merge `feat/connector-maxio` to master** so `--use-local-source` isn't
  needed and the deployment pulls published source.

### Cleanup / hygiene
- The uploaded `maxio_source.py` and wheel carry an **uncommitted Spark-4.1 shim**;
  don't treat them as source of truth. Delete once the pipeline path is live.
- If a Spark-4.1 batch shim is ever wanted for real, do it properly: commit a
  `try/except ModuleNotFoundError` guard around the streaming import so future merge-script
  builds carry it — rather than patching artifacts by hand.

---

## Key file references

- Eager streaming import: `sparkpds/lakeflow_datasource.py:11` and
  `sources/maxio/_generated_maxio_python_source.py:30`
- Wheel excludes sources: root `pyproject.toml` → `[tool.setuptools.packages.find] exclude`
- Credential injection via connection: `pipeline/ingestion_pipeline.py:38`
  (`.option("databricks.connection", connection_name)`)
- Connector reads options: `sources/maxio/maxio.py:502-504`
- Pipeline runtime defaults: `tools/community_connector/.../default_config.yaml`
  (`channel: PREVIEW`, `serverless: true`)
- Register entrypoint: `sparkpds/registry.py` (`register`, `register_lakeflow_source`)
