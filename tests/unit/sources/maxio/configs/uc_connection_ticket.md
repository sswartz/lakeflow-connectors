# Ticket: Create Unity Catalog connection for Maxio (SaaSOptics)

**Workspace:** `https://adb-467720429104851.11.azuredatabricks.net` (profile `abl-databricks-dev`)

**Requester:** Stuart Swartz (`stuart.swartz@databricks.com`) — needs USE permission on the resulting connection.

**Purpose:** Stand up a Lakeflow Community Connector ingestion pipeline (`maxio_bronze`) that pulls 7 tables from the Acuity SaaSOptics tenant into `cat_bi_sandbox_dev.lakeflow_poc`. The pipeline cannot be created until this UC connection exists.

---

## 1. Connection to create

| Field | Value |
|---|---|
| **Connection name** | `maxio_dev` |
| **Connector type** | Lakeflow Community Connector for Maxio (SaaSOptics) — repo `databrickslabs/lakeflow-community-connectors`, source `maxio` |
| **Workspace** | `https://adb-467720429104851.11.azuredatabricks.net` |
| **External options allowlist** | `max_records_per_batch` |

## 2. Connection parameters (3)

All three values are already stored in the Databricks secret scope **`acuity-maxio-dev`** in this same workspace, populated by Eliott. The connection assignee should read them from there:

| Parameter | Type | Secret? | Source |
|---|---|---|---|
| `server_subdomain` | string | no | secret-scope `acuity-maxio-dev`, key `server_subdomain` |
| `account_name` | string | no | secret-scope `acuity-maxio-dev`, key `account_name` |
| `api_key` | string | **yes** | secret-scope `acuity-maxio-dev`, key `api_key` |

For reference, the connector hits `https://{server_subdomain}.saasoptics.com/{account_name}/api/v1.0/` and auths with header `Authorization: Token <api_key>` (note: the keyword is literally `Token`, not `Bearer`).

## 3. Suggested CLI command (for the assignee to run)

This assumes the assignee has `community-connector` CLI installed (`tools/community_connector` in the lakeflow-community-connectors repo) and a Databricks profile with permission to create UC connections in the target workspace.

```bash
# Read the three values from the existing secret scope.
SCOPE=acuity-maxio-dev
PROFILE=<the-assignee-profile-with-create-connection-perms>

CREDS_JSON=$(python - <<'PY'
import base64, json
from databricks.sdk import WorkspaceClient
w = WorkspaceClient(profile="<PROFILE>")
def g(k):
    return base64.b64decode(w.secrets.get_secret(scope="acuity-maxio-dev", key=k).value).decode()
print(json.dumps({k: g(k) for k in ("server_subdomain", "account_name", "api_key")}))
PY
)

# Create the UC connection.
community-connector create_connection maxio maxio_dev -o "$CREDS_JSON"
```

The CLI reads `connector_spec.yaml` and automatically attaches `externalOptionsAllowList="max_records_per_batch"` to the connection — the assignee does **not** need to pass it.

## 4. Permission grant

After the connection is created, grant USE on it to `stuart.swartz@databricks.com` (or a group Stuart is in). In Databricks SQL:

```sql
GRANT USE CONNECTION ON CONNECTION maxio_dev TO `stuart.swartz@databricks.com`;
```

## 5. Verification

The connection assignee can confirm the connection works by running, in any notebook attached to the target workspace:

```python
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
conn = w.connections.get(name="maxio_dev")
print(conn.connection_type, conn.options.keys())
```

It should print `LAKEFLOW_COMMUNITY_CONNECTOR` (or similar) and the four option keys: `server_subdomain`, `account_name`, `api_key`, `externalOptionsAllowList`.

---

## Once the connection exists

Stuart will deploy the ingestion pipeline with:

```bash
community-connector create_pipeline maxio maxio_bronze \
  -ps tests/unit/sources/maxio/configs/maxio_bronze_spec.json \
  -c cat_bi_sandbox_dev -t lakeflow_poc \
  --use-local-source
```

…where `maxio_bronze_spec.json` is checked in alongside this ticket file.
