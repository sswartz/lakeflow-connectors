# Lakeflow Maxio Community Connector

Ingest data from the **Maxio Core / SaaSOptics REST API v1.0** into Databricks Delta tables via Lakeflow Connect.

> **Which Maxio product does this connector target?**
>
> This connector targets **Maxio Core**, formerly known as **SaaSOptics** — Maxio's billing and revenue-recognition product. The base URL for this API looks like `https://{server_subdomain}.saasoptics.com/{account_name}/api/v1.0/`.
>
> It does **not** target **Maxio Advanced Billing** (formerly Chargify), which is a separate product with a different API surface, base URL, and authentication scheme. If your data lives in Advanced Billing / Chargify, this connector is not the right tool.

## Prerequisites

- **A Maxio Core (SaaSOptics) account** with API access enabled on the tenant.
- **An API token** issued from the SaaSOptics admin UI. See "Obtaining the API token" below.
- **Your tenant's `server_subdomain` and `account_name`** — both are visible in the URL you use to access SaaSOptics in the browser. For example, if you log in at `https://s12.saasoptics.com/qbdv10_lucid/`, then `server_subdomain` is `s12` and `account_name` is `qbdv10_lucid`.
- **Network access**: the environment running the connector must be able to reach `https://{server_subdomain}.saasoptics.com`.
- **Lakeflow / Databricks environment**: a workspace where you can register a Lakeflow community connector and run ingestion pipelines.

## Setup

### Required Connection Parameters

| Name | Type | Required | Description | Example |
|------|------|----------|-------------|---------|
| `server_subdomain` | string | yes | SaaSOptics instance subdomain — the portion before `.saasoptics.com` in your tenant URL. | `s12` |
| `account_name` | string | yes | SaaSOptics account / instance path segment — the segment immediately after the subdomain in your tenant URL. | `qbdv10_lucid` |
| `api_key` | string (secret) | yes | SaaSOptics API token. Sent on every request as the `Authorization: Token <api_key>` header (note: not Bearer / not OAuth). | `abc123...` |
| `externalOptionsAllowList` | string | no | Comma-separated list of allowed table-specific options. The only table option this connector reads is `max_records_per_batch`. Leave empty unless you intend to override it per table. | `max_records_per_batch` |

Together, the three required parameters assemble the API base URL the connector calls:

```
https://{server_subdomain}.saasoptics.com/{account_name}/api/v1.0/
```

Both `server_subdomain` and `account_name` are tenant-specific and vary per customer.

### Obtaining the API token

1. Log in to your Maxio Core / SaaSOptics instance as an account admin.
2. Navigate to **Admin → API Tokens** in the settings section.
3. Create a new token with a descriptive name (e.g. `Lakeflow ingestion`). Only account admins can generate tokens initially; once created, users can regenerate their own.
4. Copy the token value and store it securely (e.g. in a Databricks secret scope or key vault). The token does not expire on a fixed schedule but can be revoked by an admin at any time.

**Permissions / scope**: the API token inherits the permissions of the user account that generated it. SaaSOptics does not define separate OAuth-style scopes — access is granted at the user/instance level. For ingestion, an admin-issued token with read access to billing, contract, and revenue data is sufficient.

**Rotation**: tokens can be revoked and re-issued at any time from the same **Admin → API Tokens** page. After rotating, update the `api_key` value on the Unity Catalog connection and re-run the pipeline.

### Create a Unity Catalog Connection

A Unity Catalog connection for this connector can be created in two ways via the UI:

1. Follow the **Lakeflow Community Connector** UI flow from the **Add Data** page.
2. Select any existing Lakeflow Community Connector connection for this source or create a new one.
3. Provide `server_subdomain`, `account_name`, and `api_key`. Most deployments do not need to set `externalOptionsAllowList`.

The connection can also be created using the standard Unity Catalog API.

## Supported Objects

The Maxio connector exposes **7 tables**, covering the core billing, contract, and revenue-recognition data model.

| Table | Description | Ingestion Type | Primary Key | Cursor field |
|-------|-------------|----------------|-------------|--------------|
| `customers` | Customer accounts, with the customer's billing profile flattened into `billing_*` columns and the shipping profile into `shipping_*` columns. | `cdc` | `id` | `modified` |
| `items` | Product / item catalog entries (subscriptions, one-time charges, discounts, sales tax items). | `cdc` | `id` | `modified` |
| `invoices` | Invoices issued to customers. Header-level only — the `line_items` array is exploded into a separate `invoices_line_items` table. | `cdc` | `id` | `audit_modified` |
| `invoices_line_items` | Individual invoice line items, derived by exploding `line_items[]` on each invoice. | `snapshot` | `(invoice_id, line_id)` | n/a |
| `contracts` | Customer contracts, with the contract's billing profile flattened into `billing_*` columns. | `cdc` | `id` | `modified` |
| `transactions` | Per-contract transactions (subscription lines, renewals, true-ups) including ARR/MRR amounts and revenue-recognition flags. | `cdc` | `id` | `audit_modified` |
| `revenue_entries` | Period-level revenue recognition entries per transaction (start/end dates and recognized amounts in home and local currency). | `snapshot` | `id` | n/a |

### Ingestion behavior

- **CDC tables** (`customers`, `items`, `invoices`, `contracts`, `transactions`) use a server-side `modified__gte` or `auditentry__modified__gte` filter on the SaaSOptics API to fetch only records changed since the last sync. On the first run, all historical records are fetched (no checkpoint to resume from); subsequent runs ingest only changes since the saved watermark.
- **`invoices_line_items`** is derived directly from the `invoices` payload — there is no standalone API endpoint for line items. It is ingested as a snapshot every run by re-walking the `invoices/` endpoint and exploding the `line_items` array. To pick up changes to line items on already-ingested invoices, run a periodic snapshot.
- **`revenue_entries`** is fetched as a per-transaction sub-resource: the connector first enumerates transactions via `/transactions/`, then for each transaction calls `/transactions/{id}/revenue_entries/` to fetch its revenue entries. See "Known behaviors and caveats" below for the cost implications.

### Schema highlights

- **Audit fields**. On `invoices` and `transactions`, the API's nested `auditentry` object is flattened into top-level columns prefixed `audit_` (`audit_created`, `audit_modified`, `audit_created_by`, etc.). The `audit_modified` column is the CDC cursor for these tables.
- **Billing profile flattening**. On `customers` and `contracts`, the API's nested `billing_profile` object is flattened into top-level `billing_*` columns. See "Address-field naming asymmetry" under "Known behaviors and caveats".
- **Currency duality**. Financial tables (`invoices`, `transactions`, `revenue_entries`, and the `invoices_line_items` projection) carry both `home_*` and `local_*` amount columns — `home_*` is the tenant's reporting currency, `local_*` is the customer's billing currency. `foreign_exchange_rate` on `invoices` and `transactions` bridges them (`home_amount = local_amount * foreign_exchange_rate`). Preserve both downstream; analytics consumers choose which to use.
- **JSON-string columns**. A few raw nested fields are preserved as serialized JSON strings to keep the table schema flat: `auditentry` on `customers` and `tax_lines` on `invoices`. Use Spark's `from_json` if you need to project fields out of them.

## Table Configurations

### Source & Destination

These are set directly under each `table` object in the pipeline spec:

| Option | Required | Description |
|---|---|---|
| `source_table` | Yes | Table name in the source system (one of the 7 listed above). |
| `destination_catalog` | No | Target catalog (defaults to pipeline's default). |
| `destination_schema` | No | Target schema (defaults to pipeline's default). |
| `destination_table` | No | Target table name (defaults to `source_table`). |

### Common `table_configuration` options

These are set inside the `table_configuration` map alongside any source-specific options:

| Option | Required | Description |
|---|---|---|
| `scd_type` | No | `SCD_TYPE_1` (default) or `SCD_TYPE_2`. Only applicable to CDC and snapshot tables. |
| `primary_keys` | No | List of columns to override the connector's default primary keys. |
| `sequence_by` | No | Column used to order records for SCD Type 2 change tracking. |

### Source-specific `table_configuration` options

| Option | Required | Description |
|---|---|---|
| `max_records_per_batch` | No | Soft cap on how many records the connector will fetch from a single source table in one read batch before yielding back to the framework. Defaults to `10000`. If you set this, you must include `max_records_per_batch` in `externalOptionsAllowList` on the connection. |

No other source-specific options are required. The 7 tables above work out of the box with no per-table configuration.

## Data Type Mapping

| Maxio API type | Connector Spark type | Notes |
|----------------|----------------------|-------|
| Integer IDs (`id`, `parent`, FKs like `contract`, `item`, `customer`) | `LongType` | All SaaSOptics identifiers fit in 64 bits. |
| Date strings (`yyyy-MM-dd`, e.g. invoice `date`, `due_date`, `entry_date`) | `DateType` | |
| ISO 8601 datetime strings (e.g. `modified`, `exported_date`, `sync_date`) | `TimestampType` | |
| `auditentry.modified` / `auditentry.created` (flattened to `audit_modified`, `audit_created`) | `StringType` | Kept as ISO 8601 string — the CDC cursor compares lexicographically. |
| Numeric amounts and rates (`subtotal`, `balance`, `home_amount`, `local_amount`, `foreign_exchange_rate`, `quantity`, …) | `DoubleType` | The API returns these as JSON strings (e.g. `"123.45"`); the connector casts them on read. |
| Custom numeric fields (`number_field1`, `number_field2`, `number_field3`) | `StringType` | Deliberately preserved as strings — they may contain non-numeric values depending on tenant configuration. |
| Booleans (`is_active`, `do_not_sync`, `is_paid`, …) | `BooleanType` | `null` for missing values. |
| Free-form text and codes (`name`, `code`, `notes`, currency codes, `qb_id`, `sf_id`, `chargify_id`, etc.) | `StringType` | |
| Nested `billing_profile.*` (on `customers`, `contracts`) | flattened to `billing_*` columns | See "Address-field naming asymmetry" below. |
| Nested `auditentry.*` (on `invoices`, `transactions`) | flattened to `audit_*` columns | Cursor field is `audit_modified`. |
| Nested `auditentry` (on `customers`) | `StringType` (JSON-encoded) | Retained as a single column to match the source. |
| Nested `tax_lines` (on `invoices`) | `StringType` (JSON-encoded) | Use Spark's `from_json` downstream if you need to project fields. |

## Known behaviors and caveats

A few SaaSOptics-specific behaviors are worth knowing before you build downstream consumers.

### Many numeric fields arrive from the API as strings

On `invoices`, `invoices_line_items`, `transactions`, and `revenue_entries`, SaaSOptics returns most amount, rate, and quantity fields as JSON strings rather than JSON numbers (e.g. `"123.45"` rather than `123.45`). This is the source system's behavior — the connector casts them to `double` on read so the destination Delta table is typed normally. You don't need to cast again downstream, but if you write your own client against the same API you'll see strings.

The `number_field1`, `number_field2`, `number_field3` custom-numeric fields on `customers` and `contracts` are an exception — they are kept as `string` because their content varies per tenant and is not guaranteed to be numeric.

### `revenue_entries` fan-out cost

`revenue_entries` is fetched per-transaction. The connector first enumerates all transactions via `/transactions/`, then for each transaction issues `GET /transactions/{id}/revenue_entries/` to retrieve that transaction's revenue periods. This means ingestion time for `revenue_entries` is roughly proportional to the number of transactions in your tenant, not to the number of revenue entries themselves. Tenants with many transactions should expect this table to take significantly longer to sync than the other six.

If you do not need period-level revenue recognition data downstream, omit `revenue_entries` from your pipeline spec — the other tables will sync normally.

### Address-field naming asymmetry on `customers` vs `contracts`

The two tables that flatten `billing_profile` use slightly different column-name conventions for the shipping address, and this asymmetry is preserved deliberately to match the source's normalization:

- On **`customers`**, shipping fields are flattened **without** a `billing_` prefix: `shipping_addr1`, `shipping_city`, `shipping_country`, etc.
- On **`contracts`**, shipping fields are flattened **with** a `billing_` prefix: `billing_shipping_addr1`, `billing_shipping_city`, `billing_shipping_country`, etc.

This is not a bug. If you join the two tables and want consistent column names, alias them in your downstream query.

### `line_items` is dropped from `invoices` and exposed as its own table

The `invoices` table does not contain the `line_items` array. It is exploded into the separate `invoices_line_items` table, keyed by `(invoice_id, line_id)`. Join on `invoice_id` if you need invoice + line item data together.

Because `invoices_line_items` has no independent cursor, it is re-derived from a snapshot of `invoices` on every run. Changes to line items on already-ingested invoices will be picked up the next time the parent invoice is fetched (i.e. the next time `audit_modified` advances).

### Rate limiting

SaaSOptics does not publish an official rate-limit quota. The connector self-caps client-side throughput at approximately **5 requests per second** and honors any `Retry-After` header SaaSOptics returns on a `429 Too Many Requests` response. On retriable errors (`429`, `500`, `502`, `503`, `504`) it retries with exponential backoff up to 5 attempts.

You typically do not need to do anything for rate limiting — the connector recovers on its own. If you consistently see slow runs, schedule pipelines less frequently rather than tuning the client.

### Initial sync window

The CDC tables (`customers`, `items`, `invoices`, `contracts`, `transactions`) do a full historical backfill on the first run because there is no checkpoint to resume from. Subsequent runs only fetch records modified since the saved watermark. For large tenants, the initial run can take a while — plan accordingly.

## How to Run

### Step 1: Reference the connector in your workspace

Follow the Lakeflow Community Connector UI flow from the **Add Data** page to copy or reference the Maxio connector source in your workspace.

### Step 2: Configure your pipeline

Update the `pipeline_spec` in your main pipeline file (e.g. `ingest.py`), point at the Unity Catalog connection, and list the tables to ingest:

```json
{
  "pipeline_spec": {
    "connection_name": "my_maxio_connection",
    "objects": [
      { "table": { "source_table": "customers" } },
      { "table": { "source_table": "items" } },
      { "table": { "source_table": "invoices" } },
      { "table": { "source_table": "invoices_line_items" } },
      { "table": { "source_table": "contracts" } },
      { "table": { "source_table": "transactions" } },
      { "table": { "source_table": "revenue_entries" } }
    ]
  }
}
```

### Step 3: Run and schedule the pipeline

The first run does a full backfill across all tables. Subsequent runs ingest only changes since the last sync for CDC tables; snapshot tables (`invoices_line_items`, `revenue_entries`) re-fetch each run.

#### Best practices

- **Start small**: begin by syncing `customers` and `items` to validate authentication and the destination schema before turning on the larger tables.
- **Use incremental sync**: the CDC tables drastically cut API call volume on subsequent runs — let them do their job.
- **Schedule thoughtfully**: balance data freshness against the per-transaction fan-out on `revenue_entries`. Hourly schedules are usually overkill; daily or every few hours is typical for billing data.
- **Plan the first run on `revenue_entries`**: if your tenant has many thousands of transactions, the first sync of `revenue_entries` may take significantly longer than the others.

#### Troubleshooting

**`401 Unauthorized`**

- Cause: the `api_key` is wrong, has been revoked by an admin, or is being sent with the wrong header keyword. The SaaSOptics API uses `Authorization: Token <api_key>` (not `Bearer`) — the connector handles this for you, so if you see a 401 the most likely cause is that the token itself is invalid or revoked.
- Fix: re-issue the token from **Admin → API Tokens** in SaaSOptics and update the `api_key` on the Unity Catalog connection.

**`404 Not Found`**

- Cause: either `server_subdomain` or `account_name` is wrong, so the base URL the connector is calling does not exist. Both come straight out of your SaaSOptics URL — for `https://s12.saasoptics.com/qbdv10_lucid/`, `server_subdomain` is `s12` and `account_name` is `qbdv10_lucid`.
- Fix: log in to SaaSOptics in the browser, copy the two segments out of the URL bar, and update the Unity Catalog connection parameters.

**`429 Too Many Requests`**

- Cause: client exceeded SaaSOptics' (undocumented) rate limit.
- Behavior: the connector retries automatically with exponential backoff and honors `Retry-After` if present. You typically don't need to do anything — the run will continue to completion.
- If it keeps happening: reduce the frequency of pipeline runs, or run the larger tables (`transactions`, `revenue_entries`) on a separate, less-frequent schedule.

**`revenue_entries` sync is slow**

- Cause: revenue entries are fetched per-transaction (see "Known behaviors and caveats"). If your tenant has many transactions, this table will be the slowest to sync.
- Fix: only ingest `revenue_entries` if you actually need period-level revenue recognition downstream. Otherwise omit it from the pipeline spec.

**Numeric columns look like strings when I query the source API directly but are doubles in my Delta table**

- This is expected. SaaSOptics returns amount, rate, and quantity fields as JSON strings; the connector casts them to `double` on read so the Delta table is typed. See "Known behaviors and caveats".

## References

- [Maxio Core (SaaSOptics) — Getting Started and Authenticating](https://support.saasync.com/article/176-getting-started-and-authenticating-with-maxio-core-formerly-saasoptics)
- [SaaSOptics on API Tracker](https://apitracker.io/a/saasoptics)
- [Stitch — SaaSOptics integration reference](https://www.stitchdata.com/docs/integrations/saas/saasoptics)
- [Lakeflow Community Connectors Documentation](https://docs.databricks.com/en/lakehouse-connect/)

## Connector Information

- **Source**: Maxio Core / SaaSOptics REST API v1.0 (`https://{server_subdomain}.saasoptics.com/{account_name}/api/v1.0/`)
- **Supported Objects**: 7 tables (`customers`, `items`, `invoices`, `invoices_line_items`, `contracts`, `transactions`, `revenue_entries`)
- **Authentication**: API token, sent as `Authorization: Token <api_key>` (not Bearer / not OAuth)
- **Supported Ingestion Types**: `cdc`, `snapshot`
