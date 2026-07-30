# Session Handoff — lakeflow-community-connectors

## Current State

- **Active branch:** `feat/connector-maxio`
- **Last session date:** 2026-05-22
- **One-line status:** Maxio (SaaSOptics REST API v1.0) community connector landed; uncommitted refinements + a `load_maxio_secrets.py` resource staged. Marked `[needs-live-testing]` — has not been run against a real Maxio tenant yet.

## Recent Activity

**Commits (2026-05-20 → 2026-05-22, all branches):**
- `a1d28bb` 2026-05-22 — feat(maxio): add maxio connector [needs-live-testing]
- `c23e9bd` 2026-05-20 — Add ingestion_agent Spark source for the ingestion agent's read API (#184)
- `1379654` 2026-05-20 — feat(actitime): add actiTIME community connector (#176)

**Uncommitted (M = modified, ?? = untracked):**
- M `src/databricks/labs/community_connector/source_simulator/specs/maxio/endpoints.yaml`
- M `src/databricks/labs/community_connector/sources/maxio/README.md`
- M `src/databricks/labs/community_connector/sources/maxio/_generated_maxio_python_source.py`
- M `src/databricks/labs/community_connector/sources/maxio/maxio.py`
- M `src/databricks/labs/community_connector/sources/maxio/maxio_api_doc.md`
- ?? `src/databricks/labs/community_connector/sources/maxio/resources/load_maxio_secrets.py`
- ?? `tests/unit/sources/maxio/configs/`
- ?? `uv.lock`

## Context

- This Maxio connector is the **POC vehicle for the Acuity-Ingestion Platform** evaluation of Lakeflow Connect against a SaaS API source (SaaSOptics is one of the systems Distech ingestion is targeting).
- API surface covers 7 tables: customers, items, invoices, invoice_line_items, contracts, transactions, revenue_entries.
- Key API quirks already captured in `maxio_api_doc.md`: `modified__gte` / `auditentry__modified__gte` incremental filter is confirmed by the Singer tap; **revenue_entries sub-resource vs top-level endpoint discrepancy is flagged for live-test verification**.
- The agent-status file for this work is `Data/agent-status/acuity-ingestion-platform.md` in the Obsidian vault.

## Next Steps

1. Stand up a Maxio test tenant (or get one from the SaaSOptics/Maxio team) and run live-test verification — this is the `[needs-live-testing]` gate.
2. Verify the revenue_entries sub-resource vs top-level endpoint discrepancy against a real tenant.
3. Confirm pagination + `modified__gte` incremental filter behavior end-to-end on real data.
4. Commit the staged refinements + add `load_maxio_secrets.py` to a follow-up commit.
5. Open PR upstream to `databrickslabs/lakeflow-community-connectors` once live-tested.
6. Once GA-ready, port the same managed-secret pattern to the Distech connector at Acuity.
