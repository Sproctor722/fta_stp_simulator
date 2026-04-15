# STP/FTA Simulator — Deployment & Operations Runbook

## Architecture Overview

| Component | Details |
|---|---|
| **App URL** | https://stp-simulator-554179923494093.aws.databricksapps.com |
| **Databricks Workspace** | nike-sole-react.cloud.databricks.com |
| **SQL Warehouse** | NikeSoleSql-trade_customs (`/sql/1.0/warehouses/ff0a7c7156e45488`) |
| **Source Code (Workspace)** | `/Workspace/Users/shannon.proctor@nike.com/stp-simulator` |
| **FTA JSON Data** | `/Workspace/Users/shannon.proctor/fta-data` (37 files) |
| **GitHub Repo** | https://github.com/Sproctor722/fta_stp_simulator |
| **Service Principal** | `app-60lct4 stp-simulator` (ID: 72886663253854) |
| **App ID** | 4b9f64fb-851b-403d-99d1-0ef51ee1b53c |

## Application Files

| File | Purpose |
|---|---|
| `app.py` | Main Streamlit UI — 6 tabs, all visualizations and metrics (77 KB) |
| `databricks_loader.py` | Databricks SQL queries, dual-mode auth (PAT / service principal) (47 KB) |
| `real_data_loader.py` | CSV fallback loader, shared constants (rates, program names, HTS exclusions) (37 KB) |
| `fta_rules.py` | FTA JSON parser, ROO difficulty discounts, lane enrichment (25 KB) |
| `data.py` | Pipeline stage definitions, companion program constants (19 KB) |
| `app.yaml` | Databricks Apps config — command and environment variables |
| `requirements.txt` | Python dependencies |

## Data Sources

All queries target Unity Catalog views in `published_domain.trade_customs`:

| View | Purpose |
|---|---|
| `trade_goods_item_v` | Core goods items — origin, destination, HTS codes, goods value |
| `declaration_trade_shipment_v` | Shipment-level data linked to declarations |
| `declaration_entry_v` | Customs entries — filing references, acceptance dates, duty amounts |
| `item_commodity_v` | Product-commodity linkage |
| `commodity_classification_v` | HTS classification for commodities |
| `commodity_tariff_v` | Tariff rates by commodity and program |
| `trade_estimated_duty_item` | Estimated duty with MFN base rates + surcharges |

## How to Update the App

### Step 1: Edit locally

Modify files in the `stp-simulator-deploy` folder (or this repo).

### Step 2: Upload to Databricks and redeploy

Run `upload_and_deploy.py`, which:

1. Reads each local file, base64-encodes it
2. POSTs to `/api/2.0/workspace/import` with `overwrite=True`
3. POSTs to `/api/2.0/apps/stp-simulator/deployments` to trigger redeployment

Deployment takes approximately 30–45 seconds.

### Step 3: Verify

- Check deployment status in Databricks Apps UI (Deployments tab)
- Refresh the app URL in browser

### Forcing a data refresh

The app caches query results using Streamlit's `@st.cache_data`. To refresh:

- **In-app**: Click "Refresh Data" in the sidebar
- **On deploy**: Increment `_DATA_VERSION` in `app.py` to invalidate all caches

## Pre-Demo Checklist

| Check | How to Verify | Fix If Needed |
|---|---|---|
| SQL Warehouse is RUNNING | Databricks UI > SQL Warehouses > NikeSoleSql-trade_customs | Click Start (2–3 min warm-up) |
| App status is RUNNING | Databricks UI > Compute > Apps > stp-simulator | Click Deploy if stopped |
| App loads with data | Open the app URL | Check Logs tab; verify warehouse is running |
| PAT token is valid | App shows live data, not CSV fallback | Generate new PAT, update app.yaml, redeploy |
| FTA JSONs accessible | Trade Lanes table shows FTA difficulty/rules | Re-upload to /Workspace/Users/shannon.proctor/fta-data |

## Troubleshooting

### 502 Bad Gateway

**Cause:** Streamlit process crashed on startup.

**Fix:** Check the Logs tab in Databricks Apps UI. Common causes:
- Python import error
- Missing dependency in `requirements.txt`
- Syntax error in app code

### "Error during request to server"

**Cause:** Cannot connect to SQL warehouse. Warehouse is stopped, PAT expired, or HTTP path is wrong.

**Fix:**
1. Start the warehouse
2. Verify `DATABRICKS_TOKEN` in `app.yaml`
3. Verify `DATABRICKS_HTTP_PATH` matches an active warehouse

### "Real data files not found" / "No data available"

**Cause:** Databricks connection failed and CSV fallback has no files. The page should display the underlying error.

**Fix:** Resolve the Databricks connection issue (usually warehouse or PAT).

### set_page_config() error

**Cause:** A Streamlit display command executed before `st.set_page_config()`.

**Fix:** Ensure no `st.*` display calls run before `st.set_page_config()` in `app.py`.

### App shows stale data

**Cause:** Streamlit caches query results.

**Fix:** Click "Refresh Data" in sidebar, or increment `_DATA_VERSION` in `app.py` and redeploy.

## Environment Variables (app.yaml)

| Variable | Purpose | Notes |
|---|---|---|
| `DATABRICKS_HOST` | Workspace URL | Only needed for PAT auth |
| `DATABRICKS_HTTP_PATH` | SQL warehouse endpoint | Required for both auth modes |
| `DATABRICKS_TOKEN` | Personal Access Token | Temporary — replace with service principal |
| `FTA_DATA_DIR` | Path to FTA JSON summaries | `/Workspace/Users/shannon.proctor/fta-data` |
| `STREAMLIT_GATHER_USAGE_STATS` | Disable Streamlit telemetry | Set to `false` |

### Transitioning to service principal auth

When a workspace admin grants the service principal access:

1. Grant `Can Use` on the SQL warehouse to `app-60lct4 stp-simulator`
2. Run:
   ```sql
   GRANT USE CATALOG ON CATALOG published_domain TO `app-60lct4 stp-simulator`;
   GRANT USE SCHEMA ON SCHEMA published_domain.trade_customs TO `app-60lct4 stp-simulator`;
   GRANT SELECT ON SCHEMA published_domain.trade_customs TO `app-60lct4 stp-simulator`;
   ```
3. Remove `DATABRICKS_HOST` and `DATABRICKS_TOKEN` from `app.yaml`
4. The Databricks SDK will auto-detect `DATABRICKS_CLIENT_ID` and `DATABRICKS_CLIENT_SECRET` injected by the runtime

## Security Notes

| Item | Current State | Target State |
|---|---|---|
| Authentication | PAT token in app.yaml (temporary) | Service principal with proper grants |
| app.yaml in GitHub | **DO NOT push** — contains PAT | Remove PAT before pushing; use Databricks secrets |
| Data Access | PAT has user's full permissions | Scoped SELECT on trade_customs only |
| App Access Control | Anyone with URL can view | Configure sharing in Databricks Apps UI |

## Key Contacts

| Role | Person | Context |
|---|---|---|
| App Owner | Shannon Proctor | Deployment, updates, data questions |
| Trade & Customs Domain | Ankita / Trade team | Data model, published_domain views |
| Stakeholder | Maribel Jimenez | FTO lead, demo audience |
| Workspace Admin (needed) | TBD | Service principal permissions |

---

*Last updated: April 15, 2026*
