<!-- Copyright 2026 Snowflake Inc.
SPDX-License-Identifier: Apache-2.0

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License. -->

# DCR Migration Tool

> **Version:** 2.3.0 &nbsp;|&nbsp; **Target:** Snowflake Collaboration Hub (API v2.0) &nbsp;|&nbsp; **Source:** Legacy Provider & Consumer (P&C) and UI Clean Rooms

---

## Overview

The DCR Migration Tool is an automated engine that upgrades legacy P&C API and UI cleanrooms to the new Snowflake Collaboration Hub architecture. It abstracts the complexity of writing YAML specifications and API calls into a streamlined **Plan → Execute → Finalize → Validate** workflow.

**Provider:** Generate Plan > Execute Setup > Join (worksheet) > Validate

**Consumer:** Generate Plan > Execute Setup > Review + Join + Link (worksheet) > Validate

| Component | Description |
|-----------|-------------|
| **Backend** | Suite of Snowflake Stored Procedures (Python) for spec generation, orchestration, and audit logging |
| **Frontend** | Streamlit App (running in Snowsight) providing a guided migration UI |

---

## Features

- **Automated Discovery** — Detects your role (Provider or Consumer) and enumerates templates, datasets, and policies from the legacy cleanroom.
- **Spec Generation** — Converts legacy SQL templates and table policies into v2.0 compliant YAML specs with literal block style for readability.
- **Smart Column Type Detection** — Recognizes common join column abbreviations (`HEM`, `HPN`, `IDFA`, etc.) and maps them to valid Snowflake `column_type` identifiers.
- **Python Cleanroom & UDF Migration** — Reads `SAMOOHA_CLEANROOM_<id>.SHARED_SCHEMA.LOAD_PYTHON_RECORD` and lists `@APP.CODE/V1_0P1`. Matches both P&C flows from [Use Python in a clean room](https://docs.snowflake.com/en/user-guide/cleanrooms/provider#use-python-in-a-clean-room): **inline** `load_python_into_cleanroom` (BODY → `code_body`) and **stage** overload (`imports` in metadata → Collaboration `artifacts` + per-function `imports`, see [custom functions](https://docs.snowflake.com/en/user-guide/cleanrooms/v2/custom-functions)). Stage paths default to `@SAMOOHA_CLEANROOM_<id>.APP.CODE/<patch>/…` with `<patch>` inferred from stage listings or `V1_0P1`. You remain responsible for Collaboration stage rules (internal stage, `DIRECTORY`, `SNOWFLAKE_SSE`, etc.). Rewrites template SQL to `cleanroom.<migrated_py_spec>$<udf>(` and registers templates with `code_specs` linkage. Requires Data Clean Rooms 12.9+ for custom functions.
- **UI cleanrooms** — Cleanrooms created in the Snowflake UI have a human-readable name distinct from the cleanroom id. The tool resolves either name or id via `VIEW_CLEANROOMS()`, uses the **id** for all P&C API calls, and names the Collaboration `migrated_<HUMAN_READABLE_NAME>` (UPPERCASE). Platform-privacy SQL templates (name contains `platform_privacy` / `prod_sql_with_platform_privacy`) are skipped in generated template specs; validate parity and data offerings accordingly.
- **Safety Guardrails** — Pre-flight checks block unsupported configurations (multi-provider, ML Jobs, SPCS) and warn about privacy downgrades (Differential Privacy).
- **Deterministic Versioning** — Provider and consumer artifact IDs use a shared suffix (currently **`MIGRATION_V2`**) so template, data offering, and collaboration registrations stay aligned.
- **Parity Validation** — Compares the new Collaboration against the legacy Cleanroom to verify template and data offering coverage.
- **Audit Logging** — Every migration run is logged to `MIGRATION_JOBS` with job ID, timestamps, status, and details.
- **Migration History** — "Migrated DCRs" view shows all past migrations with live collaboration status and job metadata.
- **Re-migration Support** — Teardown a failed collaboration and re-run; templates and data offerings are safely skipped if already registered.
- **ML Jobs / SPCS / Differential Privacy Detection** — Pre-flight checks scan `LOAD_PYTHON_RECORD` for compute pools and `snowflake_ml_python`, scan templates for SPCS `service_functions.` references, and detect DP noise injection patterns (`addnoise`, `laplace`, `dp_noise`). Unsupported cleanrooms are blocked; DP cleanrooms receive a privacy-downgrade warning.
- **Consumer Manual Join SQL** — Consumer EXECUTE returns a pre-built SQL script containing REVIEW + JOIN + LINK_DATA_OFFERING + SET_CONFIGURATION calls, ready for worksheet execution.
- **Smart Schema Policy Generation** — `GENERATE_DATA_OFFERING_SPECS` iterates all columns from `DESC TABLE`, using `guess_type()` + `refine_type_by_data()` to auto-classify each column as `join_standard` (with inferred `column_type`) or `passthrough`.
- **Human-Readable Collaboration Naming** — Resolves display names from `VIEW_CLEANROOMS()` for UPPERCASE naming (`migrated_HUMAN_NAME`).
- **ReferenceUsageGrantMissing Auto-Remediation** (Streamlit) — Parses error details, generates ready-to-copy `GRANT REFERENCE_USAGE` commands, and provides a one-click Teardown button with automated retry.
- **Cleanroom Classification Display** — Sidebar shows P&C vs UI classification with counts; 5-metric dashboard (Type, Role, Templates, Data Offerings, Status).
- **Template Classification Breakdown** — Review Plan tab shows PLATFORM_PRIVACY vs STANDARD counts with status icons.

---

## Prerequisites

1. **Snowflake Data Clean Room app** must already be installed on your account.
2. You must have access to the `SAMOOHA_APP_ROLE` role.
3. The role must have permissions to create databases/schemas (for tool installation) and call Native App procedures.

## Discovering the Migration Tool

| Channel | Details |
|---------|---------|
| **Documentation (GA)** | [docs.snowflake.com/user-guide/cleanrooms/migration-to-collab](https://docs.snowflake.com/user-guide/cleanrooms/migration-to-collab) |
| **Direct link** | You may receive a link from Snowflake Support or Solutions Engineering |
| **GitHub (v1)** | Download the code directly from the GitHub repository |

## Installation

### 1. Deploy the Backend

1. Log in to [**app.snowflake.com**](https://app.snowflake.com).
2. Open a new **SQL Worksheet**.
3. Copy the contents of `migration-backend.sql` from the GitHub repository.
4. Click **Run All**.

This creates the `DCR_SNOWVA.MIGRATION` schema with all stored procedures and the `MIGRATION_JOBS` audit table.

### Stored procedure entry points

| Procedure | Purpose |
|-----------|---------|
| **`AGENT_MIGRATE_ORCHESTRATOR(cleanroom_name, action_mode)`** | **Primary** orchestration entry. `action_mode`: `PLAN`, `EXECUTE`, `CHECK_STATUS`, `VALIDATE`, `TEARDOWN`. Accepts legacy **name or UUID**; resolves UI cleanroom names internally via `VIEW_CLEANROOMS()`. Sets Snowflake **QUERY_TAG** (`dcr_migration_tool:v3.0:…`) during actions for observability. Generated worksheets use **`collaboration.initialize(..., 'APP_WH')`**, **`SET_CONFIGURATION('TEMPLATE_AUTO_APPROVAL', 'true')`**, and commented **`add_template_request`** / dual-collaborator **`link_data_offering`** where applicable. Consumer EXECUTE returns `manual_join_sql` for worksheet-based REVIEW + JOIN + LINK. |
| Other `DCR_SNOWVA.MIGRATION.*` procedures | Building blocks (`CHECK_PREREQUISITES`, `PREVIEW`, `GENERATE_*`, `VALIDATE`, …) callable directly for advanced use. |

**Resolution strategy:** The orchestrator resolves human-readable UI names and UUIDs via `VIEW_CLEANROOMS()` to find the **cleanroom id** used in P&C API calls. Collaboration names use the human-readable display name in UPPERCASE (`migrated_HUMAN_NAME`).

### Standalone reference SQL vs this `migration-backend.sql`

You may have a **pasted** script that looks similar but differs line-by-line from the repo. Most gaps are **intentional** so UI cleanrooms, Streamlit, and Collaboration names stay consistent.

| Area | Typical pasted snippet | This repository |
|------|-------------------------|-----------------|
| **Discovery** | `VIEW_CLEANROOMS` / `IS_ENABLED` inlined in each procedure | Centralized resolution via `VIEW_CLEANROOMS()` in orchestrator |
| **Collaboration name** | `migrated_{display_name_with_underscores}` | **`migrated_{HUMAN_READABLE_NAME}`** (UPPERCASE) — resolved from `VIEW_CLEANROOMS()` |
| **Prerequisites** | Basic LAF / multi-provider checks | **ML Jobs**, **SPCS**, **Differential Privacy**, multi-provider detection; LAF checks removed |
| **VALIDATE** | P&C calls with raw input; parity counts all legacy templates | Uses resolved cleanroom id; **skips** platform-privacy templates in parity; adds `USE SECONDARY ROLES NONE` |
| **`gen_templates` errors** | `return []` when no rows | Returns **`{ "templates": [], ... }`** (VARIANT object) |
| **PLAN summary** | `len(tmps)` includes `PLATFORM_PRIVACY` dict rows | Counts **registerable** templates only; **template classification** breakdown |
| **`GENERATE_COLLABORATION_SPEC`** | `CLEANROOM_RECORD` with simple `UPPER(name)` | Tries `CURRENT_ORGANIZATION_NAME().CURRENT_ACCOUNT_NAME()` first; **multi-consumer** runners when `VIEW_CONSUMERS` returns many rows |

| **Auto-approval** | `enable_template_auto_approval` | **`SET_CONFIGURATION('TEMPLATE_AUTO_APPROVAL', 'true')`** |
| **Table registration** | `REGISTER_TABLE` | **`REGISTER_OBJECTS`** |

### Known limitations (manual follow-up)

- **Legal terms:** `SYSTEM$ACCEPT_LEGAL_TERMS` runs during collaboration **initialize** / **join**; it **cannot** run inside Streamlit or inside a stored procedure. Use a **SQL Worksheet** with the generated script when the app reports that hint.
- **Template chains:** Legacy `add_template_chain` is **not** migrated; flatten into separate templates or handle outside the tool.
- **ML Jobs / SPCS:** Cleanrooms using ML Jobs (compute pools) or SPCS (service functions) are **blocked** at pre-flight. These features are not yet supported in the Collaboration API.
- **Differential Privacy:** DP-enabled templates will be migrated but **without noise injection** — this is a privacy downgrade. The tool warns but does not block. Confirm with the data provider before proceeding.
- **Platform privacy templates:** Skipped as Collaboration templates; handled via freeform SQL data offerings. Parity validation checks for freeform-enabled offerings.
- **Aggregation policies:** Creating policies for freeform SQL may require **elevated privileges** (e.g. ACCOUNTADMIN).

### 2. Deploy the Streamlit App

You have two options:

#### Option A: Create from Repository (Preferred)

1. In [app.snowflake.com](https://app.snowflake.com), navigate to **Streamlit**.
2. Click **Create from Repository**.
3. Paste the GitHub repository URL.
4. Select the **Database** where the app will be stored and the **Warehouse** used to run it.
5. Click **Create**.

#### Option B: Manual Upload

1. Download `streamlit_app.py` from the GitHub repository.
2. In [app.snowflake.com](https://app.snowflake.com), navigate to **Streamlit**.
3. Click **+ Streamlit App**.
4. Name it `Data Clean Room v1-to-v2 Migration Tool`.
5. Select a **Warehouse** and set the database/schema to `DCR_SNOWVA.MIGRATION`.
6. Paste the contents of `streamlit_app.py` into the editor.
7. Click **Create**.

### 3. Open the App

1. After creating, select the Streamlit app you just installed.
2. The app runs under `SAMOOHA_APP_ROLE` by default.

> **Sharing:** By default, no other users in the account can see or use the app. The app owner can choose **"Share this app"** to grant access to other users or roles.

---

## Usage Workflow

### Phase 1: Plan

1. In the sidebar, click **All Cleanrooms** to list eligible legacy cleanrooms (both P&C and UI, shown with classification).
2. Select a cleanroom from the dropdown (or type the name/UUID manually).
3. Click **Generate Plan**.
4. Review the summary: role, template count, dataset count, and the generated SQL script.

### Phase 2: Execute Setup

1. Go to the **Execute Setup** tab.
2. Click **Run Setup**.
   - **Provider:** Registers all templates and data offerings, initializes the collaboration, and generates the join script.
   - **Consumer:** Registers consumer data offerings. REVIEW + JOIN + LINK must be run manually in a SQL Worksheet (see Finalize tab).
3. Review the execution logs (color-coded: errors in red, skipped in yellow).

### Phase 3: Finalize (Join)

1. Go to the **Finalize (Join)** tab.
2. Copy the provided JOIN SQL and run it in a **Snowflake SQL Worksheet** as `SAMOOHA_APP_ROLE`.
3. Click **Check Status** to verify the collaboration reaches `JOINED`.
4. If `ReferenceUsageGrantMissing` errors appear, the UI provides ready-to-copy GRANT commands and a one-click Teardown button.

> **Note:** `SYSTEM$ACCEPT_LEGAL_TERMS` is invoked during **initialize** and **join**; it cannot run inside Streamlit or inside the migration procedure. The UI surfaces hints when **Check Status** detects related failures.

> **Note:** If JOIN fails with `ReferenceUsageGrantMissingException`, an ACCOUNTADMIN must grant `REFERENCE_USAGE` on the relevant database to the share name shown in the error. See the warning in the Finalize tab for the exact command.

### Phase 4: Validate

1. Go to the **Validate** tab.
2. Click **Run Validation Check**.
3. The tool compares artifacts in the new Collaboration against the legacy Cleanroom and reports any discrepancies with remediation hints.

### Re-migration (Recovery)

If a collaboration ends up in a bad state (`JOIN_FAILED`, etc.):

1. Go to the **Cleanup** tab and run **Teardown** to remove the collaboration.
2. Re-run **Execute** — templates and data offerings that already exist will be skipped automatically.
3. Re-do the **Join** step.

---

## Sidebar Features

| Feature | Description |
|---------|-------------|
| **All Cleanrooms** | Lists eligible legacy cleanrooms (P&C and UI) with classification labels and count (`N P&C | M UI`) |
| **Migrated DCRs** | Shows all past migrations from the `MIGRATION_JOBS` table with live collaboration status |
| **`[migrated]` Badge** | Cleanrooms that have already been migrated display a badge in the dropdown |

---

## Troubleshooting

| Error | Cause | Fix |
|-------|-------|-----|
| `Side Effects [SYSTEM$ACCEPT_LEGAL_TERMS]` | Stored Procedures / Streamlit cannot accept legal terms | Copy the SQL from the Finalize tab and run it in a SQL Worksheet |
| `ReferenceUsageGrantMissingException` | Missing `REFERENCE_USAGE` grant on database for the collaboration share | Run `GRANT REFERENCE_USAGE ON DATABASE <db> TO ROLE SAMOOHA_APP_ROLE WITH GRANT OPTION;` and `GRANT REFERENCE_USAGE ON DATABASE <db> TO SHARE <share>;` as ACCOUNTADMIN. The Streamlit app auto-generates these commands from the error details. |
| `SpecValidationError: column_type invalid` | Unrecognized join column name | The tool auto-detects common abbreviations (HEM, HPN, etc.); for others, omit `column_type` or set it manually |
| `Cleanroom not found` | Wrong name or missing role | Verify the exact legacy cleanroom name and ensure `SAMOOHA_APP_ROLE` is active |
| `No data offerings found` (Provider) | No linked datasets in legacy cleanroom | Link datasets to the legacy cleanroom before migrating |
| `No data offerings found` (Consumer) | Normal for consumer-only migrations | The tool skips data registration and proceeds to joining |
| Collaboration spec shows empty `Consumer_Account` data offerings | Provider migration cannot register the consumer’s table | Expected: consumer runs **Generate Plan** in the **consumer** account, registers their data offering, then `link_data_offering` so `my_table` appears in the spec (lookalike-style templates need both sides) |
| Parity check shows "Missing templates" | Templates registered but not found in collaboration | Check the diagnostic output; may need to teardown and re-create the collaboration |
| Validation **WARN** (platform privacy / freeform) | Legacy had `prod_sql_with_platform_privacy_*` templates | Ensure provider data offerings use `allowed_analyses: template_and_freeform_sql` (or equivalent); re-register offerings if needed |
| `already exists` with wrong version suffix | Account still has artifacts from an older tool version | This repo registers **`MIGRATION_V2`** IDs; either teardown and re-migrate or keep using the version your account already has |
| `ML Jobs are not yet supported` | Cleanroom uses compute pools / `snowflake_ml_python` | ML Jobs migration is blocked until Collaboration API support is added |
| `SPCS service_functions not supported` | Templates reference SPCS service functions | SPCS migration is blocked until Collaboration API support is added |
| `Differential Privacy` warning | DP templates detected | Migrated templates will return exact results without noise injection; confirm with data provider |
| Python UDF not in generated script | UDF missing from `LOAD_PYTHON_RECORD` | Only UDFs in `LOAD_PYTHON_RECORD` are migrated to `REGISTER_CODE_SPEC`; re-run legacy `load_python_into_cleanroom` or add the UDF manually per [custom functions](https://docs.snowflake.com/en/user-guide/cleanrooms/v2/custom-functions) |
| `SpecValidationError` on `register_code_spec` (YAML / `code_body`) | Block-scalar indentation or bad parse of stored `BODY` | The generator dedents Python from `LOAD_PYTHON_RECORD` and parses handler params (incl. type hints) so names align with `arguments`; if it still fails, compare generated YAML to [custom functions](https://docs.snowflake.com/en/user-guide/cleanrooms/v2/custom-functions) |

---

## File Structure

```
dcr_migration_tool/
├── migration-backend.sql   # Snowflake stored procedures (deploy first)
├── streamlit_app.py        # Streamlit UI (deploy to Snowsight)
├── LICENSE                 # Apache License 2.0
└── README.md               # This file
```

---

## License

Copyright (c) 2026 Snowflake Inc. All rights reserved.

Licensed under the [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0).
