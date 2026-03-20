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

> **Version:** 2.1.0 &nbsp;|&nbsp; **Target:** Snowflake Collaboration Hub (API v2.0) &nbsp;|&nbsp; **Source:** Legacy Provider & Consumer (P&C) Clean Rooms

---

## Overview

The DCR Migration Tool is an automated engine that upgrades legacy P&C API cleanrooms to the new Snowflake Collaboration Hub architecture. It abstracts the complexity of writing YAML specifications and API calls into a streamlined **Plan → Execute → Finalize → Validate** workflow.

| Component | Description |
|-----------|-------------|
| **Backend** | Suite of Snowflake Stored Procedures (Python) for spec generation, orchestration, and audit logging |
| **Frontend** | Streamlit App (running in Snowsight) providing a guided migration UI |

---

## Features

- **Automated Discovery** — Detects your role (Provider or Consumer) and enumerates templates, datasets, and policies from the legacy cleanroom.
- **Spec Generation** — Converts legacy SQL templates and table policies into v2.0 compliant YAML specs with literal block style for readability.
- **Smart Column Type Detection** — Recognizes common join column abbreviations (`HEM`, `HPN`, `IDFA`, etc.) and maps them to valid Snowflake `column_type` identifiers.
- **Python Cleanroom & UDF Migration** — Reads `SAMOOHA_CLEANROOM_<id>.SHARED_SCHEMA.LOAD_PYTHON_RECORD` and lists `@APP.CODE/V1_0P1`. Builds a Collaboration **code_spec** ([custom functions](https://docs.snowflake.com/en/user-guide/cleanrooms/v2/custom-functions)) via `REGISTER_CODE_SPEC`, rewrites template SQL from `cleanroom.<udf>(` / Jinja `default('<udf>')` to `cleanroom.<migrated_py_spec>$<udf>(`, and registers templates with `code_specs` linkage. Requires Snowflake Data Clean Rooms 12.9+ where custom functions are enabled.
- **Safety Guardrails** — Pre-flight checks block unsupported configurations (multi-provider, LAF, UI-created cleanrooms).
- **Deterministic Versioning** — Provider and Consumer generate matching artifact IDs without manual coordination.
- **Parity Validation** — Compares the new Collaboration against the legacy Cleanroom to verify template and data offering coverage.
- **Audit Logging** — Every migration run is logged to `MIGRATION_JOBS` with job ID, timestamps, status, and details.
- **Migration History** — "Migrated DCRs" view shows all past migrations with live collaboration status and job metadata.
- **Re-migration Support** — Teardown a failed collaboration and re-run; templates and data offerings are safely skipped if already registered.

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

1. In the sidebar, click **P&C Cleanrooms** to list eligible legacy cleanrooms.
2. Select a cleanroom from the dropdown (or type the name manually).
3. Click **Generate Plan**.
4. Review the summary: role, template count, dataset count, and the generated SQL script.

### Phase 2: Execute Setup

1. Go to the **Execute Setup** tab.
2. Click **Execute Setup Now**.
   - **Provider:** Registers all templates and data offerings, then generates the collaboration spec.
   - **Consumer:** Registers local datasets and data offerings required for the cleanroom.
3. Review the generated script and copy it to a Snowflake Worksheet if manual execution is needed.

### Phase 3: Finalize (Join)

1. Go to the **Finalize (Join)** tab.
2. Click **Check Status** until the collaboration status returns `CREATED` or `INVITED`.
3. Copy the provided JOIN SQL and run it in a **Snowflake SQL Worksheet**.

> **Note:** The JOIN command requires `SYSTEM$ACCEPT_LEGAL_TERMS` which cannot execute from within Streamlit. The UI provides a ready-to-paste SQL snippet.

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
| **P&C Cleanrooms** | Lists eligible legacy cleanrooms; filters out UI-created and internal UUID rooms |
| **Migrated DCRs** | Shows all past migrations from the `MIGRATION_JOBS` table with live collaboration status |
| **`[migrated]` Badge** | P&C cleanrooms that have already been migrated display a badge in the dropdown |

---

## Troubleshooting

| Error | Cause | Fix |
|-------|-------|-----|
| `Side Effects [SYSTEM$ACCEPT_LEGAL_TERMS]` | Stored Procedures / Streamlit cannot accept legal terms | Copy the SQL from the Finalize tab and run it in a SQL Worksheet |
| `ReferenceUsageGrantMissingException` | Missing `REFERENCE_USAGE` grant on database for the collaboration share | Run `GRANT REFERENCE_USAGE ON DATABASE <db> TO SHARE <share>;` as ACCOUNTADMIN |
| `SpecValidationError: column_type invalid` | Unrecognized join column name | The tool auto-detects common abbreviations (HEM, HPN, etc.); for others, omit `column_type` or set it manually |
| `Cleanroom not found` | Wrong name or missing role | Verify the exact legacy cleanroom name and ensure `SAMOOHA_APP_ROLE` is active |
| `No data offerings found` (Provider) | No linked datasets in legacy cleanroom | Link datasets to the legacy cleanroom before migrating |
| `No data offerings found` (Consumer) | Normal for consumer-only migrations | The tool skips data registration and proceeds to joining |
| Collaboration spec shows empty `Consumer_Account` data offerings | Provider migration cannot register the consumer’s table | Expected: consumer runs **Generate Plan** in the **consumer** account, registers their data offering, then `link_data_offering` so `my_table` appears in the spec (lookalike-style templates need both sides) |
| Parity check shows "Missing templates" | Templates registered but not found in collaboration | Check the diagnostic output; may need to teardown and re-create the collaboration |
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
