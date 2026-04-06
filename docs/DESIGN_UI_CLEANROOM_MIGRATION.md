# Design: UI Clean Room → Collaboration Migration (Provider & Consumer)

## 1. Purpose

Extend the existing **Programmatic & API (P&C) clean room → Collaboration** migration so that **UI-created** Snowflake Data Clean Rooms can be migrated the same way: register artifacts (templates, optional Python code specs, data offerings) and build a Collaboration spec, on both **provider** and **consumer** accounts.

This document aligns with the [Provider API reference](https://docs.snowflake.com/en/user-guide/cleanrooms/provider) and [Consumer API reference](https://docs.snowflake.com/en/user-guide/cleanrooms/consumer).

## 2. Terminology

| Term | Meaning |
|------|--------|
| **P&C clean room** | Created and managed primarily via `provider.*` / `consumer.*` APIs with a stable **human-readable** `cleanroom_name` (e.g. `mj_api2ui`). |
| **UI clean room** | Created via the Clean Rooms UI; Snowflake often exposes a **clean room id** (`CLEANROOM_ID`) that may **match** the `CLEANROOM_NAME` column in `VIEW_CLEANROOMS` (name == id). Operators may pass **`cleanroom_id` as `cleanroom_name`** in API calls when that is the identifier the platform expects. |
| **Collaboration** | Snowflake Data Clean Rooms 2.x object: registries (`REGISTER_CODE_SPEC`, `REGISTER_TEMPLATE`, `REGISTER_DATA_OFFERING`), `collaboration.initialize`, `join`, etc. |

## 3. Current state in this repo

- **P&C path** is implemented: `VIEW_ADDED_TEMPLATES`, `LOAD_PYTHON_RECORD`, `view_provider_datasets` / consumer equivalents, `GENERATE_COLLABORATION_SPEC`, etc.
- **UI clean rooms are explicitly blocked** in `CHECK_PREREQUISITES` when `VIEW_CLEANROOMS` shows `CLEANROOM_NAME == CLEANROOM_ID` (UI-style row). That guard must be **revisited** or **replaced** with a UI-capable flow (see §7).

## 4. Discovery: `describe_cleanroom` as the hub

Per product direction, **use `cleanroom_id` as the `cleanroom_name` argument** when calling:

```sql
CALL samooha_by_snowflake_local_db.provider.describe_cleanroom($cleanroom_name);
```

**Consumer (installed clean room):**

```sql
CALL samooha_by_snowflake_local_db.consumer.describe_cleanroom($cleanroom_name);
```

**Expected content (conceptual)** — the procedure returns a **string summary** that may include:

- **Clean room name** (display vs internal id)
- **Templates** in the clean room
- **Provider datasets** (provider account)
- **Join / column / activation** policy summaries (as applicable)
- **Collaborators** (“shared with” / account locators)

**Design principle:** treat `describe_cleanroom` as **human-readable discovery and validation**, not as a structured machine-parseable source of truth. For migration generation, **continue to use typed APIs** that return tables or known shapes (see §5).

## 5. Parity matrix: legacy APIs → Collaboration steps

The same logical steps as P&C apply; **UI and P&C share the same procedure names** where both sides exist.

| Concern | Provider APIs | Consumer APIs | Collaboration target |
|--------|----------------|---------------|----------------------|
| List / resolve clean room | `provider.view_cleanrooms`, `provider.describe_cleanroom` | `consumer.view_cleanrooms`, `consumer.view_installed_cleanrooms`, `consumer.describe_cleanroom` | N/A (discovery) |
| Templates | `provider.view_added_templates`, `provider.view_template_definition` | `consumer.view_added_templates`, `consumer.view_template_definition` | `REGISTER_TEMPLATE` + `code_specs` linkage |
| Python / UDF | `LOAD_PYTHON_RECORD` (per app DB `SAMOOHA_CLEANROOM_<id>`), stage listing | Same if provider-side; consumer may have no Python | `REGISTER_CODE_SPEC` |
| Provider data | `provider.view_provider_datasets` | `consumer.view_provider_datasets` | Provider `REGISTER_DATA_OFFERING` |
| Consumer data | N/A (provider) | `consumer.view_consumer_datasets` | Consumer `REGISTER_DATA_OFFERING` + `link_data_offering` |
| Join / column policies | `provider.view_join_policy`, `provider.view_column_policy` | `consumer.view_join_policy`, `consumer.view_column_policy`, … | Column metadata in data offering YAML |
| Collaborators | `provider.view_consumers` | (install / listing procedures) | `collaboration.initialize` aliases + `data_providers` |

## 6. Provider account flow (UI clean room)

1. **Input:** User supplies **clean room id** (used as `cleanroom_name` for `describe_cleanroom` and other calls).
2. **Optional:** Call `provider.describe_cleanroom` → show in UI / logs; **do not** rely on parsing alone for codegen.
3. **Eligibility:** Same prerequisite checks as today (LAF, multi-provider, etc.), **except** remove or narrow the “UI clean room not supported” error when `name == id`.
4. **Artifact collection:** Unchanged from P&C: templates, `LOAD_PYTHON_RECORD`, datasets, policies.
5. **Script generation:** Same `EXECUTE` / `PLAN` pipeline as P&C.
6. **Collaboration:** `GENERATE_COLLABORATION_SPEC` with provider DO IDs; consumer DO IDs empty until consumer migrates.

## 7. Consumer account flow (UI clean room)

1. **Input:** Clean room **must be installed** in the consumer account (`consumer.install_cleanroom`); use `consumer.is_enabled` to confirm readiness.
2. **Discovery:** `consumer.describe_cleanroom` for summary; use `consumer.view_*` for structured data.
3. **Artifacts:** Consumer data offerings from `consumer.view_consumer_datasets` + policies; join `link_data_offering` after collaboration exists.
4. **No provider templates:** Consumer migration typically registers **consumer** data offerings and **joins** the collaboration; provider must run provider migration first (same as P&C).

## 8. Differences vs P&C (operational)

| Topic | P&C | UI |
|-------|-----|-----|
| **Identifier** | Stable name string | Often **id == name** in `VIEW_CLEANROOMS`; user may paste **UUID-style id** |
| **Blocking rule** | Currently allowed | Today **blocked** in code; must be lifted for UI path |
| **Mental model** | “API name” | “Use id as `cleanroom_name` for `describe_cleanroom`” |
| **describe_cleanroom** | Optional | **Recommended** for UX and audit trail |

## 9. Implementation phases (recommended)

### Phase A — Discovery & eligibility

- Add a **mode** or **auto-detect**: if `VIEW_CLEANROOMS` row has `NAME == ID`, classify as **UI** (informational only).
- Replace hard error with **warning** or **supported branch** in `CHECK_PREREQUISITES`.
- Optionally call `provider.describe_cleanroom` / `consumer.describe_cleanroom` and attach summary to **PLAN** JSON (for Streamlit).

### Phase B — Parity with P&C codegen

- Reuse existing `GENERATE_TEMPLATE_SPECS`, `GENERATE_DATA_OFFERING_SPECS`, `GENERATE_COLLABORATION_SPEC` with the **same** `cleanroom_name` string passed through (the id).
- Confirm `SAMOOHA_CLEANROOM_<id>` exists for Python/LOAD_PYTHON_RECORD when id is used.

### Phase C — UX polish

- Streamlit: allow **“Clean room id (UI)”** field with hint: *Use the same value as `describe_cleanroom`.*
- Show `describe_cleanroom` output in a read-only expander.

### Phase D — Validation

- Extend parity / validation checks to compare Collaboration output vs legacy `describe_cleanroom` + structured `view_*` calls.

## 10. Risks and open questions

1. **String parsing `describe_cleanroom`:** Format may change between releases; **do not** make codegen depend on parsing it.
2. **Id vs display name:** `collaboration.initialize` and registry objects use **names**; ensure the same identifier is used consistently in all `CALL`s.
3. **Python / stage paths:** UI users may upload Python via UI; same `LOAD_PYTHON_RECORD` + stage rules as P&C; edge cases if UI stores metadata differently.
4. **LAF / multi-provider:** Same as today — may remain unsupported until explicitly implemented.

## 11. References

- [Snowflake Data Clean Rooms: Provider API reference](https://docs.snowflake.com/en/user-guide/cleanrooms/provider) — `describe_cleanroom`, `view_cleanrooms`, `load_python_into_cleanroom`, templates, policies, datasets.
- [Snowflake Data Clean Rooms: Consumer API reference](https://docs.snowflake.com/en/user-guide/cleanrooms/consumer) — `install_cleanroom`, `describe_cleanroom`, `view_consumer_datasets`, `run_analysis`, policies.
- Collaboration v2 custom functions (code specs): see project README and Snowflake “Upload and use custom functions in Collaboration Clean Rooms”.

---

## 12. TL;DR

- **UI clean rooms** are the same Snowflake objects as P&C; the **identifier** is often the **clean room id**, passed as **`cleanroom_name`** to `describe_cleanroom` and other APIs.
- **Use `describe_cleanroom` for discovery and UX**, not as the sole parser for migration.
- **Remove the current “UI not supported” block** and wire UI ids through the **existing** migration pipeline; add **provider** vs **consumer** flows mirroring P&C.
- **Iterate in phases**: eligibility → shared codegen → UX → validation.
