

USE ROLE SAMOOHA_APP_ROLE;
CREATE DATABASE IF NOT EXISTS DCR_SNOWVA;
CREATE SCHEMA IF NOT EXISTS DCR_SNOWVA.MIGRATION;

CREATE OR REPLACE TABLE DCR_SNOWVA.MIGRATION.MIGRATION_JOBS (
  JOB_ID STRING,
  CLEANROOM_NAME STRING,
  ACTION STRING,
  ROLE STRING,
  STARTED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  FINISHED_AT TIMESTAMP_NTZ,
  STATUS STRING,
  DETAILS VARIANT
);

CREATE OR REPLACE PROCEDURE DCR_SNOWVA.MIGRATION.CHECK_PREREQUISITES(CLEANROOM_NAME STRING)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('snowflake-snowpark-python', 'pandas')
HANDLER = 'check_prereqs'
EXECUTE AS CALLER
AS
$$
def check_prereqs(session, cleanroom_name):
    errors = []
    warnings = []

    found_as_provider = False
    found_as_consumer = False
    target_uuid = None
    is_ui_cleanroom = False
    cleanroom_type = "UNKNOWN"

    # 1. Provider-side discovery and classification
    try:
        p_res = session.sql("CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.PROVIDER.VIEW_CLEANROOMS()").collect()
        for r in p_res:
            d = {k.upper(): v for k, v in r.as_dict().items()}
            c_name = d.get('CLEANROOM_NAME') or d.get('NAME')
            c_id = d.get('CLEANROOM_ID') or d.get('ID')

            name_match = c_name and c_name.upper().replace(' ', '_') == cleanroom_name.upper().replace(' ', '_')
            id_match = c_id and c_id.upper().replace(' ', '_') == cleanroom_name.upper().replace(' ', '_')

            if name_match or id_match:
                found_as_provider = True
                target_uuid = c_id

                if str(c_name).upper().replace(' ', '_') != str(c_id).upper().replace(' ', '_'):
                    is_ui_cleanroom = True

                break
    except Exception as e:
        warnings.append(f"Could not list provider cleanrooms: {str(e)[:200]}")

    # 1b. Detect freeform SQL sub-type
    if target_uuid:
        has_ui_freeform = False
        has_pc_freeform = False
        try:
            sql_tables = session.sql(f"SELECT COUNT(*) AS CNT FROM SAMOOHA_CLEANROOM_{target_uuid}.SHARED_SCHEMA.TABLES_ENABLED_FOR_SQL").collect()
            if sql_tables and sql_tables[0]['CNT'] > 0:
                has_ui_freeform = True
        except:
            pass
        try:
            wf = session.sql(f"SELECT COUNT(*) AS CNT FROM SAMOOHA_CLEANROOM_{target_uuid}.SHARED_SCHEMA.WORKFLOWS_ENABLED WHERE WORKFLOW_NAME = 'freeform_sql'").collect()
            if wf and wf[0]['CNT'] > 0:
                has_pc_freeform = True
        except:
            pass

        if is_ui_cleanroom:
            cleanroom_type = "UI_FREEFORM_SQL" if has_ui_freeform else "UI_TEMPLATE_ONLY"
        else:
            cleanroom_type = "PC_FREEFORM_SQL" if has_pc_freeform else "PC_TEMPLATE_ONLY"

        warnings.append(f"Cleanroom classified as: {cleanroom_type}")

    # 2. Consumer-side discovery
    if not found_as_provider:
        try:
            is_cons = session.call("SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.CONSUMER.IS_ENABLED", cleanroom_name)
            if is_cons:
                found_as_consumer = True
        except:
            pass

    # 3. Not found at all
    if not found_as_provider and not found_as_consumer:
        errors.append(f"Cleanroom '{cleanroom_name}' was not found as either a provider or consumer cleanroom. Please verify the cleanroom name is correct (use the exact P&C API name, not a UUID).")
        return {"status": "FAIL", "errors": errors, "warnings": warnings}

    # 4. Provider-side deep checks (multi-provider)
    if target_uuid:
        try:
            mp_check = session.sql(f"SHOW TABLES LIKE 'APPROVED_MULTIPROVIDER_CLEANROOMS' IN SCHEMA SAMOOHA_CLEANROOM_{target_uuid}.ADMIN").collect()
            if len(mp_check) > 0:
                rows = session.sql(f"SELECT COUNT(*) as CNT FROM SAMOOHA_CLEANROOM_{target_uuid}.ADMIN.APPROVED_MULTIPROVIDER_CLEANROOMS").collect()
                if rows and rows[0]['CNT'] > 0:
                    errors.append("Multi-provider cleanroom migration is not supported.")
        except:
            pass

    # 6. ML Jobs / SPCS detection (not supported in Collaboration API)
    if target_uuid:
        has_ml_jobs = False
        has_spcs = False
        dp_templates = []
        try:
            py_recs = session.sql(f"""
                SELECT FUNCTION_NAME, ADDITIONAL_PARAMS
                FROM SAMOOHA_CLEANROOM_{target_uuid}.SHARED_SCHEMA.LOAD_PYTHON_RECORD
            """).collect()
            for rec in py_recs:
                rd = {k.upper(): v for k, v in rec.as_dict().items()}
                params_str = str(rd.get('ADDITIONAL_PARAMS', '') or '')
                if 'compute_pool' in params_str.lower():
                    has_ml_jobs = True
                if 'snowflake_ml_python' in params_str.lower() or 'snowflake_ml-' in params_str.lower():
                    has_ml_jobs = True
        except:
            pass
        try:
            tmps = session.sql(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.PROVIDER.VIEW_ADDED_TEMPLATES('{cleanroom_name}')").collect()
            for t in tmps:
                td = {k.upper(): v for k, v in t.as_dict().items()}
                template_body = str(td.get('TEMPLATE', ''))
                if 'service_functions.' in template_body.lower() or 'service_function.' in template_body.lower():
                    has_spcs = True
                body_lower = template_body.lower()
                if 'addnoise' in body_lower or 'add_noise' in body_lower or 'privacy.epsilon' in body_lower or 'dp_noise' in body_lower or 'laplace' in body_lower:
                    dp_templates.append(td.get('TEMPLATE_NAME', 'unknown'))
        except:
            pass
        if has_ml_jobs:
            errors.append("This cleanroom uses ML Jobs (load_ml_jobs_code_to_cleanroom). ML Jobs are not yet supported in the Collaboration API. Migration is blocked until ML Jobs support is added.")
        if has_spcs:
            errors.append("This cleanroom uses SPCS (Snowpark Container Services). Templates reference 'service_functions.' which is not supported in the Collaboration API. Migration is blocked until SPCS support is added.")
        if dp_templates:
            warnings.append(
                f"Differential Privacy is enabled on {len(dp_templates)} template(s): "
                f"{', '.join(dp_templates[:5])}. "
                f"DP is NOT supported in the Collaboration API. Migrated templates will return exact results without noise injection. "
                f"This is a privacy downgrade — confirm with the data provider before proceeding."
            )

    if errors:
        return {"status": "FAIL", "errors": errors, "warnings": warnings, "cleanroom_type": cleanroom_type}
    result = {"status": "PASS", "cleanroom_type": cleanroom_type, "is_ui_cleanroom": is_ui_cleanroom, "target_uuid": target_uuid}
    if warnings:
        result["warnings"] = warnings
    return result
$$;

CREATE OR REPLACE PROCEDURE DCR_SNOWVA.MIGRATION.PREVIEW(CLEANROOM_NAME STRING)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('snowflake-snowpark-python', 'pandas')
HANDLER = 'preview'
EXECUTE AS CALLER
AS
$$
import pandas as pd

def preview(session, cleanroom_name):
    result = {"cleanroom_name": cleanroom_name, "role": "UNKNOWN", "cleanroom_type": "UNKNOWN", "datasets": [], "templates": [], "policies": {"join": [], "column": [], "activation": []}, "consumers": [], "freeform_sql": {}, "aggregation_policies": [], "errors": []}
    
    def fetch_df(query):
        try:
            res = session.sql(query).collect()
            if not res: return pd.DataFrame()
            return pd.DataFrame([{k.upper(): v for k, v in r.as_dict().items()} for r in res])
        except Exception as e:
            return pd.DataFrame()

    # --- ROLE DETECTION (supports both P&C and UI cleanrooms) ---
    is_provider = False
    is_ui_cleanroom = False
    target_uuid = None
    try:
        p_res = session.sql("CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.PROVIDER.VIEW_CLEANROOMS()").collect()
        for r in p_res:
            d = {k.upper(): v for k, v in r.as_dict().items()}
            c_name = d.get('CLEANROOM_NAME') or d.get('NAME')
            c_id = d.get('CLEANROOM_ID') or d.get('ID')
            c_state = d.get('STATE') or d.get('STATUS')
            
            name_match = c_name and c_name.upper().replace(' ', '_') == cleanroom_name.upper().replace(' ', '_')
            id_match = c_id and c_id.upper().replace(' ', '_') == cleanroom_name.upper().replace(' ', '_')
            if name_match or id_match:
                target_uuid = c_id
                if str(c_name).upper().replace(' ', '_') != str(c_id).upper().replace(' ', '_'):
                    is_ui_cleanroom = True
                if c_state == 'CREATED':
                    is_provider = True
                break
    except: pass

    is_consumer = False
    if not is_provider:
        api_cleanroom_name_tmp = target_uuid if (is_ui_cleanroom and target_uuid) else cleanroom_name
        try:
            is_consumer = session.call("SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.CONSUMER.IS_ENABLED", api_cleanroom_name_tmp)
        except:
            try:
                is_consumer = session.call("SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.CONSUMER.IS_ENABLED", cleanroom_name)
            except: pass

    if not is_provider and not is_consumer:
        result["errors"].append("Cleanroom not found or not installed.")
        return result
    
    result["role"] = "PROVIDER" if is_provider else "CONSUMER"
    result["is_ui_cleanroom"] = is_ui_cleanroom
    result["target_uuid"] = target_uuid
    api_cleanroom_name = target_uuid if (is_ui_cleanroom and target_uuid) else cleanroom_name

    # --- CLASSIFY FREEFORM SQL TYPE ---
    has_ui_freeform = False
    has_pc_freeform = False
    if target_uuid:
        try:
            sql_t = session.sql(f"SELECT COUNT(*) AS CNT FROM SAMOOHA_CLEANROOM_{target_uuid}.SHARED_SCHEMA.TABLES_ENABLED_FOR_SQL").collect()
            if sql_t and sql_t[0]['CNT'] > 0:
                has_ui_freeform = True
                sql_tables_df = fetch_df(f"SELECT * FROM SAMOOHA_CLEANROOM_{target_uuid}.SHARED_SCHEMA.TABLES_ENABLED_FOR_SQL")
                if not sql_tables_df.empty:
                    result["freeform_sql"]["type"] = "UI_FREEFORM_SQL"
                    result["freeform_sql"]["tables"] = sql_tables_df.to_dict(orient='records')
        except: pass
        try:
            wf = session.sql(f"SELECT COUNT(*) AS CNT FROM SAMOOHA_CLEANROOM_{target_uuid}.SHARED_SCHEMA.WORKFLOWS_ENABLED WHERE WORKFLOW_NAME = 'freeform_sql'").collect()
            if wf and wf[0]['CNT'] > 0:
                has_pc_freeform = True
                wf_tables_df = fetch_df(f"SELECT * FROM SAMOOHA_CLEANROOM_{target_uuid}.SHARED_SCHEMA.WORKFLOWS_ENABLED_TABLES WHERE WORKFLOW_NAME = 'freeform_sql'")
                wf_consumers_df = fetch_df(f"SELECT * FROM SAMOOHA_CLEANROOM_{target_uuid}.SHARED_SCHEMA.WORKFLOWS_ENABLED WHERE WORKFLOW_NAME = 'freeform_sql'")
                result["freeform_sql"]["type"] = "PC_FREEFORM_SQL"
                if not wf_tables_df.empty:
                    result["freeform_sql"]["tables"] = wf_tables_df.to_dict(orient='records')
                if not wf_consumers_df.empty:
                    result["freeform_sql"]["consumers"] = wf_consumers_df.to_dict(orient='records')
        except: pass

    if is_ui_cleanroom:
        result["cleanroom_type"] = "UI_FREEFORM_SQL" if has_ui_freeform else "UI_TEMPLATE_ONLY"
    else:
        result["cleanroom_type"] = "PC_FREEFORM_SQL" if has_pc_freeform else "PC_TEMPLATE_ONLY"

    # --- EXTRACT AGGREGATION POLICIES (for freeform SQL cleanrooms) ---
    if target_uuid and (has_ui_freeform or has_pc_freeform):
        try:
            import re
            agg_policies = session.sql(f"SHOW AGGREGATION POLICIES IN SCHEMA SAMOOHA_CLEANROOM_{target_uuid}.SHARED_SCHEMA").collect()
            for ap in agg_policies:
                ap_d = {k.upper(): v for k, v in ap.as_dict().items()}
                policy_name = ap_d.get('NAME', '')
                try:
                    desc = session.sql(f"DESCRIBE AGGREGATION POLICY SAMOOHA_CLEANROOM_{target_uuid}.SHARED_SCHEMA.{policy_name}").collect()
                    if desc:
                        body = str({k.upper(): v for k, v in desc[0].as_dict().items()}.get('BODY', ''))
                        m = re.search(r'min_row_count\s*=>\s*(\d+)', body)
                        threshold = int(m.group(1)) if m else None
                        result["aggregation_policies"].append({"policy_name": policy_name, "body": body, "threshold": threshold})
                except: pass
        except: pass

        if has_pc_freeform:
            try:
                for tbl_rec in result.get("freeform_sql", {}).get("tables", []):
                    table_fqn = tbl_rec.get("TABLE_NAME", "")
                    if table_fqn:
                        parts = table_fqn.split('.')
                        if len(parts) >= 2:
                            src_db_sch = f"{parts[0]}.{parts[1]}" if len(parts) >= 2 else parts[0]
                            pol_refs = fetch_df(f"SELECT * FROM TABLE({parts[0]}.INFORMATION_SCHEMA.POLICY_REFERENCES(REF_ENTITY_NAME => '{table_fqn}', REF_ENTITY_DOMAIN => 'TABLE'))")
                            if not pol_refs.empty:
                                tbl_rec["source_policies"] = pol_refs.to_dict(orient='records')
            except: pass

    try:
        if is_provider:
            prov_ds = fetch_df(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.PROVIDER.view_provider_datasets('{api_cleanroom_name}')")
            if not prov_ds.empty:
                result["datasets"] = prov_ds['TABLE_NAME'].tolist()
            
            cons_df = fetch_df(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.PROVIDER.VIEW_CONSUMERS('{api_cleanroom_name}')")
            if not cons_df.empty:
                c_col = 'CONSUMER_ACCOUNT_NAME' if 'CONSUMER_ACCOUNT_NAME' in cons_df.columns else 'CONSUMER_NAME'
                if c_col in cons_df.columns: result["consumers"] = cons_df[c_col].tolist()
            
            tmps = fetch_df(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.PROVIDER.VIEW_ADDED_TEMPLATES('{api_cleanroom_name}')")
            if not tmps.empty: result["templates"] = tmps['TEMPLATE_NAME'].tolist()

            cr_record = fetch_df(f"SELECT * FROM SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.PUBLIC.CLEANROOM_RECORD WHERE UPPER(CLEANROOM_NAME) = '{api_cleanroom_name.upper()}' OR UPPER(CLEANROOM_ID) = '{api_cleanroom_name.upper()}'")
            if not cr_record.empty:
                uuid_col = 'CLEANROOM_ID' if 'CLEANROOM_ID' in cr_record.columns else 'ID'
                uuid = cr_record.iloc[0][uuid_col]
                
                jp = fetch_df(f"SELECT * FROM SAMOOHA_CLEANROOM_{uuid}.SHARED_SCHEMA.JOIN_COLUMNS")
                if not jp.empty: result["policies"]["join"] = jp.to_dict(orient='records')

                cp = fetch_df(f"SELECT * FROM SAMOOHA_CLEANROOM_{uuid}.SHARED_SCHEMA.POLICY_COLUMNS")
                if not cp.empty: result["policies"]["column"] = cp.to_dict(orient='records')
                
                try:
                    ap = fetch_df(f"SELECT * FROM SAMOOHA_CLEANROOM_{uuid}.SHARED_SCHEMA.ACTIVATION_COLUMNS")
                    if not ap.empty: result["policies"]["activation"] = ap.to_dict(orient='records')
                except: pass

        elif is_consumer:
            cons_ds = fetch_df(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.CONSUMER.view_consumer_datasets('{api_cleanroom_name}')")
            if not cons_ds.empty:
                t_col = None
                for candidate in ['LINKED_TABLE', 'TABLE_NAME', 'DATASET_NAME', 'OBJECT_NAME', 'NAME', 'VIEW_NAME']:
                    if candidate in cons_ds.columns:
                        t_col = candidate
                        break
                if not t_col:
                    for c in cons_ds.columns:
                        if 'TABLE' in c or 'DATASET' in c or 'OBJECT' in c:
                            t_col = c
                            break
                if t_col:
                    result["datasets"] = cons_ds[t_col].tolist()
                result["consumer_dataset_columns"] = list(cons_ds.columns)

            try:
                added_tmps = fetch_df(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.CONSUMER.VIEW_ADDED_TEMPLATES('{api_cleanroom_name}')")
                if not added_tmps.empty:
                    t_col = 'TEMPLATE_NAME' if 'TEMPLATE_NAME' in added_tmps.columns else ('NAME' if 'NAME' in added_tmps.columns else None)
                    if t_col:
                        result["templates"] = added_tmps[t_col].tolist()
            except: pass

            if not result.get("templates"):
                reqs = fetch_df(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.CONSUMER.LIST_TEMPLATE_REQUESTS('{api_cleanroom_name}')")
                if not reqs.empty: result["templates"] = reqs['TEMPLATE_NAME'].tolist() if 'TEMPLATE_NAME' in reqs.columns else []

            try:
                jp = fetch_df(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.CONSUMER.view_join_policy('{api_cleanroom_name}')")
                if not jp.empty:
                    result["policies"]["join"] = jp.to_dict(orient='records')
                    result["join_policy_columns"] = list(jp.columns)
            except: pass

            try:
                cp = fetch_df(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.CONSUMER.view_column_policy('{api_cleanroom_name}')")
                if not cp.empty:
                    result["policies"]["column"] = cp.to_dict(orient='records')
                    result["column_policy_columns"] = list(cp.columns)
            except: pass

            try:
                ap = fetch_df(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.CONSUMER.view_activation_policy('{api_cleanroom_name}')")

                if not ap.empty: result["policies"]["activation"] = ap.to_dict(orient='records')
            except: pass

    except Exception as e:
        result["errors"].append(str(e))

    return result
$$;

CREATE OR REPLACE PROCEDURE DCR_SNOWVA.MIGRATION.GENERATE_TEMPLATE_SPECS(CLEANROOM_NAME STRING)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('snowflake-snowpark-python', 'pandas', 'pyyaml')
HANDLER = 'gen_templates'
EXECUTE AS CALLER
AS
$$
import yaml
import pandas as pd
import re
import textwrap
from datetime import datetime
import json

class LiteralBlockDumper(yaml.SafeDumper):
    pass

def _literal_str_representer(dumper, data):
    if '\n' in data:
        return dumper.represent_scalar('tag:yaml.org,2002:str', data, style='|')
    return dumper.represent_scalar('tag:yaml.org,2002:str', data)

LiteralBlockDumper.add_representer(str, _literal_str_representer)

def _sql_type_from_legacy(s):
    if not s:
        return 'VARIANT'
    s = str(s).strip().lower()
    mp = {'variant': 'VARIANT', 'string': 'STRING', 'varchar': 'STRING', 'float': 'FLOAT',
          'number': 'NUMBER', 'integer': 'INTEGER', 'int': 'INTEGER', 'boolean': 'BOOLEAN', 'object': 'OBJECT'}
    return mp.get(s, s.upper()[:64] if len(str(s)) < 64 else 'VARIANT')

def _split_py_params(param_str):
    """Split a Python parameter list on commas not inside ()[]{}."""
    if not param_str or not str(param_str).strip():
        return []
    depth = 0
    cur = []
    parts = []
    for c in param_str:
        if c in '([{':
            depth += 1
        elif c in ')]}':
            depth -= 1
        elif c == ',' and depth == 0:
            parts.append(''.join(cur).strip())
            cur = []
            continue
        cur.append(c)
    if cur:
        parts.append(''.join(cur).strip())
    return [p for p in parts if p]


def _infer_py_arg_names(body, handler):
    """Match YAML argument names to the Python handler signature (v2 docs / metadata).

    Only module-level `def handler(...)` counts — avoids class methods like `def train(self, X, y)`.
    """
    if not body or not handler:
        return None
    pat = re.compile(
        r'^(\s*)def\s+' + re.escape(str(handler)) + r'\s*\(([^)]*)\)',
        re.MULTILINE | re.IGNORECASE | re.DOTALL,
    )
    raw = None
    for m in pat.finditer(body):
        if len(m.group(1)) > 0:
            continue
        raw = m.group(2).strip()
        break
    if raw is None:
        return None
    if not raw:
        return []
    parts = []
    for a in _split_py_params(raw):
        a = a.strip()
        if not a or a.startswith('*'):
            continue
        pre_default = a.split('=', 1)[0].strip()
        name = pre_default.split(':', 1)[0].strip()
        if not name or name in ('self', 'cls', 'session'):
            continue
        parts.append(name)
    return parts if parts else None


def _normalize_code_body_for_yaml(body):
    """Dedent stored Python before YAML dump (helps consistent block layout)."""
    if not body:
        return '\n'
    b = str(body).replace('\r\n', '\n').replace('\r', '\n').rstrip() + '\n'
    try:
        b = textwrap.dedent(b)
    except Exception:
        pass
    return b


def _strip_code_body_indentation_indicators(yaml_text):
    """PyYAML often emits 'code_body: |2' (explicit indent); Snowflake docs show plain '|'.

    The digit is only PyYAML's indent hint — removing it yields standard literal '|' / '|-' / '|+'
    while keeping the same following lines, which YAML 1.1 parses the same way.
    """
    if not yaml_text:
        return yaml_text

    def _repl(m):
        return m.group(1) + '|' + (m.group(3) or '')

    return re.sub(r'(^[ \t]*code_body: )\|(\d*)([-+]?)[ \t]*$', _repl, yaml_text, flags=re.MULTILINE)


def _coerce_imports_list(v):
    if v is None:
        return []
    if isinstance(v, list):
        return [str(x).strip() for x in v if x is not None and str(x).strip()]
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return []
        if s.startswith('['):
            try:
                return _coerce_imports_list(json.loads(s))
            except Exception:
                return []
        if ',' in s:
            return [x.strip() for x in s.split(',') if x.strip()]
        return [s]
    return []


def _normalize_import_path_fragment(s):
    if s is None:
        return None
    t = str(s).strip().strip('"').strip("'")
    while len(t) >= 2 and ((t[0] == t[-1] == '"') or (t[0] == t[-1] == "'")):
        t = t[1:-1].strip()
    if not t or t in ('""', "''", 'null', 'None'):
        return None
    return t


def _imports_look_like_real_stage_paths(items):
    """Ignore placeholder/junk imports (e.g. empty strings) that Samooha may store for inline UDFs."""
    if not items:
        return False
    for s in items:
        if s.startswith('@'):
            return True
        if '/' in s:
            return True
        low = s.lower()
        if low.endswith('.py') or low.endswith('.whl'):
            return True
    return False


def _extract_imports_from_row(row):
    """P&C stage overload stores imports in ADDITIONAL_PARAMS.imports; inline UDFs may leave noise — normalize and drop empties."""
    parsed = _coerce_imports_list(row.get('IMPORTS'))
    if not parsed:
        ap = row.get('ADDITIONAL_PARAMS')
        if ap:
            try:
                d = json.loads(ap) if isinstance(ap, str) else ap
                if isinstance(d, dict):
                    parsed = _coerce_imports_list(d.get('imports'))
            except Exception:
                parsed = []
    out = []
    for x in (parsed or []):
        n = _normalize_import_path_fragment(x)
        if n:
            out.append(n)
    return out


def _legacy_arg_names_from_types(types):
    names = []
    for t in types:
        s = str(t).strip()
        if not s:
            continue
        tok = s.split(None, 1)
        if tok:
            names.append(tok[0])
    return names


def _detect_code_patch_prefix(stage_files):
    if not stage_files:
        return 'V1_0P1'
    counts = {}
    for sf in stage_files:
        parts = str(sf).split('/')
        if parts and re.match(r'^V\d+_\d+P\d+$', parts[0]):
            counts[parts[0]] = counts.get(parts[0], 0) + 1
    if counts:
        return max(counts.items(), key=lambda x: x[1])[0]
    return 'V1_0P1'


def _import_path_to_stage_fqn(cleanroom_uuid, rel_path, patch_prefix):
    rel = str(rel_path).strip()
    if rel.startswith('@'):
        return rel
    rel = rel.lstrip('/')
    return '@DCR_SNOWVA.MIGRATION.CODE_ARTIFACTS/%s/%s' % (cleanroom_uuid, rel)


def _copy_stage_artifacts(session, cleanroom_uuid, imports_list, patch_prefix):
    try:
        session.sql("CREATE STAGE IF NOT EXISTS DCR_SNOWVA.MIGRATION.CODE_ARTIFACTS DIRECTORY = (ENABLE = TRUE)").collect()
    except:
        try:
            session.sql("CREATE STAGE IF NOT EXISTS DCR_SNOWVA.MIGRATION.CODE_ARTIFACTS").collect()
            session.sql("ALTER STAGE DCR_SNOWVA.MIGRATION.CODE_ARTIFACTS SET DIRECTORY = (ENABLE = TRUE)").collect()
        except:
            pass
    copied = []
    for rel in imports_list:
        rel = str(rel).strip().lstrip('/')
        src = '@SAMOOHA_CLEANROOM_%s.APP.CODE/%s/%s' % (cleanroom_uuid, patch_prefix, rel)
        dst = '@DCR_SNOWVA.MIGRATION.CODE_ARTIFACTS/%s/' % cleanroom_uuid
        try:
            session.sql(f"COPY FILES INTO {dst} FROM {src}").collect()
            copied.append(rel)
        except:
            pass
    if copied:
        try:
            session.sql("ALTER STAGE DCR_SNOWVA.MIGRATION.CODE_ARTIFACTS REFRESH").collect()
        except:
            pass
    return copied


def _fetch_load_python_and_stage(session, cleanroom_name, is_provider):
    if not is_provider:
        return [], [], None
    uuid = None
    try:
        p_res = session.sql("CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.PROVIDER.VIEW_CLEANROOMS()").collect()
        for r in p_res:
            d = {k.upper(): v for k, v in r.as_dict().items()}
            c_name = d.get('CLEANROOM_NAME') or d.get('NAME')
            c_id = d.get('CLEANROOM_ID') or d.get('ID')
            name_match = c_name and str(c_name).upper().replace(' ', '_') == cleanroom_name.upper().replace(' ', '_')
            id_match = c_id and str(c_id).upper().replace(' ', '_') == cleanroom_name.upper().replace(' ', '_')
            if name_match or id_match:
                uuid = c_id
                break
    except:
        pass
    rows = []
    if uuid:
        try:
            q = f"""SELECT FUNCTION_NAME, ARGUMENT_TYPES, BODY, ADDITIONAL_PARAMS,
                TRY_PARSE_JSON(ADDITIONAL_PARAMS):handler::STRING AS HANDLER,
                TRY_PARSE_JSON(ADDITIONAL_PARAMS):packages::STRING AS PACKAGES,
                TRY_PARSE_JSON(ADDITIONAL_PARAMS):rettype::STRING AS RETURN_TYPE,
                TRY_PARSE_JSON(ADDITIONAL_PARAMS):imports AS IMPORTS
            FROM SAMOOHA_CLEANROOM_{uuid}.SHARED_SCHEMA.LOAD_PYTHON_RECORD"""
            for r in session.sql(q).collect():
                rows.append({k.upper(): v for k, v in r.as_dict().items()})
        except:
            pass
    stage_files = []
    if uuid:
        try:
            for row in session.sql(f"ls @SAMOOHA_CLEANROOM_{uuid}.APP.CODE/V1_0P1").collect():
                rd = row.as_dict() if hasattr(row, 'as_dict') else {}
                nm = rd.get('name') or rd.get('NAME') or (row[0] if len(row) > 0 else None)
                if nm:
                    stage_files.append(str(nm))
        except:
            pass
        if rows and stage_files:
            all_imports = []
            for r in rows:
                all_imports.extend(_extract_imports_from_row(r))
            if all_imports and _imports_look_like_real_stage_paths(all_imports):
                patch_prefix = _detect_code_patch_prefix(stage_files)
                _copy_stage_artifacts(session, uuid, all_imports, patch_prefix)
    return rows, stage_files, uuid

def _build_python_code_spec_yaml(cleanroom_name, py_rows, code_spec_name, ver_str, cleanroom_uuid=None, stage_files=None):
    """Inline BODY -> code_body; stage load_python (imports[]) -> Collaboration artifacts + function imports (v2 custom functions)."""
    patch_prefix = _detect_code_patch_prefix(stage_files or [])
    path_to_alias = {}
    artifact_specs = []
    functions = []

    def _ensure_artifact(stage_path):
        if stage_path in path_to_alias:
            return path_to_alias[stage_path]
        alias = 'artifact_%d' % len(path_to_alias)
        path_to_alias[stage_path] = alias
        artifact_specs.append({'alias': alias, 'stage_path': stage_path})
        return alias

    for row in py_rows:
        fname = row.get('FUNCTION_NAME')
        if not fname:
            continue
        handler = row.get('HANDLER') or str(fname).split('_')[-1]
        arg_types_str = row.get('ARGUMENT_TYPES') or ''
        types = [x.strip() for x in str(arg_types_str).split(',') if x.strip()]
        hinfer = str(handler).rsplit('.', 1)[-1] if '.' in str(handler) else str(handler)
        names = _infer_py_arg_names(row.get('BODY') or '', hinfer)
        if not names or len(names) != len(types):
            leg = _legacy_arg_names_from_types(types)
            if leg and len(leg) == len(types):
                names = leg
            else:
                names = ['arg_%d' % i for i in range(len(types))]
        if len(names) != len(set(names)):
            names = ['arg_%d' % i for i in range(len(names))]
        arguments = [{'name': names[i], 'type': _sql_type_from_legacy(types[i])} for i in range(len(types))]
        pkg_raw = row.get('PACKAGES') or ''
        packages = [p.strip() for p in str(pkg_raw).replace(';', ',').split(',') if p.strip()]
        returns = _sql_type_from_legacy(row.get('RETURN_TYPE'))
        body_raw = row.get('BODY') or ''
        body = _normalize_code_body_for_yaml(body_raw)
        body_nonempty = bool(str(body_raw).strip())
        imports_list = _extract_imports_from_row(row)
        imports_ok = _imports_look_like_real_stage_paths(imports_list)
        # Inline load_python always has BODY; prefer code_body. Stage overload has no body + real paths like /file.py.
        use_stage = bool(cleanroom_uuid) and imports_ok and (not body_nonempty)

        fn_entry = {
            'name': str(fname),
            'type': 'UDF',
            'language': 'PYTHON',
            'runtime_version': '3.10',
            'handler': str(handler),
            'arguments': arguments,
            'returns': returns,
            'packages': packages,
        }

        if use_stage:
            fn_aliases = []
            for rel in imports_list:
                sp = _import_path_to_stage_fqn(cleanroom_uuid, rel, patch_prefix)
                fn_aliases.append(_ensure_artifact(sp))
            fn_entry['imports'] = fn_aliases
        else:
            if not body_nonempty:
                continue
            fixed_body = re.sub(
                r'(def\s+' + re.escape(str(handler).rsplit('.', 1)[-1]) + r'\s*\(\s*)session\s*,\s*',
                r'\1',
                body
            )
            fn_entry['code_body'] = fixed_body
        functions.append(fn_entry)

    if not functions:
        return None
    spec = {
        'api_version': '2.0.0',
        'spec_type': 'code_spec',
        'name': code_spec_name,
        'version': ver_str,
        'description': 'Migrated Python UDFs from P&C cleanroom %s' % cleanroom_name,
    }
    if artifact_specs:
        spec['artifacts'] = artifact_specs
    spec['functions'] = functions
    dumped = yaml.dump(spec, Dumper=LiteralBlockDumper, default_flow_style=False, sort_keys=False)
    return _strip_code_body_indentation_indicators(dumped)

def _template_uses_python_udf(t_sql, py_fn_names):
    for fn in py_fn_names:
        if fn in t_sql:
            return True
    return False

def _rewrite_python_udf_calls(t_sql, py_fn_names, spec_base):
    for fn in sorted(py_fn_names, key=lambda x: -len(str(x))):
        if not fn:
            continue
        esc = re.escape(str(fn))
        t_sql = re.sub(
            r'cleanroom\.\{\{\s*([a-zA-Z0-9_]+)\s*\|\s*default\(\s*[\'\"]' + esc + r'[\'\"]\s*\)\s*\|\s*sqlsafe\s*\}\}',
            'cleanroom.%s${{ \\1 | default(\'%s\') | sqlsafe }}' % (spec_base, fn),
            t_sql
        )
        t_sql = re.sub(r'cleanroom\.' + esc + r'(?=\s*\()', 'cleanroom.%s$%s' % (spec_base, fn), t_sql)
    return t_sql

def gen_templates(session, cleanroom_name):
    # --- ROLE DETECTION ---
    is_provider = False
    is_ui_cleanroom = False
    target_uuid = None
    try:
        p_res = session.sql("CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.PROVIDER.VIEW_CLEANROOMS()").collect()
        for r in p_res:
            d = {k.upper(): v for k, v in r.as_dict().items()}
            c_name = d.get('CLEANROOM_NAME') or d.get('NAME')
            c_id = d.get('CLEANROOM_ID') or d.get('ID')
            c_state = d.get('STATE') or d.get('STATUS')
            name_match = c_name and c_name.upper().replace(' ', '_') == cleanroom_name.upper().replace(' ', '_')
            id_match = c_id and c_id.upper().replace(' ', '_') == cleanroom_name.upper().replace(' ', '_')
            if name_match or id_match:
                target_uuid = c_id
                if str(c_name).upper().replace(' ', '_') != str(c_id).upper().replace(' ', '_'):
                    is_ui_cleanroom = True
                if c_state == 'CREATED': is_provider = True
                break
    except: pass

    api_cleanroom_name = target_uuid if (is_ui_cleanroom and target_uuid) else cleanroom_name
    
    is_consumer = False
    if not is_provider:
        try: is_consumer = session.call("SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.CONSUMER.IS_ENABLED", api_cleanroom_name)
        except:
            try: is_consumer = session.call("SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.CONSUMER.IS_ENABLED", cleanroom_name)
            except: pass

    if not is_provider and not is_consumer:
        return {'templates': [], 'python_code_spec': None, 'python_udf_names': [], 'python_stage_files': []}

    py_rows, stage_files, _uuid = _fetch_load_python_and_stage(session, api_cleanroom_name, is_provider)
    ver_str = "MIGRATION_V2"
    safe_cn = re.sub(r'[^a-zA-Z0-9_]', '_', cleanroom_name)[:50].strip('_').lower() or 'cleanroom'
    code_spec_name = 'migrated_py_%s' % safe_cn
    py_code_yaml = _build_python_code_spec_yaml(cleanroom_name, py_rows, code_spec_name, ver_str, _uuid, stage_files) if py_rows else None
    py_fn_names = [str(r.get('FUNCTION_NAME')) for r in py_rows if r.get('FUNCTION_NAME')]
    code_spec_bundle_id = '%s_%s' % (code_spec_name, ver_str)

    df = pd.DataFrame()
    if is_provider:
        df_res = session.sql(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.PROVIDER.VIEW_ADDED_TEMPLATES('{api_cleanroom_name}')").collect()
        if df_res: df = pd.DataFrame([r.as_dict() for r in df_res])
        try:
            req_res = session.sql(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.PROVIDER.VIEW_TEMPLATE_REQUESTS('{api_cleanroom_name}')").collect()
            if req_res: 
                df2 = pd.DataFrame([r.as_dict() for r in req_res])
                df = pd.concat([df, df2], ignore_index=True)
        except: pass
    elif is_consumer:
        try:
            df_res = session.sql(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.CONSUMER.LIST_TEMPLATE_REQUESTS('{api_cleanroom_name}')").collect()
            if df_res: df = pd.DataFrame([r.as_dict() for r in df_res])
        except: pass

    if df.empty:
        return {'templates': [], 'python_code_spec': py_code_yaml, 'python_udf_names': py_fn_names,
                'python_stage_files': stage_files}
    
    norm_data = []
    for _, row in df.iterrows():
        r = {k.upper(): v for k, v in row.to_dict().items()}
        if 'TEMPLATE_NAME' not in r and 'NAME' in r:
            r['TEMPLATE_NAME'] = r['NAME']
        norm_data.append(r)

    if not norm_data: return []

    df_norm = pd.DataFrame(norm_data)
    if 'TEMPLATE_NAME' in df_norm.columns:
        df_norm = df_norm.drop_duplicates(subset=['TEMPLATE_NAME'])
    
    ver_str = "MIGRATION_V2"

    specs = []
    for _, r in df_norm.iterrows():
        t_name = r.get('TEMPLATE_NAME')
        t_sql = str(r.get('TEMPLATE') or r.get('SQL_TEXT') or '')
        
        if not t_name or not t_sql: continue

        t_classification = "CUSTOM_SQL"
        if 'prod_sql_with_platform_privacy' in str(t_name).lower():
            t_classification = "PLATFORM_PRIVACY"
        elif str(t_name).lower().startswith('activation_') or 'cleanroom.activation_' in t_sql.lower():
            t_classification = "ACTIVATION"
        elif any(fn in t_sql for fn in py_fn_names if fn):
            t_classification = "PYTHON_UDF"
        elif str(t_name).lower() in ('prod_overlap_analysis', 'prod_provider_overlap_analysis', 'prod_overlap_analysis_v2'):
            t_classification = "STANDARD"

        if t_classification == "PLATFORM_PRIVACY":
            dp_sens = r.get('DP_SENSITIVITY', 0)
            specs.append({
                'template_name': t_name,
                'classification': t_classification,
                'status': 'REQUIRES_FREEFORM_SQL_MIGRATION',
                'dp_sensitivity': dp_sens,
                'warning': 'This template uses generic_sql_query_with_aggregation_and_projection_policies which is not available in Collaboration APIs. Migrate using freeform SQL data offerings (see design doc Section 7).',
                'original_body': t_sql[:500]
            })
            continue

        if '\\n' in t_sql:
            t_sql = t_sql.replace('\\n', '\n')
        if '\\t' in t_sql:
            t_sql = t_sql.replace('\\t', '\t')

        is_activation = "cleanroom.activation_" in t_sql.lower()
        cleaned_sql = t_sql
        if not is_activation:
            match = re.search(r"CREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+.*?\s+AS\s*(.*?);", t_sql, re.IGNORECASE | re.DOTALL)
            if match: cleaned_sql = match.group(1).strip()

        source_count = len(re.findall(r'\{\{\s*source_table\[', cleaned_sql))
        my_table_refs = re.findall(r'\{\{\s*my_table\[(\d+)\]', cleaned_sql)
        for mt_idx in sorted(set(my_table_refs), key=int, reverse=True):
            new_idx = int(mt_idx) + source_count
            cleaned_sql = re.sub(
                r'\{\{\s*my_table\[' + mt_idx + r'\]',
                '{{ source_table[' + str(new_idx) + ']',
                cleaned_sql
            )

        params = []
        raw_params = r.get('PARAMETERS')
        if raw_params:
            try:
                if isinstance(raw_params, str):
                    try: params = json.loads(raw_params)
                    except: params = yaml.safe_load(raw_params)
                else: params = raw_params
                if not isinstance(params, list):
                    params = []
            except: pass

        system_vars = ['source_table', 'source_tables', 'my_table', 'my_tables']
        existing_param_names = set()
        if params:
            for p in params:
                if isinstance(p, dict) and 'name' in p:
                    existing_param_names.add(p['name'].lower())
            params = [p for p in params if isinstance(p, dict) and p.get('name', '').lower() not in system_vars]

        if cleaned_sql:
            jinja_vars = re.findall(r"\{\{\s*([a-zA-Z0-9_]+)", cleaned_sql)
            seen = set(existing_param_names)
            for v in jinja_vars:
                if v.lower() not in system_vars and v.lower() not in seen:
                    seen.add(v.lower())
                    params.append({
                        "name": v,
                        "type": "string",
                        "description": f"Auto-detected parameter: {v}",
                        "default": ""
                    })

        def normalize_param(p):
            if not isinstance(p, dict): return p
            ordered = {}
            ordered['name'] = p.get('name', '')
            ordered['type'] = p.get('type', 'string')
            ordered['description'] = p.get('description', '')
            ordered['default'] = p.get('default', '')
            for k, v in p.items():
                if k not in ordered:
                    ordered[k] = v
            return ordered

        params = [normalize_param(p) for p in params]

        tpl_work = cleaned_sql
        uses_py = bool(py_fn_names) and _template_uses_python_udf(tpl_work, py_fn_names)
        if uses_py:
            tpl_work = _rewrite_python_udf_calls(tpl_work, set(py_fn_names), code_spec_name)

        spec_dict_no_tpl = {
            'api_version': '2.0.0', 
            'spec_type': 'template', 
            'name': f"migrated_{t_name}",
            'version': ver_str, 
            'type': 'sql_activation' if is_activation else 'sql_analysis',
            'description': f"Migrated from legacy: {t_name}", 
            'parameters': params
        }
        if uses_py and py_code_yaml:
            spec_dict_no_tpl['code_specs'] = [code_spec_bundle_id]
        yaml_header = yaml.dump(spec_dict_no_tpl, Dumper=LiteralBlockDumper, default_flow_style=False, sort_keys=False)

        tpl_clean = tpl_work.strip()
        if '\n' in tpl_clean:
            tpl_lines = tpl_clean.split('\n')
            indented = '\n'.join('  ' + line.rstrip() for line in tpl_lines)
            yaml_out = yaml_header + f"template: |\n{indented}\n"
        else:
            safe_tpl = tpl_clean.replace("'", "''")
            yaml_out = yaml_header + f"template: '{safe_tpl}'\n"

        specs.append({'yaml': yaml_out, 'template_name': t_name, 'classification': t_classification})
    return {
        'templates': specs,
        'python_code_spec': py_code_yaml,
        'python_udf_names': py_fn_names,
        'python_stage_files': stage_files
    }
$$;

CREATE OR REPLACE PROCEDURE DCR_SNOWVA.MIGRATION.GENERATE_DATA_OFFERING_SPECS(CLEANROOM_NAME STRING)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('snowflake-snowpark-python', 'pandas', 'pyyaml')
HANDLER = 'gen_data_offerings'
EXECUTE AS CALLER
AS
$$
import yaml
import pandas as pd
import re
import hashlib
from datetime import datetime

def gen_data_offerings(session, cleanroom_name):
    VALID_COLUMN_TYPES = [
        'email', 'hashed_email_sha256', 'hashed_email_b64_encoded',
        'phone', 'hashed_phone_sha256', 'hashed_phone_b64_encoded',
        'device_id', 'hashed_device_id_sha256', 'hashed_device_b64_encoded',
        'ip_address', 'hashed_ip_address_sha256', 'hashed_ip_address_b64_encoded',
        'first_name', 'hashed_first_name_sha256', 'hashed_first_name_b64_encoded',
    ]

    def guess_type(cname):
        c = cname.lower().strip()
        if c in ('hem', 'hashed_email', 'h_email') or ('email' in c and 'device' not in c):
            if 'b64' in c: return 'hashed_email_b64_encoded'
            if c == 'email': return 'email'
            return 'hashed_email_sha256'
        if c in ('hpn', 'hashed_phone', 'h_phone') or 'phone' in c:
            if 'b64' in c: return 'hashed_phone_b64_encoded'
            if c == 'phone': return 'phone'
            return 'hashed_phone_sha256'
        if ('ip_addr' in c or c == 'ip_address' or c == 'hashed_ip') and 'zip' not in c:
            if 'b64' in c: return 'hashed_ip_address_b64_encoded'
            if 'hash' in c or 'sha256' in c: return 'hashed_ip_address_sha256'
            return 'ip_address'
        if ('device_id' in c or 'idfa' in c or 'maid' in c) and c != 'device_type':
            if 'b64' in c: return 'hashed_device_b64_encoded'
            if 'hash' in c or 'sha256' in c: return 'hashed_device_id_sha256'
            return 'device_id'
        if 'first_name' in c or c == 'fname':
            if 'b64' in c: return 'hashed_first_name_b64_encoded'
            if 'hash' in c: return 'hashed_first_name_sha256'
            return 'first_name'
        if 'last_name' in c or c == 'lname':
            if 'b64' in c: return 'hashed_last_name_b64_encoded'
            if 'hash' in c: return 'hashed_last_name_sha256'
            return 'last_name'
        if 'zipcode' in c or 'zip_code' in c or c == 'zip':
            return None
        return None
    
    VALID_COLUMN_TYPES = {
        'email', 'hashed_email_sha256', 'hashed_email_b64_encoded',
        'phone', 'hashed_phone_sha256', 'hashed_phone_b64_encoded',
        'device_id', 'hashed_device_id_sha256', 'hashed_device_b64_encoded',
        'ip_address', 'hashed_ip_address_sha256', 'hashed_ip_address_b64_encoded',
        'first_name', 'hashed_first_name_sha256', 'hashed_first_name_b64_encoded',
        'last_name', 'hashed_last_name_sha256', 'hashed_last_name_b64_encoded'
    }

    def refine_type_by_data(table, col, proposed_type):
        if not proposed_type: return proposed_type
        if proposed_type in ('email', 'phone', 'first_name', 'last_name', 'ip_address', 'device_id'):
            return proposed_type if proposed_type in VALID_COLUMN_TYPES else None
        try:
            res = session.sql(f'SELECT "{col}" FROM {table} WHERE "{col}" IS NOT NULL LIMIT 5').collect()
            if not res: return proposed_type
            for row in res:
                val = str(row[0]).strip()
                if not val: continue
                if all(ch == '*' for ch in val) or val in ('null', 'NULL', 'None', ''):
                    continue
                is_b64 = val.endswith('=') or '/' in val or '+' in val
                is_hex = len(val) == 64 and all(ch in '0123456789abcdef' for ch in val.lower())
                base = proposed_type.replace('_sha256', '').replace('_b64_encoded', '').replace('hashed_', '')
                if is_b64:
                    candidate = f'hashed_{base}_b64_encoded'
                    return candidate if candidate in VALID_COLUMN_TYPES else proposed_type
                elif is_hex:
                    candidate = f'hashed_{base}_sha256'
                    return candidate if candidate in VALID_COLUMN_TYPES else proposed_type
                else:
                    return None
            return None
        except:
            return proposed_type if proposed_type in VALID_COLUMN_TYPES else None

    def get_df_upper(query):
        try:
            res = session.sql(query).collect()
            if not res: return pd.DataFrame()
            return pd.DataFrame([{k.upper(): v for k, v in r.as_dict().items()} for r in res])
        except: return pd.DataFrame()

    def sanitize_name(name):
        clean = re.sub(r'[^A-Za-z0-9_]', '_', name)
        if len(clean) > 75:
            h = hashlib.md5(name.encode()).hexdigest()[:8]
            clean = f"{clean[:60]}_{h}"
        if not clean[0].isalpha() and clean[0] != '_':
            clean = "T_" + clean
        return clean


    is_provider = False
    is_ui_cleanroom = False
    target_uuid = None
    has_ui_freeform = False
    has_pc_freeform = False
    try:
        p_res = session.sql("CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.PROVIDER.VIEW_CLEANROOMS()").collect()
        for r in p_res:
            d = {k.upper(): v for k, v in r.as_dict().items()}
            c_name = d.get('CLEANROOM_NAME') or d.get('NAME')
            c_id = d.get('CLEANROOM_ID') or d.get('ID')
            c_state = d.get('STATE') or d.get('STATUS')
            if c_name and c_name.upper().replace(' ', '_') == cleanroom_name.upper().replace(' ', '_'):
                target_uuid = c_id
                if str(c_name).upper().replace(' ', '_') != str(c_id).upper().replace(' ', '_'):
                    is_ui_cleanroom = True
                if c_state == 'CREATED':
                    is_provider = True
                break
            if c_id and c_id.upper().replace(' ', '_') == cleanroom_name.upper().replace(' ', '_'):
                target_uuid = c_id
                is_ui_cleanroom = True
                if c_state == 'CREATED':
                    is_provider = True
                break
    except: pass

    api_cleanroom_name = target_uuid if (is_ui_cleanroom and target_uuid) else cleanroom_name

    is_consumer = False
    if not is_provider:
        try:
            is_consumer = session.call("SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.CONSUMER.IS_ENABLED", api_cleanroom_name)
        except:
            try:
                is_consumer = session.call("SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.CONSUMER.IS_ENABLED", cleanroom_name)
            except: pass

    if not is_provider and not is_consumer:
        return []

    agg_policy_map = {}
    if target_uuid:
        try:
            sql_t = session.sql(f"SELECT COUNT(*) AS CNT FROM SAMOOHA_CLEANROOM_{target_uuid}.SHARED_SCHEMA.TABLES_ENABLED_FOR_SQL").collect()
            if sql_t and sql_t[0]['CNT'] > 0:
                has_ui_freeform = True
        except: pass
        try:
            wf = session.sql(f"SELECT COUNT(*) AS CNT FROM SAMOOHA_CLEANROOM_{target_uuid}.SHARED_SCHEMA.WORKFLOWS_ENABLED WHERE WORKFLOW_NAME = 'freeform_sql'").collect()
            if wf and wf[0]['CNT'] > 0:
                has_pc_freeform = True
        except: pass

        if has_ui_freeform:
            try:
                agg_pols = session.sql(f"SHOW AGGREGATION POLICIES IN SCHEMA SAMOOHA_CLEANROOM_{target_uuid}.SHARED_SCHEMA").collect()
                for ap in agg_pols:
                    ap_d = {k.upper(): v for k, v in ap.as_dict().items()}
                    pol_name = ap_d.get('NAME', '')
                    try:
                        desc = session.sql(f"DESCRIBE AGGREGATION POLICY SAMOOHA_CLEANROOM_{target_uuid}.SHARED_SCHEMA.{pol_name}").collect()
                        if desc:
                            body = str({k.upper(): v for k, v in desc[0].as_dict().items()}.get('BODY', ''))
                            m = re.search(r'min_row_count\s*=>\s*(\d+)', body)
                            threshold = int(m.group(1)) if m else 5
                            table_suffix = re.sub(r'^AGG\d+_', '', pol_name)
                            agg_policy_map[table_suffix.upper()] = {'threshold': threshold, 'policy_name': pol_name, 'body': body}
                    except: pass
            except: pass

    tables_data = [] 
    join_df = pd.DataFrame()
    col_df = pd.DataFrame()
    act_df = pd.DataFrame()
    prov_join_df = pd.DataFrame()
    prov_col_df = pd.DataFrame()
    ui_sql_tables = set()
    pc_sql_tables = set()

    if is_provider:
        prov_res = session.sql(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.PROVIDER.view_provider_datasets('{api_cleanroom_name}')").collect()
        if prov_res:
            for row in prov_res:
                d = {k.upper(): v for k, v in row.as_dict().items()}
                t_name = d.get('TABLE_NAME')
                if t_name:
                    tables_data.append({'TABLE_NAME': t_name, 'SQL_ENABLED': d.get('SQL_ENABLED', False)})

        if target_uuid and has_ui_freeform:
            try:
                sql_t_df = get_df_upper(f"SELECT * FROM SAMOOHA_CLEANROOM_{target_uuid}.SHARED_SCHEMA.TABLES_ENABLED_FOR_SQL")
                if not sql_t_df.empty:
                    for _, row in sql_t_df.iterrows():
                        ui_sql_tables.add(str(row.get('TABLE_NAME', '')).upper())
            except: pass

        if target_uuid and has_pc_freeform:
            try:
                wf_t_df = get_df_upper(f"SELECT * FROM SAMOOHA_CLEANROOM_{target_uuid}.SHARED_SCHEMA.WORKFLOWS_ENABLED_TABLES WHERE WORKFLOW_NAME = 'freeform_sql'")
                if not wf_t_df.empty:
                    for _, row in wf_t_df.iterrows():
                        pc_sql_tables.add(str(row.get('TABLE_NAME', '')).upper())
            except: pass

        for t_data in tables_data:
            tn_upper = t_data['TABLE_NAME'].upper()
            if tn_upper in ui_sql_tables or tn_upper in pc_sql_tables:
                t_data['SQL_ENABLED'] = True
        
        uuid = target_uuid
        if not uuid:
            cr = session.sql(f"SELECT * FROM SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.PUBLIC.CLEANROOM_RECORD WHERE UPPER(CLEANROOM_NAME) = '{api_cleanroom_name.upper()}' OR UPPER(CLEANROOM_ID) = '{api_cleanroom_name.upper()}'").collect()
            if cr:
                cr_dict = {k.upper(): v for k, v in cr[0].as_dict().items()}
                uuid = cr_dict.get('CLEANROOM_ID') or cr_dict.get('ID') or cr_dict.get('CLEANROOM_UUID')
                if not uuid:
                    for k, v in cr_dict.items():
                        if 'ID' in k and 'SIDE' not in k: uuid = v; break
        
        if uuid:
            join_df = get_df_upper(f"SELECT * FROM SAMOOHA_CLEANROOM_{uuid}.SHARED_SCHEMA.JOIN_COLUMNS")
            col_df = get_df_upper(f"SELECT * FROM SAMOOHA_CLEANROOM_{uuid}.SHARED_SCHEMA.POLICY_COLUMNS")
            try: act_df = get_df_upper(f"SELECT * FROM SAMOOHA_CLEANROOM_{uuid}.SHARED_SCHEMA.ACTIVATION_COLUMNS")
            except: pass
    elif is_consumer:
        cons_res = session.sql(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.CONSUMER.view_consumer_datasets('{api_cleanroom_name}')").collect()
        if cons_res:
            for row in cons_res:
                d = {k.upper(): v for k, v in row.as_dict().items()}
                t_name = None
                for col_candidate in ['LINKED_TABLE', 'TABLE_NAME', 'DATASET_NAME', 'OBJECT_NAME', 'NAME']:
                    if col_candidate in d and d[col_candidate]:
                        t_name = d[col_candidate]
                        break
                if not t_name:
                    for k, v in d.items():
                        if v and isinstance(v, str) and '.' in v and 'VIEW' not in k:
                            t_name = v
                            break
                if t_name:
                    tables_data.append({'TABLE_NAME': t_name, 'SQL_ENABLED': False})
        
        try:
            join_df = get_df_upper(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.CONSUMER.view_join_policy('{api_cleanroom_name}')")
        except: pass
        try:
            col_df = get_df_upper(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.CONSUMER.view_column_policy('{api_cleanroom_name}')")
        except: pass
        try: act_df = get_df_upper(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.CONSUMER.view_activation_policy('{api_cleanroom_name}')")
        except: pass
        try:
            prov_join_df = get_df_upper(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.CONSUMER.view_provider_join_policy('{api_cleanroom_name}')")
        except: pass
        try:
            prov_col_df = get_df_upper(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.CONSUMER.view_provider_column_policy('{api_cleanroom_name}')")
        except: pass

    if not tables_data: return []

    specs = []
    ver_str = "MIGRATION_V2"

    def table_matches(policy_table, target_table):
        if not policy_table or not target_table:
            return False
        pt = policy_table.upper().strip()
        tt = target_table.upper().strip()
        if pt == tt:
            return True
        pt_parts = pt.split('.')
        tt_parts = tt.split('.')
        if len(pt_parts) >= 3 and len(tt_parts) >= 3 and pt_parts[-3:] == tt_parts[-3:]:
            return True
        if pt_parts[-1] == tt_parts[-1] and len(pt_parts) >= 2 and len(tt_parts) >= 2 and pt_parts[-2] == tt_parts[-2]:
            return True
        if tt.endswith(pt) or pt.endswith(tt):
            return True
        return False

    def find_table_col(df):
        for c in ['TABLE_NAME', 'DATASET_NAME', 'TABLE', 'OBJECT_NAME']:
            if c in df.columns:
                return c
        for c in df.columns:
            if 'TABLE' in c or 'DATASET' in c or 'OBJECT' in c:
                return c
        return None

    def find_col_col(df):
        for c in ['COLUMN_NAME', 'COLUMN', 'COL_NAME']:
            if c in df.columns:
                return c
        for c in df.columns:
            if 'COL' in c and c != find_table_col(df):
                return c
        return None

    def extract_policies_from_df(df, target_table, policy_type='column'):
        """Extract (table, column) pairs from a policy DataFrame.
        Handles multiple result formats from consumer API:
        1. Separate TABLE_NAME + COLUMN_NAME columns
        2. Combined 'template:table:column' format in a single column
        3. Combined 'table:column' format in a single column
        """
        results = []
        if df.empty:
            return results

        tc = find_table_col(df)
        cc = find_col_col(df)

        if tc and cc:
            for _, row in df.iterrows():
                p_table = str(row.get(tc, ''))
                p_col = str(row.get(cc, ''))
                if table_matches(p_table, target_table) and p_col:
                    results.append(p_col)
            if results:
                return results

        for _, row in df.iterrows():
            for col in df.columns:
                val = str(row.get(col, '')).strip()
                if ':' in val:
                    parts = val.split(':')
                    if len(parts) == 3:
                        tpl, tbl, colname = parts[0].strip(), parts[1].strip(), parts[2].strip()
                        if table_matches(tbl, target_table) and colname:
                            results.append(colname)
                    elif len(parts) == 2:
                        tbl, colname = parts[0].strip(), parts[1].strip()
                        if table_matches(tbl, target_table) and colname:
                            results.append(colname)
        if results:
            return results

        if tc and not cc:
            for _, row in df.iterrows():
                p_table = str(row.get(tc, ''))
                if table_matches(p_table, target_table):
                    for c in df.columns:
                        if c != tc:
                            v = str(row.get(c, '')).strip()
                            if v and '.' not in v and len(v) < 100:
                                results.append(v)
        return results

    for t_data in tables_data:
        t_name = t_data['TABLE_NAME']
        if not t_name or "TEMP_PUBLIC_KEY" in t_name: continue
        
        schema_policies = {}

        for policy_df in [join_df, prov_join_df]:
            if policy_df.empty:
                continue
            join_cols = extract_policies_from_df(policy_df, t_name, 'join')
            for cname in join_cols:
                cname = cname.upper().strip()
                if cname and cname not in schema_policies:
                    gtype = guess_type(cname)
                    gtype = refine_type_by_data(t_name, cname, gtype)
                    if gtype and gtype in VALID_COLUMN_TYPES:
                        policy = {'category': 'join_standard', 'column_type': gtype}
                    else:
                        policy = {'category': 'passthrough'}
                    schema_policies[cname] = policy

        for policy_df in [col_df, prov_col_df]:
            if policy_df.empty:
                continue
            col_names = extract_policies_from_df(policy_df, t_name, 'column')
            for cname in col_names:
                cname = cname.upper().strip()
                if cname and cname not in schema_policies:
                    schema_policies[cname] = {'category': 'passthrough'}

        if not act_df.empty:
            act_cols = extract_policies_from_df(act_df, t_name, 'activation')
            for cname in act_cols:
                cname = cname.upper().strip()
                if cname:
                    if cname not in schema_policies:
                        schema_policies[cname] = {'category': 'passthrough'}
                    schema_policies[cname]['activation_allowed'] = True
                     
        try:
            desc_res = session.sql(f"DESC TABLE {t_name}").collect()
            if desc_res:
                for row in desc_res:
                    rd = {k.upper(): v for k, v in row.as_dict().items()}
                    col_name = rd.get('NAME', '').upper().strip()
                    if not col_name or col_name in schema_policies:
                        continue
                    gtype = guess_type(col_name)
                    if gtype:
                        gtype = refine_type_by_data(t_name, col_name, gtype)
                        if gtype and gtype in VALID_COLUMN_TYPES:
                            schema_policies[col_name] = {'category': 'join_standard', 'column_type': gtype}
                        else:
                            schema_policies[col_name] = {'category': 'passthrough'}
                    else:
                        schema_policies[col_name] = {'category': 'passthrough'}
        except: 
            if not schema_policies:
                schema_policies['DUMMY_COL'] = {'category': 'passthrough'}

        safe_name = sanitize_name(f"migrated_{t_name}")
        
        dataset_obj = {
            'alias': safe_name,
            'data_object_fqn': t_name, 
            'allowed_analyses': 'template_and_freeform_sql' if t_data.get('SQL_ENABLED') else 'template_only',
            'object_class': 'custom', 
            'schema_and_template_policies': schema_policies
        }
        
        if t_data.get('SQL_ENABLED'):
            freeform_policies = {}
            migration_note = ""
            table_suffix_key = t_name.replace('.', '__').upper()
            matched_agg = None
            for suffix, pol_info in agg_policy_map.items():
                if suffix.upper() in table_suffix_key or table_suffix_key in suffix.upper():
                    matched_agg = pol_info
                    break
            if matched_agg:
                threshold = matched_agg['threshold']
                join_cols_for_entity = [c for c in schema_policies if schema_policies[c].get('category') == 'join_standard']
                freeform_policies['aggregation_policy'] = {
                    'name': f"PLACEHOLDER_AGG_POLICY_{threshold}"
                }
                if join_cols_for_entity:
                    freeform_policies['aggregation_policy']['entity_keys'] = join_cols_for_entity
                migration_note = f"Create aggregation policy with MIN_GROUP_SIZE => {threshold} before registering. Original: {matched_agg['policy_name']} ({matched_agg['body']})"
            elif has_pc_freeform:
                migration_note = "P&C freeform SQL: policies are on source tables. Use POLICY_REFERENCES() to extract and reference them in freeform_sql_policies."

            if freeform_policies:
                dataset_obj['freeform_sql_policies'] = freeform_policies
            dataset_obj['require_freeform_sql_policy'] = False

        spec = {
            'api_version': '2.0.0', 'spec_type': 'data_offering', 'name': safe_name,
            'version': ver_str, 'description': f"Migrated {t_name}",
            'datasets': [dataset_obj]
        }
        yaml_str = yaml.dump(spec, default_flow_style=False, sort_keys=False)
        if t_data.get('SQL_ENABLED') and migration_note:
            yaml_str = f"# NOTE: {migration_note}\n" + yaml_str
        specs.append(yaml_str)
    return specs
$$;

CREATE OR REPLACE PROCEDURE DCR_SNOWVA.MIGRATION.GENERATE_COLLABORATION_SPEC(
    CLEANROOM_NAME STRING, PROVIDER_DO_IDS ARRAY, CONSUMER_DO_IDS ARRAY, TEMPLATE_IDS ARRAY, ENABLE_ACTIVATION BOOLEAN
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('snowflake-snowpark-python', 'pyyaml')
HANDLER = 'gen_collab'
EXECUTE AS CALLER
AS
$$
import yaml
from datetime import datetime

def gen_collab(session, cleanroom_name, prov_ids, cons_ids, temp_ids, enable_activation):
    try:
        cr_res = session.sql(f"SELECT * FROM SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.PUBLIC.CLEANROOM_RECORD WHERE UPPER(CLEANROOM_NAME) = '{cleanroom_name.upper()}' OR UPPER(CLEANROOM_ID) = '{cleanroom_name.upper()}'").collect()
    except:
        cr_res = []

    prov_acct = "PROVIDER_ACCOUNT"
    try:
        curr_org = session.sql("SELECT CURRENT_ORGANIZATION_NAME()").collect()[0][0]
        curr_acct = session.sql("SELECT CURRENT_ACCOUNT_NAME()").collect()[0][0]
        if curr_org and curr_acct:
            prov_acct = f"{curr_org}.{curr_acct}"
    except: pass

    if prov_acct == "PROVIDER_ACCOUNT":
        if cr_res:
            cr = {k.upper(): v for k, v in cr_res[0].as_dict().items()}
            for k,v in cr.items():
                if "PROVIDER" in k and "LOCATOR" in k and v:
                    prov_acct = v
                    break
        if prov_acct != "PROVIDER_ACCOUNT" and '.' not in prov_acct:
            try:
                curr_org = session.sql("SELECT CURRENT_ORGANIZATION_NAME()").collect()[0][0]
                prov_acct = f"{curr_org}.{prov_acct}"
            except: pass

    cons_acct = "CONSUMER_ACCOUNT"
    consumer_resolved = False
    all_consumers = []
    try:
        cons_res = session.sql(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.PROVIDER.VIEW_CONSUMERS('{cleanroom_name}')").collect()
        if cons_res:
            for cr_row in cons_res:
                c_dict = {k.upper(): v for k, v in cr_row.as_dict().items()}
                acct_val = None
                for k,v in c_dict.items():
                    if ('NAME' in k or 'ACCOUNT' in k) and v:
                        acct_val = str(v)
                        break
                if acct_val and acct_val not in all_consumers:
                    all_consumers.append(acct_val)
            if all_consumers:
                cons_acct = all_consumers[0]
                consumer_resolved = True
    except:
        pass

    if not consumer_resolved or cons_acct == "CONSUMER_ACCOUNT" or '.' not in cons_acct:
         try:
             curr_org = session.sql("SELECT CURRENT_ORGANIZATION_NAME()").collect()[0][0]
             if consumer_resolved and cons_acct != "CONSUMER_ACCOUNT":
                 cons_acct = f"{curr_org}.{cons_acct}"
             else:
                 cons_acct = f"{curr_org}.REPLACE_WITH_CONSUMER_ACCOUNT"
                 consumer_resolved = False
         except:
             if not consumer_resolved:
                 cons_acct = "ORG.REPLACE_WITH_CONSUMER_ACCOUNT"

    is_single_account = (
        prov_acct.upper().strip() == cons_acct.upper().strip()
        and 'REPLACE' not in cons_acct.upper()
    )

    runners = {}

    if is_single_account:
        runner_config = {
            'templates': [{'id': x} for x in temp_ids]
        }
        runner_config['data_providers'] = {'Provider_Account': {'data_offerings': [{'id': x} for x in prov_ids]}}
        if enable_activation:
            runner_config['activation_destinations'] = {'snowflake_collaborators': ['Provider_Account']}
        runners['Provider_Account'] = runner_config
    elif len(all_consumers) > 1:
        for i, c_acct in enumerate(all_consumers):
            c_alias = f"Consumer_{i+1}"
            cons_dp = {
                'Provider_Account': {'data_offerings': [{'id': x} for x in prov_ids]},
                c_alias: {'data_offerings': []},
            }
            cons_runner_config = {
                'templates': [{'id': x} for x in temp_ids],
                'data_providers': cons_dp,
            }
            if enable_activation:
                cons_runner_config['activation_destinations'] = {'snowflake_collaborators': [c_alias]}
            runners[c_alias] = cons_runner_config

        can_prov_run = False
        try:
            res = session.sql(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.LIBRARY.IS_PROVIDER_RUN_ENABLED('{cleanroom_name}')").collect()
            if res:
                val = str(res[0][0]).lower()
                if "provider side run analysis is enabled" in val: can_prov_run = True
        except: pass
        if can_prov_run:
            prov_dp = {'Provider_Account': {'data_offerings': [{'id': x} for x in prov_ids]}}
            for i in range(len(all_consumers)):
                prov_dp[f"Consumer_{i+1}"] = {'data_offerings': []}
            runners['Provider_Account'] = {
                'templates': [{'id': x} for x in temp_ids],
                'data_providers': prov_dp,
            }
    else:
        # Consumer_Account runner must list BOTH collaborators' offerings: provider data (e.g. CUSTOMERS)
        # and consumer data (e.g. labels / my_table). Previously only Provider_Account appeared, which made
        # the spec look like the consumer data offering was "missing" for cross-table templates (lookalike).
        cons_dp = {
            'Provider_Account': {'data_offerings': [{'id': x} for x in prov_ids]},
            'Consumer_Account': {'data_offerings': [{'id': x} for x in cons_ids]},
        }
        cons_runner_config = {
            'templates': [{'id': x} for x in temp_ids],
            'data_providers': cons_dp,
        }
        if enable_activation:
            cons_runner_config['activation_destinations'] = {'snowflake_collaborators': ['Consumer_Account']}
        runners['Consumer_Account'] = cons_runner_config

        if cons_ids:
            prov_runner_config = {
                'templates': [{'id': x} for x in temp_ids],
                'data_providers': {
                    'Provider_Account': {'data_offerings': [{'id': x} for x in prov_ids]},
                    'Consumer_Account': {'data_offerings': [{'id': x} for x in cons_ids]},
                },
            }
            if enable_activation:
                 prov_runner_config['activation_destinations'] = {'snowflake_collaborators': ['Consumer_Account']}
            runners['Provider_Account'] = prov_runner_config
        else:
            can_prov_run = False
            try:
                res = session.sql(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.LIBRARY.IS_PROVIDER_RUN_ENABLED('{cleanroom_name}')").collect()
                if res:
                     val = str(res[0][0]).lower()
                     if "provider side run analysis is enabled" in val: can_prov_run = True
            except: pass
            
            if can_prov_run:
                prov_runner_config = {
                    'templates': [{'id': x} for x in temp_ids],
                    'data_providers': {
                        'Provider_Account': {'data_offerings': [{'id': x} for x in prov_ids]},
                        'Consumer_Account': {'data_offerings': []},
                    },
                }
                if enable_activation:
                     prov_runner_config['activation_destinations'] = {'snowflake_collaborators': ['Consumer_Account']}
                runners['Provider_Account'] = prov_runner_config

    ver_str = "MIGRATION_V2"

    human_name = cleanroom_name
    try:
        p_res = session.sql("CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.PROVIDER.VIEW_CLEANROOMS()").collect()
        for r in p_res:
            d = {k.upper(): v for k, v in r.as_dict().items()}
            c_name = d.get('CLEANROOM_NAME') or d.get('NAME')
            c_id = d.get('CLEANROOM_ID') or d.get('ID')
            name_match = c_name and c_name.upper().replace(' ', '_') == cleanroom_name.upper().replace(' ', '_')
            id_match = c_id and c_id.upper().replace(' ', '_') == cleanroom_name.upper().replace(' ', '_')
            if name_match or id_match:
                human_name = c_name if c_name else cleanroom_name
                break
    except: pass

    safe_collab_name = f"migrated_{human_name.replace(' ', '_').upper()}"

    yaml_str = f"api_version: 2.0.0\n"
    yaml_str += f"spec_type: collaboration\n"
    yaml_str += f"name: {safe_collab_name}\n"
    yaml_str += f"description: 'Migrated from P&C: {cleanroom_name}'\n"
    yaml_str += f"version: {ver_str}\n"
    yaml_str += f"owner: Provider_Account\n"
    
    if is_single_account:
        aliases = {
            'collaborator_identifier_aliases': {
                'Provider_Account': prov_acct
            }
        }
    elif len(all_consumers) > 1:
        alias_map = {'Provider_Account': prov_acct}
        for i, c_acct in enumerate(all_consumers):
            resolved_c = c_acct
            if '.' not in resolved_c:
                try:
                    curr_org = session.sql("SELECT CURRENT_ORGANIZATION_NAME()").collect()[0][0]
                    resolved_c = f"{curr_org}.{c_acct}"
                except: pass
            alias_map[f"Consumer_{i+1}"] = resolved_c
        aliases = {'collaborator_identifier_aliases': alias_map}
    else:
        aliases = {
            'collaborator_identifier_aliases': {
                'Provider_Account': prov_acct,
                'Consumer_Account': cons_acct
            }
        }
    yaml_str += yaml.dump(aliases, sort_keys=False)
    runners_dict = {'analysis_runners': runners}
    yaml_str += yaml.dump(runners_dict, sort_keys=False)

    if not consumer_resolved and not is_single_account:
        yaml_str = f"# WARNING: Consumer account could not be resolved automatically.\n# Please replace 'REPLACE_WITH_CONSUMER_ACCOUNT' with the actual ORG.ACCOUNT identifier.\n" + yaml_str

    return yaml_str
$$;

CREATE OR REPLACE PROCEDURE DCR_SNOWVA.MIGRATION.VALIDATE(CLEANROOM_NAME STRING, COLLABORATION_NAME STRING)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('snowflake-snowpark-python', 'pandas')
HANDLER = 'validate'
EXECUTE AS CALLER
AS
$$
import pandas as pd

def validate(session, cleanroom_name, collab_name):
    report = {"overall_status": "PASS", "steps": [], "missing_objects": [], "remediation": []}
    def log_step(name, status, details="", fix_hint=""):
        report['steps'].append({"name": name, "status": status, "details": details, "fix_hint": fix_hint})
        if status == "FAIL":
            report['overall_status'] = "FAIL"
            if fix_hint:
                report['remediation'].append(f"[{name}] {fix_hint}")

    try:
        try:
            session.sql("USE SECONDARY ROLES NONE").collect()
        except: pass

        is_provider = False
        is_ui_cleanroom = False
        target_uuid = None
        try:
            p_res = session.sql("CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.PROVIDER.VIEW_CLEANROOMS()").collect()
            for r in p_res:
                d = {k.upper(): v for k, v in r.as_dict().items()}
                c_name = d.get('CLEANROOM_NAME') or d.get('NAME')
                c_id = d.get('CLEANROOM_ID') or d.get('ID')
                c_state = d.get('STATE') or d.get('STATUS')
                name_match = c_name and c_name.upper().replace(' ', '_') == cleanroom_name.upper().replace(' ', '_')
                id_match = c_id and c_id.upper().replace(' ', '_') == cleanroom_name.upper().replace(' ', '_')
                if name_match or id_match:
                    target_uuid = c_id
                    if str(c_name).upper().replace(' ', '_') != str(c_id).upper().replace(' ', '_'):
                        is_ui_cleanroom = True
                    if c_state == 'CREATED':
                        is_provider = True
                    break
        except: pass

        collab_ready = False
        try:
            res = session.sql(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.COLLABORATION.GET_STATUS('{collab_name}')").collect()
            if res:
                statuses = {}
                for r in res:
                    rd = {k.upper(): v for k, v in r.as_dict().items()}
                    name = rd.get('COLLABORATOR_NAME', '')
                    st = rd.get('STATUS', '')
                    statuses[name] = st
                status_summary = ", ".join([f"{k}={v}" for k, v in statuses.items()])
                joined_statuses = {'JOINED', 'CREATED'}
                if any(s in joined_statuses for s in statuses.values()):
                    log_step("Collaboration Status", "PASS", f"Collaborator statuses: {status_summary}")
                    collab_ready = True
                else:
                    log_step("Collaboration Status", "FAIL",
                        f"No collaborator has joined yet. Statuses: {status_summary}",
                        f"You must JOIN the collaboration before running validation. VIEW_TEMPLATES and VIEW_DATA_OFFERINGS require a joined state.")
            else:
                log_step("Collaboration Status", "FAIL", "Collaboration not found.",
                    f"Run the migration EXECUTE step first, then JOIN, then validate.")
        except Exception as e:
            err_msg = str(e)[:300]
            if 'collaborationnotfoun' in err_msg.lower() or 'not found' in err_msg.lower():
                log_step("Collaboration Status", "FAIL",
                    f"Collaboration '{collab_name}' does not exist.",
                    f"Run the EXECUTE step first to create the collaboration, then JOIN it, then validate.")
            else:
                log_step("Collaboration Status", "FAIL", err_msg,
                    f"Check: CALL samooha_by_snowflake_local_db.collaboration.get_status('{collab_name}');")

        if not collab_ready:
            report['remediation'].append("Validation requires the collaboration to exist and at least one collaborator to have joined. Complete the EXECUTE and JOIN steps first.")
            return report

        def _extract_all_names(rows):
            """Extract all string values from every column of every row, uppercased."""
            names = set()
            for r in rows:
                row_dict = r.as_dict()
                for k, v in row_dict.items():
                    if v and isinstance(v, str) and len(v) > 2:
                        names.add(v.upper())
            return names

        def _fuzzy_match(expected_prefix, name_set):
            """Check if expected_prefix matches any name (exact, with version suffix, or prefix)."""
            up = expected_prefix.upper()
            for n in name_set:
                if up == n or n.startswith(up + '_') or n.startswith(up + ':') or up in n:
                    return True
            return False

        if is_provider:
            try:
                legacy_df = session.sql(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.PROVIDER.VIEW_ADDED_TEMPLATES('{cleanroom_name}')").collect()
                legacy_names = [r['TEMPLATE_NAME'] for r in legacy_df] if legacy_df else []
                new_templates_df = session.sql(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.COLLABORATION.VIEW_TEMPLATES('{collab_name}')").collect()

                new_template_names = _extract_all_names(new_templates_df) if new_templates_df else set()

                missing = []
                for old in legacy_names:
                    expected = f"migrated_{old}"
                    if not _fuzzy_match(expected, new_template_names):
                        missing.append(expected)
                if not missing:
                    log_step("Template Parity", "PASS", f"All {len(legacy_names)} templates found in new collaboration.")
                else:
                     log_step("Template Parity", "FAIL", f"Missing templates: {missing}",
                         f"Found in collaboration: {sorted(list(new_template_names))[:10]}. "
                         f"Re-register missing templates, then re-run EXECUTE.")
                     report['missing_objects'].extend(missing)
            except Exception as e:
                log_step("Template Parity", "FAIL", str(e)[:300],
                    "Could not query templates. Verify the collaboration exists and you have SAMOOHA_APP_ROLE.")

            try:
                prov_ds = session.sql(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.PROVIDER.view_provider_datasets('{cleanroom_name}')").collect()
                legacy_tables = [r['TABLE_NAME'] for r in prov_ds if "TEMP_PUBLIC_KEY" not in r['TABLE_NAME']] if prov_ds else []
                new_dos_df = session.sql(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.COLLABORATION.VIEW_DATA_OFFERINGS('{collab_name}')").collect()

                new_do_names = _extract_all_names(new_dos_df) if new_dos_df else set()

                missing_dos = []
                for t in legacy_tables:
                    sanitized = f"migrated_{t.replace('.', '_')}"
                    if not _fuzzy_match(sanitized, new_do_names):
                        missing_dos.append(sanitized)

                if not missing_dos:
                    log_step("Data Offering Parity", "PASS", f"All {len(legacy_tables)} data offerings found in new collaboration.")
                else:
                    log_step("Data Offering Parity", "FAIL", f"Missing data offerings: {missing_dos}",
                        f"Found in collaboration: {sorted(list(new_do_names))[:10]}. "
                        f"Re-register missing data offerings, then re-run EXECUTE.")
                    report['missing_objects'].extend(missing_dos)
            except Exception as e:
                log_step("Data Offering Parity", "FAIL", str(e)[:300],
                    "Could not query data offerings. Verify the collaboration exists and you have SAMOOHA_APP_ROLE.")
        else:
             log_step("Consumer Check", "INFO", "Consumer-side validation: verified collaboration access. Full parity checks run from the provider side.")

    except Exception as e:
        report['overall_status'] = "ERROR"
        report['error'] = str(e)
        report['remediation'].append(f"Unexpected error during validation: {str(e)[:300]}. Verify both the legacy cleanroom and new collaboration exist.")

    return report
$$;

CREATE OR REPLACE PROCEDURE DCR_SNOWVA.MIGRATION.TEARDOWN(COLLABORATION_NAME STRING)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('snowflake-snowpark-python', 'pandas')
HANDLER = 'teardown_collab'
EXECUTE AS CALLER
AS
$$
import time

def teardown_collab(session, collab_name):
    collab_name = collab_name.replace(' ', '_')
    try:
        session.sql(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.COLLABORATION.TEARDOWN('{collab_name}')").collect()
        
        for _ in range(10):
            res = session.sql(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.COLLABORATION.GET_STATUS('{collab_name}')").collect()
            if res:
                row = {k.upper(): v for k, v in res[0].as_dict().items()}
                status = row.get('STATUS')
                if status == 'LOCAL_DROP_PENDING':
                    break
            time.sleep(2)
            
        session.sql(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.COLLABORATION.TEARDOWN('{collab_name}')").collect()
        
        return f"Teardown completed for {collab_name}"
    except Exception as e:
        return f"Teardown error: {str(e)}"
$$;

CREATE OR REPLACE PROCEDURE DCR_SNOWVA.MIGRATION.AGENT_MIGRATE_ORCHESTRATOR(
    CLEANROOM_NAME STRING, 
    ACTION_MODE STRING
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('snowflake-snowpark-python', 'pandas', 'pyyaml', 'snowflake-snowpark-python')
HANDLER = 'agent_main'
EXECUTE AS CALLER
AS
$$
import json
import yaml
import pandas as pd
import hashlib
import time
import uuid as _uuid

def agent_main(session, cleanroom_name, action_mode):
    action = action_mode.upper()
    dd = "$" + "$"
    job_id = str(_uuid.uuid4())
    job_role = None
    job_start = time.time()
    _tool_version = 'v3.0'

    def _set_query_tag(action, cr_type='', tmpl_count=0, do_count=0, has_python=False, has_freeform=False):
        try:
            safe_cr = cleanroom_name.replace("'", "''")[:100]
            tag = f"dcr_migration_tool:{_tool_version}:action={action}:type={cr_type}:cr={safe_cr}:tmpl={tmpl_count}:do={do_count}:py={has_python}:ffs={has_freeform}"
            session.sql(f"ALTER SESSION SET QUERY_TAG = '{tag}'").collect()
        except:
            pass

    def _clear_query_tag():
        try:
            session.sql("ALTER SESSION UNSET QUERY_TAG").collect()
        except:
            pass

    def _log_job(status, result_str):
        try:
            elapsed = round(time.time() - job_start, 2)
            summary = result_str[:4000] if result_str else '{}'
            summary = summary.replace("'", "''")
            role_val = f"'{job_role}'" if job_role else 'NULL'
            safe_cr = cleanroom_name.replace("'", "''")
            session.sql(f"""
                INSERT INTO DCR_SNOWVA.MIGRATION.MIGRATION_JOBS
                    (JOB_ID, CLEANROOM_NAME, ACTION, ROLE, STARTED_AT, FINISHED_AT, STATUS, DETAILS)
                SELECT
                    '{job_id}', '{safe_cr}', '{action}', {role_val},
                    TIMESTAMPADD(SECOND, -{elapsed}, CURRENT_TIMESTAMP()),
                    CURRENT_TIMESTAMP(), '{status}',
                    TRY_PARSE_JSON('{summary}')
            """).collect()
        except:
            pass

    def _finish(result_str):
        try:
            parsed = json.loads(result_str)
            status = parsed.get('status', 'UNKNOWN')
        except:
            status = 'UNKNOWN'
        _log_job(status, result_str)
        _clear_query_tag()
        return result_str

    try:
        check = session.call("DCR_SNOWVA.MIGRATION.CHECK_PREREQUISITES", cleanroom_name)
        if isinstance(check, str): check = json.loads(check)
        prereq_warnings = check.get('warnings', [])
        if check.get('status') == 'FAIL':
             errors = check.get('errors', [])
             msg = " | ".join(errors) if errors else "Prerequisites check failed."
             return _finish(json.dumps({"status": "ERROR", "message": msg, "warnings": prereq_warnings}))
        
        is_provider = False
        is_ui_cleanroom = False
        target_uuid = None
        human_readable_name = cleanroom_name
        cleanroom_type = check.get('cleanroom_type', 'UNKNOWN')
        try:
            p_res = session.sql("CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.PROVIDER.VIEW_CLEANROOMS()").collect()
            for r in p_res:
                d = {k.upper(): v for k, v in r.as_dict().items()}
                c_name = d.get('CLEANROOM_NAME') or d.get('NAME')
                c_id = d.get('CLEANROOM_ID') or d.get('ID')
                c_state = d.get('STATE') or d.get('STATUS')
                if c_name and c_name.upper().replace(' ', '_') == cleanroom_name.upper().replace(' ', '_'):
                    target_uuid = c_id
                    human_readable_name = c_name
                    if str(c_name).upper().replace(' ', '_') != str(c_id).upper().replace(' ', '_'):
                        is_ui_cleanroom = True
                    if c_state == 'CREATED':
                        is_provider = True
                    break
                if c_id and c_id.upper().replace(' ', '_') == cleanroom_name.upper().replace(' ', '_'):
                    target_uuid = c_id
                    human_readable_name = c_name if c_name else cleanroom_name
                    is_ui_cleanroom = True
                    if c_state == 'CREATED':
                        is_provider = True
                    break
        except: pass

        api_cleanroom_name = target_uuid if (is_ui_cleanroom and target_uuid) else cleanroom_name
        
        is_consumer = False
        if not is_provider:
             try:
                 is_consumer = session.call("SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.CONSUMER.IS_ENABLED", api_cleanroom_name)
             except:
                 try:
                     is_consumer = session.call("SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.CONSUMER.IS_ENABLED", cleanroom_name)
                 except: pass
             
        if not is_provider and not is_consumer:
             return _finish(json.dumps({"status": "ERROR", "message": f"Cleanroom '{cleanroom_name}' not found or access denied."}))

        role_type = "PROVIDER" if is_provider else "CONSUMER"
        job_role = role_type
        
        safe_collab_name = f"migrated_{human_readable_name.replace(' ', '_').upper()}"

        if not is_provider:
            try:
                collab_list = session.sql("CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.COLLABORATION.VIEW_COLLABORATIONS()").collect()
                if collab_list:
                    for cr in collab_list:
                        cd = {k.upper(): v for k, v in cr.as_dict().items()}
                        src = cd.get('SOURCE_NAME', '') or cd.get('COLLABORATION_NAME', '') or cd.get('NAME', '')
                        if src and 'MIGRATED_' in src.upper() and cleanroom_name.upper().replace(' ', '_') in src.upper():
                            safe_collab_name = src
                            break
            except: pass
        
        if action == 'TEARDOWN':
            res = session.call("DCR_SNOWVA.MIGRATION.TEARDOWN", safe_collab_name)
            return _finish(json.dumps({"status": "SUCCESS", "message": res}))

        res_tmps = session.call("DCR_SNOWVA.MIGRATION.GENERATE_TEMPLATE_SPECS", api_cleanroom_name)
        _tpack = json.loads(res_tmps) if isinstance(res_tmps, str) else res_tmps
        if isinstance(_tpack, list):
            tmps = _tpack
            py_code_spec = None
            py_udf_names = []
            py_stage_files = []
        else:
            tmps = _tpack.get('templates', [])
            py_code_spec = _tpack.get('python_code_spec')
            py_udf_names = _tpack.get('python_udf_names', [])
            py_stage_files = _tpack.get('python_stage_files', [])
        
        res_dos = session.call("DCR_SNOWVA.MIGRATION.GENERATE_DATA_OFFERING_SPECS", api_cleanroom_name)
        dos = json.loads(res_dos) if isinstance(res_dos, str) else res_dos

        if not dos and not tmps and action != 'TEARDOWN' and action != 'CHECK_STATUS' and action != 'JOIN':
             if role_type == 'PROVIDER':
                  prereq_warnings.append("WARNING: No data offerings or templates were found. The collaboration will be created without them.")

        script_lines = []
        script_lines.append("USE ROLE SAMOOHA_APP_ROLE;")
        script_lines.append("USE SECONDARY ROLES NONE;")
        script_lines.append(f"-- MIGRATION SCRIPT FOR: {cleanroom_name} ({role_type})")
        script_lines.append(f"-- Cleanroom type: {cleanroom_type}")
        script_lines.append("-- Generated via DCR_SNOWVA.MIGRATION Package\n")

        step_n = 0
        if py_code_spec:
            script_lines.append("-- [0] REGISTER PYTHON CODE SPEC (Collaboration custom functions / UDFs)")
            script_lines.append("-- Ref: https://docs.snowflake.com/en/user-guide/cleanrooms/v2/custom-functions")
            script_lines.append(f"CALL samooha_by_snowflake_local_db.registry.register_code_spec({dd}\n{py_code_spec}\n{dd});\n")
            step_n = 1

        if tmps:
            platform_privacy_tmps = [t for t in tmps if isinstance(t, dict) and t.get('classification') == 'PLATFORM_PRIVACY']
            normal_tmps = [t for t in tmps if isinstance(t, dict) and t.get('classification') != 'PLATFORM_PRIVACY']

            if platform_privacy_tmps:
                script_lines.append(f"\n-- WARNING: {len(platform_privacy_tmps)} platform privacy template(s) detected.")
                script_lines.append("-- These templates use generic_sql_query_with_aggregation_and_projection_policies")
                script_lines.append("-- which is NOT available in Collaboration APIs.")
                script_lines.append("-- They are handled via freeform SQL data offerings (see data offerings below).")
                for pp in platform_privacy_tmps:
                    script_lines.append(f"-- SKIPPED: {pp.get('template_name', 'unknown')} (classification: PLATFORM_PRIVACY)")
                script_lines.append("")

            if normal_tmps:
                script_lines.append(f"-- [{step_n}] REGISTER TEMPLATES ({len(normal_tmps)} found)")
                for t_entry in normal_tmps:
                    y_str = t_entry.get('yaml', '') if isinstance(t_entry, dict) else str(t_entry)
                    script_lines.append(f"CALL samooha_by_snowflake_local_db.registry.register_template({dd}\n{y_str}\n{dd});\n")
                step_n += 1

        if dos:
            script_lines.append(f"\n-- [{step_n}] REGISTER DATA OFFERINGS ({len(dos)} found)")
            for y_str in dos:
                spec = yaml.safe_load(y_str)
                script_lines.append(f"-- {role_type} Offering: {spec['name']}")
                script_lines.append(f"CALL samooha_by_snowflake_local_db.registry.register_data_offering({dd}\n{y_str}\n{dd});\n")
            step_n += 1

        if is_provider:
             prov_ds_df = session.sql(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.PROVIDER.view_provider_datasets('{api_cleanroom_name}')").collect()
             prov_table_names = set()
             if prov_ds_df:
                 for row in prov_ds_df:
                     d = {k.upper(): v for k, v in row.as_dict().items()}
                     t_name = d.get('TABLE_NAME')
                     if t_name: prov_table_names.add(t_name)

             prov_ids = []
             for y_str in dos:
                spec = yaml.safe_load(y_str)
                do_id = f"{spec['name']}_{spec['version']}"
                prov_ids.append(do_id)

             tmp_ids = []
             has_activation = False
             for t_entry in tmps:
                if isinstance(t_entry, dict) and t_entry.get('classification') == 'PLATFORM_PRIVACY':
                    continue
                y_str = t_entry.get('yaml', '') if isinstance(t_entry, dict) else str(t_entry)
                spec = yaml.safe_load(y_str)
                t_id = f"{spec['name']}_{spec['version']}"
                tmp_ids.append(t_id)
                if spec.get('type') == 'sql_activation':
                    has_activation = True
                t_sql = str(spec.get('template', '')).lower()
                if 'activation' in t_sql and ('cleanroom.activation_' in t_sql or 'activation_data' in t_sql):
                    has_activation = True

             if not has_activation:
                 try:
                     cr_rec = session.sql(f"SELECT * FROM SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.PUBLIC.CLEANROOM_RECORD WHERE UPPER(CLEANROOM_NAME) = '{api_cleanroom_name.upper()}' OR UPPER(CLEANROOM_ID) = '{api_cleanroom_name.upper()}'").collect()
                     if cr_rec:
                         cr_d = {k.upper(): v for k, v in cr_rec[0].as_dict().items()}
                         uuid = cr_d.get('CLEANROOM_ID') or cr_d.get('ID')
                         if uuid:
                             try:
                                 act_res = session.sql(f"SELECT COUNT(*) AS CNT FROM SAMOOHA_CLEANROOM_{uuid}.SHARED_SCHEMA.ACTIVATION_COLUMNS").collect()
                                 if act_res and act_res[0]['CNT'] > 0:
                                     has_activation = True
                             except: pass
                 except: pass

             is_prov_run = False
             try:
                 pr_res = session.sql(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.LIBRARY.IS_PROVIDER_RUN_ENABLED('{api_cleanroom_name}')").collect()
                 if pr_res and "provider side run analysis is enabled" in str(pr_res[0][0]).lower():
                     is_prov_run = True
             except: pass

             collab_yml = session.call("DCR_SNOWVA.MIGRATION.GENERATE_COLLABORATION_SPEC", api_cleanroom_name, prov_ids, [], tmp_ids, has_activation)

             script_lines.append(f"\n-- [{step_n}] CREATE COLLABORATION: {safe_collab_name}")
             script_lines.append(f"CALL samooha_by_snowflake_local_db.collaboration.initialize({dd}\n{collab_yml}\n{dd}, 'APP_WH');\n")
             script_lines.append("-- NOTE: Under analysis_runners.Consumer_Account.data_providers you should see BOTH")
             script_lines.append("-- Provider_Account (your linked table, e.g. CUSTOMERS) AND Consumer_Account.")
             script_lines.append("-- Consumer_Account.data_offerings is [] until the consumer registers their dataset and links it.")
             script_lines.append("-- Lookalike-style templates use source_table (provider) and my_table (consumer); both must appear in the spec.\n")
             script_lines.append(f"-- Wait for status 'CREATED' before joining")
             script_lines.append(f"CALL samooha_by_snowflake_local_db.collaboration.get_status('{safe_collab_name}');\n")
             script_lines.append(f"-- [{step_n + 1}] JOIN COLLABORATION (Self-Join for Provider)")
             script_lines.append(f"CALL samooha_by_snowflake_local_db.collaboration.join('{safe_collab_name}');\n")
             script_lines.append(f"-- [{step_n + 2}] ENABLE AUTO-APPROVAL FOR TEMPLATE REQUESTS")
             script_lines.append(f"-- Uses SET_CONFIGURATION (replaces deprecated enable_template_auto_approval).")
             script_lines.append(f"CALL samooha_by_snowflake_local_db.collaboration.set_configuration('{safe_collab_name}', 'TEMPLATE_AUTO_APPROVAL', 'true');\n")
             if tmp_ids:
                 script_lines.append(f"-- [{step_n + 3}] ADD TEMPLATE REQUESTS (share templates with all collaborators)")
                 script_lines.append(f"-- Templates in the INITIALIZE spec are scoped to specific analysis_runners.")
                 script_lines.append(f"-- To share templates with additional collaborators after creation, use add_template_request:")
                 for t_id in tmp_ids:
                     script_lines.append(f"-- CALL samooha_by_snowflake_local_db.collaboration.add_template_request('{safe_collab_name}', '{t_id}', ['Provider_Account', 'Consumer_Account']);\n")
             if is_prov_run:
                 script_lines.append(f"-- [5] PROVIDER-RUN ANALYSIS DETECTED")
                 script_lines.append(f"-- This legacy cleanroom has provider-run analysis enabled.")
                 script_lines.append(f"-- The consumer MUST run their own migration to register data offerings with policies,")
                 script_lines.append(f"-- then link them to this collaboration after joining.")
                 script_lines.append(f"-- Consumer data offerings are listed as empty in the collaboration spec above")
                 script_lines.append(f"-- because the provider cannot see consumer datasets.\n")
             script_lines.append(f"-- CONSUMER: After reviewing and joining, the consumer should register their data offerings")
             script_lines.append(f"-- and link them to the collaboration. Link to ALL collaborators who need the data:")
             script_lines.append(f"-- CALL samooha_by_snowflake_local_db.registry.register_data_offering(<data_offering_spec>);")
             script_lines.append(f"-- CALL samooha_by_snowflake_local_db.collaboration.link_data_offering('{safe_collab_name}', '<data_offering_id>', ['Provider_Account', 'Consumer_Account']);")
        else:
             if not tmps and not dos:
                 script_lines.append(f"\n-- NOTE: No templates or data offerings found on the consumer side.")
                 script_lines.append(f"-- The provider must run migration first to create the collaboration.")
                 script_lines.append(f"-- Once the collaboration is created, run the following steps:\n")

             owner_acct = "REPLACE_WITH_PROVIDER_ORG.ACCOUNT"
             try:
                 cr_rec = session.sql(f"SELECT * FROM SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.PUBLIC.CLEANROOM_RECORD WHERE UPPER(CLEANROOM_NAME) = '{api_cleanroom_name.upper()}' OR UPPER(CLEANROOM_ID) = '{api_cleanroom_name.upper()}'").collect()
                 if cr_rec:
                     cr_d = {k.upper(): v for k, v in cr_rec[0].as_dict().items()}
                     for k, v in cr_d.items():
                         if "PROVIDER" in k and "LOCATOR" in k and v:
                             owner_acct = str(v)
                             break
             except: pass
             if '.' not in owner_acct or 'REPLACE' in owner_acct:
                 try:
                     curr_org = session.sql("SELECT CURRENT_ORGANIZATION_NAME()").collect()[0][0]
                     if owner_acct and 'REPLACE' not in owner_acct:
                         owner_acct = f"{curr_org}.{owner_acct}"
                     else:
                         owner_acct = f"{curr_org}.REPLACE_WITH_PROVIDER_ACCOUNT"
                 except: pass

             script_lines.append(f"\n-- [3] REVIEW COLLABORATION (requires owner account identifier)")
             script_lines.append(f"CALL samooha_by_snowflake_local_db.collaboration.review('{safe_collab_name}', '{owner_acct}');\n")
             script_lines.append(f"-- [4] JOIN COLLABORATION")
             script_lines.append(f"CALL samooha_by_snowflake_local_db.collaboration.join('{safe_collab_name}');\n")

             if dos:
                 script_lines.append(f"-- [5] LINK CONSUMER DATA OFFERINGS (run after join)")
                 script_lines.append(f"-- link_data_offering shares your data with the specified collaborators.")
                 script_lines.append(f"-- List ALL collaborator aliases who need access to your data.\n")
                 for y_str in dos:
                     spec = yaml.safe_load(y_str)
                     do_id = f"{spec['name']}_{spec['version']}"
                     script_lines.append(f"CALL samooha_by_snowflake_local_db.collaboration.link_data_offering('{safe_collab_name}', '{do_id}', ['Provider_Account', 'Consumer_Account']);\n")

             if tmps:
                 consumer_tmp_ids = []
                 for t_entry in tmps:
                     if isinstance(t_entry, dict) and t_entry.get('classification') == 'PLATFORM_PRIVACY':
                         continue
                     y_str = t_entry.get('yaml', '') if isinstance(t_entry, dict) else str(t_entry)
                     try:
                         spec = yaml.safe_load(y_str)
                         consumer_tmp_ids.append(f"{spec['name']}_{spec['version']}")
                     except: pass
                 if consumer_tmp_ids:
                     script_lines.append(f"-- [6] ADD TEMPLATE REQUESTS (share consumer templates with collaborators)")
                     script_lines.append(f"-- Run SET_CONFIGURATION for auto-approval, then add_template_request for each template.\n")
                     script_lines.append(f"CALL samooha_by_snowflake_local_db.collaboration.set_configuration('{safe_collab_name}', 'TEMPLATE_AUTO_APPROVAL', 'true');\n")
                     for t_id in consumer_tmp_ids:
                         script_lines.append(f"CALL samooha_by_snowflake_local_db.collaboration.add_template_request('{safe_collab_name}', '{t_id}', ['Provider_Account', 'Consumer_Account']);\n")

        full_script_text = "\n".join(script_lines)

        required_dbs = set()
        all_table_fqns = []
        if dos:
            for y_str in dos:
                try:
                    spec = yaml.safe_load(y_str)
                    for ds in spec.get('datasets', []):
                        fqn = ds.get('data_object_fqn', '')
                        if fqn:
                            all_table_fqns.append(fqn)
                            if '.' in fqn:
                                required_dbs.add(fqn.split('.')[0].upper())
                except:
                    pass

        ownership_issues = []
        for tbl_fqn in all_table_fqns:
            try:
                grants = session.sql(f"SHOW GRANTS ON TABLE {tbl_fqn}").collect()
                owner_role = None
                has_samooha_select = False
                for g in grants:
                    gd = {k.upper(): v for k, v in g.as_dict().items()}
                    if gd.get('PRIVILEGE') == 'OWNERSHIP':
                        owner_role = gd.get('GRANTEE_NAME', '')
                    if gd.get('PRIVILEGE') == 'SELECT' and gd.get('GRANTEE_NAME') == 'SAMOOHA_APP_ROLE':
                        has_samooha_select = True
                if owner_role and owner_role.upper() != 'SAMOOHA_APP_ROLE':
                    ownership_issues.append({
                        'table': tbl_fqn,
                        'current_owner': owner_role,
                        'has_select': has_samooha_select
                    })
            except:
                ownership_issues.append({'table': tbl_fqn, 'current_owner': 'UNKNOWN (cannot check)', 'has_select': False})

        prereq_lines = []
        if required_dbs or ownership_issues:
            prereq_lines.append("\n-- ============================================================")
            prereq_lines.append("-- PREREQUISITES: Run these BEFORE clicking 'Run Setup'")
            prereq_lines.append("-- ============================================================")

        if required_dbs:
            prereq_lines.append("\n-- Step 1: Register source databases and tables")
            prereq_lines.append("-- USE ROLE SAMOOHA_APP_ROLE;")
            prereq_lines.append("-- USE SECONDARY ROLES NONE;")
            for db in sorted(required_dbs):
                prereq_lines.append(f"-- CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.PROVIDER.REGISTER_DB('{db}');")
            for tbl_fqn in all_table_fqns:
                prereq_lines.append(f"-- CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.LIBRARY.REGISTER_OBJECTS(['{tbl_fqn}']);")

        if ownership_issues:
            non_app_owned = [oi for oi in ownership_issues if oi['current_owner'] != 'SAMOOHA_BY_SNOWFLAKE']
            if non_app_owned:
                prereq_lines.append("\n-- Step 2: Ensure tables are accessible to SAMOOHA_BY_SNOWFLAKE application")
                prereq_lines.append("-- The Collaboration API requires the application to access shared tables.")
                prereq_lines.append("-- After running Step 1, verify with:")
                for oi in non_app_owned:
                    prereq_lines.append(f"-- SHOW GRANTS ON TABLE {oi['table']};")
                    prereq_lines.append(f"--   (current owner: {oi['current_owner']}, needs SELECT grant to APPLICATION SAMOOHA_BY_SNOWFLAKE)")

        if prereq_lines:
            prereq_lines.append("")
            full_script_text = full_script_text.replace(
                "-- Generated via DCR_SNOWVA.MIGRATION Package\n",
                "-- Generated via DCR_SNOWVA.MIGRATION Package\n" + "\n".join(prereq_lines) + "\n"
            )

        if action == 'PLAN':
            t_count = len(tmps) if tmps else 0
            d_count = len(dos) if dos else 0
            _set_query_tag('PLAN', cleanroom_type, t_count, d_count, bool(py_code_spec),
                cleanroom_type in ('UI_FREEFORM_SQL', 'PC_FREEFORM_SQL'))
            py_note = ""
            if py_udf_names:
                py_note = " Python UDFs from LOAD_PYTHON_RECORD: %s." % (", ".join(py_udf_names[:20]))
            plan_payload = {
                "status": "READY_TO_MIGRATE",
                "role": role_type,
                "summary": f"Found {t_count} templates and {d_count} datasets.{py_note}",
                "generated_script": full_script_text,
                "next_step": "Ask user to confirm execution.",
                "details": {
                    "cleanroom_type": cleanroom_type,
                    "is_ui_cleanroom": is_ui_cleanroom,
                    "templates": tmps,
                    "provider_data": dos,
                    "target_collaboration": safe_collab_name,
                    "python_udf_names": py_udf_names,
                    "python_stage_files": py_stage_files[:30],
                    "has_python_code_spec": bool(py_code_spec)
                }
            }
            if prereq_warnings:
                plan_payload["warnings"] = prereq_warnings
            if required_dbs:
                db_list = ", ".join(sorted(required_dbs))
                plan_payload.setdefault("warnings", []).append(
                    f"PREREQUISITE: Register databases before executing: {db_list}. "
                    f"Run: CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.PROVIDER.REGISTER_DB('<DB_NAME>'); "
                    f"AND CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.LIBRARY.REGISTER_OBJECTS(['<DB.SCHEMA.TABLE>']); for each table."
                )
            if ownership_issues:
                non_app_owned = [oi for oi in ownership_issues if oi['current_owner'] != 'SAMOOHA_BY_SNOWFLAKE']
                if non_app_owned:
                    dbs_needing_fix = set()
                    for oi in non_app_owned:
                        fqn = oi['table']
                        if '.' in fqn:
                            dbs_needing_fix.add(fqn.split('.')[0])
                    plan_payload.setdefault("warnings", []).append(
                        f"PREREQUISITE: The Collaboration API requires tables to be accessible to the SAMOOHA_BY_SNOWFLAKE application. "
                        f"{len(non_app_owned)} table(s) are owned by roles other than SAMOOHA_BY_SNOWFLAKE. "
                        f"Run as ACCOUNTADMIN: "
                        f"1) CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.PROVIDER.REGISTER_DB('<DB>'); "
                        f"2) CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.LIBRARY.REGISTER_OBJECTS(['<DB.SCHEMA.TABLE>']); "
                        f"Databases: {', '.join(sorted(dbs_needing_fix))}"
                    )
                plan_payload["details"]["ownership_issues"] = ownership_issues

            return _finish(json.dumps(plan_payload))

        elif action == 'EXECUTE':
            actions_taken = []
            _set_query_tag('EXECUTE', cleanroom_type)

            try:
                session.sql("USE SECONDARY ROLES NONE").collect()
            except:
                pass
            
            if py_code_spec:
                try:
                    session.call("SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.REGISTRY.REGISTER_CODE_SPEC", py_code_spec)
                    actions_taken.append("Registered Python code_spec (custom UDF bundle)")
                except Exception as e:
                    if "already exists" in str(e).lower():
                        actions_taken.append("Python code_spec already registered (skipped)")
                    else:
                        raise e
            
            if tmps:
                for t_entry in tmps:
                    if isinstance(t_entry, dict) and t_entry.get('classification') == 'PLATFORM_PRIVACY':
                        actions_taken.append(f"Skipped platform privacy template: {t_entry.get('template_name', 'unknown')} (handled via freeform SQL data offerings)")
                        continue
                    y_str = t_entry.get('yaml', '') if isinstance(t_entry, dict) else str(t_entry)
                    try:
                        spec = yaml.safe_load(y_str)
                        t_name = spec.get('name', 'unknown')
                    except:
                        t_name = 'unknown'
                    try:
                        session.call("SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.REGISTRY.REGISTER_TEMPLATE", y_str)
                        actions_taken.append(f"Registered template: {t_name}")
                    except Exception as e:
                        if "already exists" in str(e).lower():
                            actions_taken.append(f"Template already registered (skipped): {t_name}")
                        else: raise e
            
            if dos:
                for y_str in dos:
                    try:
                        spec = yaml.safe_load(y_str)
                        do_name = spec.get('name', 'unknown')
                    except:
                        do_name = 'unknown'
                    try:
                        session.call("SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.REGISTRY.REGISTER_DATA_OFFERING", y_str)
                        actions_taken.append(f"Registered data offering: {do_name}")
                    except Exception as e:
                        if "already exists" in str(e).lower():
                            actions_taken.append(f"Data offering already registered (skipped): {do_name}")
                        else: raise e

            if is_provider:
                collab_yml = session.call("DCR_SNOWVA.MIGRATION.GENERATE_COLLABORATION_SPEC", api_cleanroom_name, prov_ids, [], tmp_ids, has_activation)
                actions_taken.append(f"Generated collaboration spec for: {safe_collab_name}")
                return _finish(json.dumps({
                    "status": "SUCCESS",
                    "message": f"Templates and data offerings registered for '{cleanroom_name}'.",
                    "actions": actions_taken,
                    "role": "PROVIDER",
                    "collab_spec": collab_yml,
                    "collab_name": safe_collab_name,
                    "next_step": "INITIALIZE"
                }))
            else:
                owner_acct = ""
                try:
                    cr_rec = session.sql(f"SELECT * FROM SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.PUBLIC.CLEANROOM_RECORD WHERE UPPER(CLEANROOM_NAME) = '{api_cleanroom_name.upper()}' OR UPPER(CLEANROOM_ID) = '{api_cleanroom_name.upper()}'").collect()
                    if cr_rec:
                        cr_d = {k.upper(): v for k, v in cr_rec[0].as_dict().items()}
                        for k, v in cr_d.items():
                            if "PROVIDER" in k and "LOCATOR" in k and v:
                                owner_acct = v
                                break
                except: pass

                if not owner_acct or '.' not in owner_acct:
                    try:
                        curr_org = session.sql("SELECT CURRENT_ORGANIZATION_NAME()").collect()[0][0]
                        if owner_acct and '.' not in owner_acct:
                            owner_acct = f"{curr_org}.{owner_acct}"
                    except: pass

                do_ids = []
                if dos:
                    for y_str in dos:
                        try:
                            spec = yaml.safe_load(y_str)
                            do_ids.append(f"{spec['name']}_{spec['version']}")
                        except: pass

                manual_sql = []
                manual_sql.append("USE ROLE SAMOOHA_APP_ROLE;")
                manual_sql.append("USE SECONDARY ROLES NONE;")
                if owner_acct:
                    manual_sql.append(f"\nCALL samooha_by_snowflake_local_db.collaboration.review('{safe_collab_name}', '{owner_acct}');")
                manual_sql.append(f"CALL samooha_by_snowflake_local_db.collaboration.join('{safe_collab_name}');")
                manual_sql.append(f"CALL samooha_by_snowflake_local_db.collaboration.get_status('{safe_collab_name}');")
                for do_id in do_ids:
                    manual_sql.append(f"\nCALL samooha_by_snowflake_local_db.collaboration.link_data_offering('{safe_collab_name}', '{do_id}', ['Provider_Account', 'Consumer_Account']);")
                manual_sql.append(f"\nCALL samooha_by_snowflake_local_db.collaboration.set_configuration('{safe_collab_name}', 'TEMPLATE_AUTO_APPROVAL', 'true');")

                actions_taken.append("Consumer data offerings registered.")
                actions_taken.append("REVIEW + JOIN must be run manually in a SQL worksheet (requires SYSTEM$ACCEPT_LEGAL_TERMS).")
                return _finish(json.dumps({
                    "status": "SUCCESS",
                    "message": f"Consumer artifacts registered for '{cleanroom_name}'. Run REVIEW + JOIN manually.",
                    "actions": actions_taken,
                    "role": "CONSUMER",
                    "collab_name": safe_collab_name,
                    "owner_account": owner_acct,
                    "consumer_do_ids": do_ids,
                    "manual_join_sql": "\n".join(manual_sql),
                    "next_step": "RUN_MANUAL_JOIN"
                }))

        elif action == 'CHECK_STATUS':
            _set_query_tag('CHECK_STATUS', cleanroom_type)
            try:
                res = session.sql(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.COLLABORATION.GET_STATUS('{safe_collab_name}')").collect()
                overall_status = "UNKNOWN"
                collaborators = []
                error_details = []
                if res:
                    for r in res:
                        rd = {k.upper(): v for k, v in r.as_dict().items()}
                        c_name = rd.get('COLLABORATOR_NAME', '')
                        c_status = rd.get('STATUS', '')
                        c_roles = rd.get('COLLABORATOR_ROLES', rd.get('ROLES', ''))
                        c_account = rd.get('COLLABORATOR_ACCOUNT', '')
                        c_details = rd.get('DETAILS', '')
                        detail_msg = ""
                        if c_details:
                            try:
                                d_parsed = json.loads(str(c_details)) if isinstance(c_details, str) else c_details
                                if isinstance(d_parsed, dict):
                                    inner = d_parsed.get('details', d_parsed)
                                    detail_msg = inner.get('message', str(inner))
                                else:
                                    detail_msg = str(d_parsed)
                            except:
                                detail_msg = str(c_details)
                        collaborators.append({
                            "name": c_name,
                            "status": c_status,
                            "roles": str(c_roles),
                            "account": c_account,
                            "details": detail_msg
                        })
                        if 'FAIL' in c_status.upper() and detail_msg:
                            error_details.append(f"{c_name}: {detail_msg}")
                    statuses = [c['status'] for c in collaborators]
                    if 'JOINED' in statuses:
                        overall_status = 'JOINED'
                    elif 'CREATED' in statuses:
                        overall_status = 'CREATED'
                    elif any('FAIL' in s.upper() for s in statuses):
                        failed = [s for s in statuses if 'FAIL' in s.upper()]
                        overall_status = failed[0]
                    elif statuses:
                        overall_status = statuses[0]
                result = {"status": "SUCCESS", "collaboration_status": overall_status, "collaborators": collaborators}
                if 'FAIL' in overall_status.upper():
                    if error_details:
                        result["error_details"] = error_details
                    if any('side effects' in d.lower() or 'accept_legal_terms' in d.lower() for d in error_details):
                        result["hint"] = "INITIALIZE failed because it requires SYSTEM$ACCEPT_LEGAL_TERMS which cannot run inside a stored procedure. Please run the INITIALIZE and JOIN commands manually in a Snowflake worksheet. Use the generated script from the Review Plan tab."
                    else:
                        result["hint"] = "The collaboration creation failed. Check the error details above. You may need to teardown and re-create, or run the script manually in a worksheet."
                return _finish(json.dumps(result))
            except Exception as e:
                err = str(e)[:500]
                hint = ""
                if 'collaborationnotfoun' in err.lower() or 'not found' in err.lower():
                    hint = "Collaboration not found. Run EXECUTE first to create it."
                return _finish(json.dumps({"status": "ERROR", "message": err, "hint": hint}))

        elif action == 'JOIN':
            _set_query_tag('JOIN', cleanroom_type)
            try:
                if role_type == 'PROVIDER':
                    res = session.sql(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.COLLABORATION.GET_STATUS('{safe_collab_name}')").collect()
                    current_status = "UNKNOWN"
                    if res:
                        row = {k.upper(): v for k, v in res[0].as_dict().items()}
                        current_status = row.get('STATUS')
                    if current_status != 'CREATED':
                        return _finish(json.dumps({"status": "ERROR", "message": f"Collaboration is not ready. Current status: {current_status}. Please wait for 'CREATED'."}))

                msg = []
                if role_type == 'CONSUMER':
                    try:
                        owner_acct = None
                        try:
                            cr_rec = session.sql(f"SELECT * FROM SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.PUBLIC.CLEANROOM_RECORD WHERE UPPER(CLEANROOM_NAME) = '{api_cleanroom_name.upper()}' OR UPPER(CLEANROOM_ID) = '{api_cleanroom_name.upper()}'").collect()
                            if cr_rec:
                                cr_d = {k.upper(): v for k, v in cr_rec[0].as_dict().items()}
                                for k, v in cr_d.items():
                                    if "PROVIDER" in k and "LOCATOR" in k and v:
                                        owner_acct = v
                                        break
                        except: pass
                        if owner_acct:
                            session.call("SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.COLLABORATION.REVIEW", safe_collab_name, owner_acct)
                        else:
                            session.call("SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.COLLABORATION.REVIEW", safe_collab_name)
                        msg.append("Reviewed")
                    except: pass

                session.call("SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.COLLABORATION.JOIN", safe_collab_name)
                msg.append("Join command submitted")
                return _finish(json.dumps({"status": "SUCCESS", "message": ". ".join(msg)}))
            except Exception as e:
                if "side effects" in str(e).lower() or "accept_legal_terms" in str(e).lower():
                    return _finish(json.dumps({
                        "status": "WARNING",
                        "message": f"Join requires manual acceptance of legal terms. Please run 'CALL samooha_by_snowflake_local_db.collaboration.join('{safe_collab_name}')' in a worksheet."
                    }))
                return _finish(json.dumps({"status": "ERROR", "message": str(e)}))
            
        elif action == 'VALIDATE':
            _set_query_tag('VALIDATE', cleanroom_type)
            report = session.call("DCR_SNOWVA.MIGRATION.VALIDATE", cleanroom_name, safe_collab_name)
            return _finish(str(report))

        elif action == 'TEARDOWN':
            _set_query_tag('TEARDOWN', cleanroom_type)
            res = session.call("DCR_SNOWVA.MIGRATION.TEARDOWN", safe_collab_name)
            return _finish(json.dumps({"status": "SUCCESS", "message": res}))
        
        else: return _finish(json.dumps({"status": "ERROR", "message": "INVALID MODE"}))

    except Exception as e:
        return _finish(json.dumps({"status": "ERROR", "message": str(e)}))
$$;