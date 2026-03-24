-- Copyright 2026 Snowflake Inc.
-- SPDX-License-Identifier: Apache-2.0
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
-- http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

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

    # 1. Provider-side discovery and checks
    try:
        p_res = session.sql("CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.PROVIDER.VIEW_CLEANROOMS()").collect()
        for r in p_res:
            d = {k.upper(): v for k, v in r.as_dict().items()}
            c_name = d.get('CLEANROOM_NAME') or d.get('NAME')
            c_id = d.get('CLEANROOM_ID') or d.get('ID')

            if c_name and c_name.upper().replace(' ', '_') == cleanroom_name.upper().replace(' ', '_'):
                found_as_provider = True
                target_uuid = c_id

                # UI Cleanroom Check: Name != ID implies UI creation
                if str(c_name).upper().replace(' ', '_') != str(c_id).upper().replace(' ', '_'):
                    errors.append(f"Cleanroom '{cleanroom_name}' is a UI-created cleanroom (name='{c_name}', id='{c_id}'). UI cleanroom migration is not supported.")

                break
    except Exception as e:
        warnings.append(f"Could not list provider cleanrooms: {str(e)[:200]}")

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

    # 4. LAF Check (both sides)
    try:
        if found_as_consumer:
            is_laf = session.call("SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.CONSUMER.IS_LAF_ENABLED_FOR_CLEANROOM", cleanroom_name)
            if is_laf:
                errors.append(f"LAF (Cross-Cloud Auto-Fulfillment) is enabled for cleanroom '{cleanroom_name}'. LAF cleanroom migration is not supported.")
    except:
        pass

    try:
        res = session.sql("CALL samooha_by_snowflake_local_db.library.is_laf_enabled_on_account()").collect()
        if res and str(res[0][0]).upper() == 'TRUE':
            if not any('LAF' in e for e in errors):
                warnings.append("LAF is enabled on this account. If this specific cleanroom uses LAF, migration may not be supported. Please verify.")
    except:
        pass

    # 5. Provider-side deep checks (multi-provider)
    if target_uuid:
        try:
            mp_check = session.sql(f"SHOW TABLES LIKE 'APPROVED_MULTIPROVIDER_CLEANROOMS' IN SCHEMA SAMOOHA_CLEANROOM_{target_uuid}.ADMIN").collect()
            if len(mp_check) > 0:
                rows = session.sql(f"SELECT COUNT(*) as CNT FROM SAMOOHA_CLEANROOM_{target_uuid}.ADMIN.APPROVED_MULTIPROVIDER_CLEANROOMS").collect()
                if rows and rows[0]['CNT'] > 0:
                    errors.append("Multi-provider cleanroom migration is not supported.")
        except:
            pass

    if errors:
        return {"status": "FAIL", "errors": errors, "warnings": warnings}
    result = {"status": "PASS"}
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
    result = {"cleanroom_name": cleanroom_name, "role": "UNKNOWN", "datasets": [], "templates": [], "policies": {"join": [], "column": [], "activation": []}, "consumers": [], "errors": []}
    
    def fetch_df(query):
        try:
            res = session.sql(query).collect()
            if not res: return pd.DataFrame()
            return pd.DataFrame([{k.upper(): v for k, v in r.as_dict().items()} for r in res])
        except Exception as e:
            return pd.DataFrame()

    # --- ROLE DETECTION ---
    is_provider = False
    try:
        p_res = session.sql("CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.PROVIDER.VIEW_CLEANROOMS()").collect()
        for r in p_res:
            d = {k.upper(): v for k, v in r.as_dict().items()}
            c_name = d.get('CLEANROOM_NAME') or d.get('NAME')
            c_id = d.get('CLEANROOM_ID') or d.get('ID')
            c_state = d.get('STATE') or d.get('STATUS')
            
            # FIX: Normalize name
            if c_name and c_name.upper().replace(' ', '_') == cleanroom_name.upper().replace(' ', '_'):
                if str(c_name).upper().replace(' ', '_') == str(c_id).upper().replace(' ', '_') and c_state == 'CREATED':
                    is_provider = True
                break
    except: pass

    is_consumer = False
    if not is_provider:
        try:
            is_consumer = session.call("SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.CONSUMER.IS_ENABLED", cleanroom_name)
        except: pass

    if not is_provider and not is_consumer:
        result["errors"].append("Cleanroom not found or not installed.")
        return result
    
    result["role"] = "PROVIDER" if is_provider else "CONSUMER"

    try:
        if is_provider:
            prov_ds = fetch_df(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.PROVIDER.view_provider_datasets('{cleanroom_name}')")
            if not prov_ds.empty:
                result["datasets"] = prov_ds['TABLE_NAME'].tolist()
            
            cons_df = fetch_df(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.PROVIDER.VIEW_CONSUMERS('{cleanroom_name}')")
            if not cons_df.empty:
                c_col = 'CONSUMER_ACCOUNT_NAME' if 'CONSUMER_ACCOUNT_NAME' in cons_df.columns else 'CONSUMER_NAME'
                if c_col in cons_df.columns: result["consumers"] = cons_df[c_col].tolist()
            
            tmps = fetch_df(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.PROVIDER.VIEW_ADDED_TEMPLATES('{cleanroom_name}')")
            if not tmps.empty: result["templates"] = tmps['TEMPLATE_NAME'].tolist()

            cr_record = fetch_df(f"SELECT * FROM SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.PUBLIC.CLEANROOM_RECORD WHERE UPPER(CLEANROOM_NAME) = '{cleanroom_name.upper()}'")
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
            cons_ds = fetch_df(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.CONSUMER.view_consumer_datasets('{cleanroom_name}')")
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
                added_tmps = fetch_df(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.CONSUMER.VIEW_ADDED_TEMPLATES('{cleanroom_name}')")
                if not added_tmps.empty:
                    t_col = 'TEMPLATE_NAME' if 'TEMPLATE_NAME' in added_tmps.columns else ('NAME' if 'NAME' in added_tmps.columns else None)
                    if t_col:
                        result["templates"] = added_tmps[t_col].tolist()
            except: pass

            if not result.get("templates"):
                reqs = fetch_df(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.CONSUMER.LIST_TEMPLATE_REQUESTS('{cleanroom_name}')")
                if not reqs.empty: result["templates"] = reqs['TEMPLATE_NAME'].tolist() if 'TEMPLATE_NAME' in reqs.columns else []

            try:
                jp = fetch_df(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.CONSUMER.view_join_policy('{cleanroom_name}')")
                if not jp.empty:
                    result["policies"]["join"] = jp.to_dict(orient='records')
                    result["join_policy_columns"] = list(jp.columns)
            except: pass

            try:
                cp = fetch_df(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.CONSUMER.view_column_policy('{cleanroom_name}')")
                if not cp.empty:
                    result["policies"]["column"] = cp.to_dict(orient='records')
                    result["column_policy_columns"] = list(cp.columns)
            except: pass

            try:
                ap = fetch_df(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.CONSUMER.view_activation_policy('{cleanroom_name}')")
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
    """Match YAML argument names to the Python handler signature (v2 docs / metadata)."""
    if not body or not handler:
        return None
    m = re.search(r'def\s+' + re.escape(str(handler)) + r'\s*\(([^)]*)\)', body, re.IGNORECASE | re.DOTALL)
    if not m:
        return None
    raw = m.group(1).strip()
    if not raw:
        return []
    parts = []
    for a in _split_py_params(raw):
        a = a.strip()
        if not a or a.startswith('*'):
            continue
        pre_default = a.split('=', 1)[0].strip()
        name = pre_default.split(':', 1)[0].strip()
        if not name or name in ('self', 'cls'):
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
        return [s]
    return []


def _extract_imports_from_row(row):
    """P&C load_python_into_cleanroom (stage signature) stores imports in ADDITIONAL_PARAMS.imports."""
    parsed = _coerce_imports_list(row.get('IMPORTS'))
    if parsed:
        return parsed
    ap = row.get('ADDITIONAL_PARAMS')
    if not ap:
        return []
    try:
        d = json.loads(ap) if isinstance(ap, str) else ap
        if isinstance(d, dict):
            return _coerce_imports_list(d.get('imports'))
    except Exception:
        pass
    return []


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
    cr = 'SAMOOHA_CLEANROOM_%s' % cleanroom_uuid
    return '@%s.APP.CODE/%s/%s' % (cr, patch_prefix, rel)


def _fetch_load_python_and_stage(session, cleanroom_name, is_provider):
    if not is_provider:
        return [], [], None
    uuid = None
    try:
        p_res = session.sql("CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.PROVIDER.VIEW_CLEANROOMS()").collect()
        for r in p_res:
            d = {k.upper(): v for k, v in r.as_dict().items()}
            c_name = d.get('CLEANROOM_NAME') or d.get('NAME')
            if c_name and str(c_name).upper().replace(' ', '_') == cleanroom_name.upper().replace(' ', '_'):
                uuid = d.get('CLEANROOM_ID') or d.get('ID')
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
        arguments = [{'name': names[i], 'type': _sql_type_from_legacy(types[i])} for i in range(len(types))]
        pkg_raw = row.get('PACKAGES') or ''
        packages = [p.strip() for p in str(pkg_raw).replace(';', ',').split(',') if p.strip()]
        returns = _sql_type_from_legacy(row.get('RETURN_TYPE'))
        body = _normalize_code_body_for_yaml(row.get('BODY') or '')
        imports_list = _extract_imports_from_row(row)
        use_stage = bool(imports_list) and bool(cleanroom_uuid)

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
            if not str(body).strip():
                continue
            fn_entry['code_body'] = body
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
    try:
        p_res = session.sql("CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.PROVIDER.VIEW_CLEANROOMS()").collect()
        for r in p_res:
            d = {k.upper(): v for k, v in r.as_dict().items()}
            c_name = d.get('CLEANROOM_NAME') or d.get('NAME')
            c_id = d.get('CLEANROOM_ID') or d.get('ID')
            c_state = d.get('STATE') or d.get('STATUS')
            # FIX: Normalize name
            if c_name and c_name.upper().replace(' ', '_') == cleanroom_name.upper().replace(' ', '_'):
                if str(c_name).upper().replace(' ', '_') == str(c_id).upper().replace(' ', '_') and c_state == 'CREATED': is_provider = True
                break
    except: pass
    
    is_consumer = False
    if not is_provider:
        try: is_consumer = session.call("SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.CONSUMER.IS_ENABLED", cleanroom_name)
        except: pass

    if not is_provider and not is_consumer:
        return {'templates': [], 'python_code_spec': None, 'python_udf_names': [], 'python_stage_files': []}

    py_rows, stage_files, _uuid = _fetch_load_python_and_stage(session, cleanroom_name, is_provider)
    ver_str = "MIGRATION_V1"
    safe_cn = re.sub(r'[^a-zA-Z0-9_]', '_', cleanroom_name)[:50].strip('_') or 'cleanroom'
    code_spec_name = 'migrated_py_%s' % safe_cn
    py_code_yaml = _build_python_code_spec_yaml(cleanroom_name, py_rows, code_spec_name, ver_str, _uuid, stage_files) if py_rows else None
    py_fn_names = [str(r.get('FUNCTION_NAME')) for r in py_rows if r.get('FUNCTION_NAME')]
    code_spec_bundle_id = '%s_%s' % (code_spec_name, ver_str)

    df = pd.DataFrame()
    if is_provider:
        df_res = session.sql(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.PROVIDER.VIEW_ADDED_TEMPLATES('{cleanroom_name}')").collect()
        if df_res: df = pd.DataFrame([r.as_dict() for r in df_res])
        try:
            req_res = session.sql(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.PROVIDER.VIEW_TEMPLATE_REQUESTS('{cleanroom_name}')").collect()
            if req_res: 
                df2 = pd.DataFrame([r.as_dict() for r in req_res])
                df = pd.concat([df, df2], ignore_index=True)
        except: pass
    elif is_consumer:
        try:
            df_res = session.sql(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.CONSUMER.LIST_TEMPLATE_REQUESTS('{cleanroom_name}')").collect()
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
    
    ver_str = "MIGRATION_V1" 

    specs = []
    for _, r in df_norm.iterrows():
        t_name = r.get('TEMPLATE_NAME')
        t_sql = str(r.get('TEMPLATE') or r.get('SQL_TEXT') or '')
        
        if not t_name or not t_sql: continue

        if '\\n' in t_sql:
            t_sql = t_sql.replace('\\n', '\n')
        if '\\t' in t_sql:
            t_sql = t_sql.replace('\\t', '\t')

        is_activation = "cleanroom.activation_" in t_sql.lower()
        cleaned_sql = t_sql
        if not is_activation:
            match = re.search(r"CREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+.*?\s+AS\s*(.*?);", t_sql, re.IGNORECASE | re.DOTALL)
            if match: cleaned_sql = match.group(1).strip()

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

        specs.append(yaml_out)
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
        if c in ('hem', 'hashed_email', 'h_email') or 'email' in c:
            if 'b64' in c: return 'hashed_email_b64_encoded'
            if c == 'email': return 'email'
            return 'hashed_email_sha256'
        if c in ('hpn', 'hashed_phone', 'h_phone') or 'phone' in c:
            if 'b64' in c: return 'hashed_phone_b64_encoded'
            if c == 'phone': return 'phone'
            return 'hashed_phone_sha256'
        if 'ip' in c and 'zip' not in c:
            if 'b64' in c: return 'hashed_ip_address_b64_encoded'
            if 'hash' in c or 'sha256' in c: return 'hashed_ip_address_sha256'
            return 'ip_address'
        if 'device' in c or 'idfa' in c or 'maid' in c:
            if 'b64' in c: return 'hashed_device_b64_encoded'
            if 'hash' in c or 'sha256' in c: return 'hashed_device_id_sha256'
            return 'device_id'
        if 'first_name' in c or c == 'fname':
            if 'b64' in c: return 'hashed_first_name_b64_encoded'
            if 'hash' in c or 'sha256' in c: return 'hashed_first_name_sha256'
            return 'first_name'
        return None
    
    def refine_type_by_data(table, col, proposed_type):
        if not proposed_type or 'sha256' not in proposed_type: return proposed_type
        try:
            res = session.sql(f"SELECT {col} FROM {table} WHERE {col} IS NOT NULL LIMIT 1").collect()
            if not res: return proposed_type
            val = str(res[0][0])
            if len(val) == 44 or val.endswith('='):
                 if 'email' in proposed_type: return 'hashed_email_b64_encoded'
                 if 'phone' in proposed_type: return 'hashed_phone_b64_encoded'
                 if 'ip' in proposed_type: return 'hashed_ip_address_b64_encoded'
                 if 'device' in proposed_type: return 'hashed_device_b64_encoded'
            return proposed_type
        except: return proposed_type

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
    try:
        p_res = session.sql("CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.PROVIDER.VIEW_CLEANROOMS()").collect()
        for r in p_res:
            d = {k.upper(): v for k, v in r.as_dict().items()}
            c_name = d.get('CLEANROOM_NAME') or d.get('NAME')
            c_id = d.get('CLEANROOM_ID') or d.get('ID')
            c_state = d.get('STATE') or d.get('STATUS')
            # FIX: Normalize name
            if c_name and c_name.upper().replace(' ', '_') == cleanroom_name.upper().replace(' ', '_'):
                if str(c_name).upper().replace(' ', '_') == str(c_id).upper().replace(' ', '_') and c_state == 'CREATED': is_provider = True
                break
    except: pass

    is_consumer = False
    if not is_provider:
        try:
            is_consumer = session.call("SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.CONSUMER.IS_ENABLED", cleanroom_name)
        except: pass

    if not is_provider and not is_consumer:
        return []

    tables_data = [] 
    join_df = pd.DataFrame()
    col_df = pd.DataFrame()
    act_df = pd.DataFrame()
    prov_join_df = pd.DataFrame()
    prov_col_df = pd.DataFrame()

    if is_provider:
        prov_res = session.sql(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.PROVIDER.view_provider_datasets('{cleanroom_name}')").collect()
        if prov_res:
            for row in prov_res:
                d = {k.upper(): v for k, v in row.as_dict().items()}
                t_name = d.get('TABLE_NAME')
                if t_name:
                    tables_data.append({'TABLE_NAME': t_name, 'SQL_ENABLED': d.get('SQL_ENABLED', False)})
        
        cr = session.sql(f"SELECT * FROM SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.PUBLIC.CLEANROOM_RECORD WHERE UPPER(CLEANROOM_NAME) = '{cleanroom_name.upper()}'").collect()
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
        cons_res = session.sql(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.CONSUMER.view_consumer_datasets('{cleanroom_name}')").collect()
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
            join_df = get_df_upper(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.CONSUMER.view_join_policy('{cleanroom_name}')")
        except: pass
        try:
            col_df = get_df_upper(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.CONSUMER.view_column_policy('{cleanroom_name}')")
        except: pass
        try: act_df = get_df_upper(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.CONSUMER.view_activation_policy('{cleanroom_name}')")
        except: pass
        try:
            prov_join_df = get_df_upper(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.CONSUMER.view_provider_join_policy('{cleanroom_name}')")
        except: pass
        try:
            prov_col_df = get_df_upper(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.CONSUMER.view_provider_column_policy('{cleanroom_name}')")
        except: pass

    if not tables_data: return []

    specs = []
    ver_str = "MIGRATION_V1"

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
                    policy = {'category': 'join_standard'}
                    if gtype:
                        policy['column_type'] = gtype
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
                     
        if not schema_policies:
            try:
                desc_res = session.sql(f"DESC TABLE {t_name}").collect()
                if desc_res:
                    first_col = desc_res[0]['name']
                    schema_policies[first_col] = {'category': 'passthrough'}
            except: 
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
            dataset_obj['freeform_sql_policies'] = {}
            dataset_obj['require_freeform_sql_policy'] = False

        spec = {
            'api_version': '2.0.0', 'spec_type': 'data_offering', 'name': safe_name,
            'version': ver_str, 'description': f"Migrated {t_name}",
            'datasets': [dataset_obj]
        }
        specs.append(yaml.dump(spec, default_flow_style=False, sort_keys=False))
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
        cr_res = session.sql(f"SELECT * FROM SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.PUBLIC.CLEANROOM_RECORD WHERE UPPER(CLEANROOM_NAME) = '{cleanroom_name.upper()}'").collect()
    except:
        cr_res = []

    prov_acct = "PROVIDER_ACCOUNT"
    if cr_res:
        cr = {k.upper(): v for k, v in cr_res[0].as_dict().items()}
        for k,v in cr.items():
            if "PROVIDER" in k and "LOCATOR" in k and v:
                prov_acct = v

    if prov_acct == "PROVIDER_ACCOUNT" or '.' not in prov_acct:
        try:
            curr_org = session.sql("SELECT CURRENT_ORGANIZATION_NAME()").collect()[0][0]
            curr_acct = session.sql("SELECT CURRENT_ACCOUNT_NAME()").collect()[0][0]
            if prov_acct == "PROVIDER_ACCOUNT":
                prov_acct = f"{curr_org}.{curr_acct}"
            else:
                prov_acct = f"{curr_org}.{prov_acct}"
        except: pass

    cons_acct = "CONSUMER_ACCOUNT"
    consumer_resolved = False
    try:
        cons_res = session.sql(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.PROVIDER.VIEW_CONSUMERS('{cleanroom_name}')").collect()
        if cons_res:
            c_dict = {k.upper(): v for k, v in cons_res[0].as_dict().items()}
            for k,v in c_dict.items():
                if ('NAME' in k or 'ACCOUNT' in k) and v:
                    cons_acct = str(v)
                    consumer_resolved = True
                    break
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

    ver_str = "MIGRATION_V1"

    safe_collab_name = f"migrated_{cleanroom_name.replace(' ', '_')}"

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

CREATE OR REPLACE PROCEDURE DCR_SNOWVA.MIGRATION.GENERATE_ANALYSIS_SCRIPT(CLEANROOM_NAME STRING, TEMPLATE_NAME STRING)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('snowflake-snowpark-python', 'pandas', 'pyyaml')
HANDLER = 'gen_run_script'
EXECUTE AS CALLER
AS
$$
import yaml
import pandas as pd

def gen_run_script(session, cleanroom_name, template_name):
    collab_name = f"migrated_{cleanroom_name}"
    target_template = None
    try:
        df = session.sql(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.PROVIDER.VIEW_ADDED_TEMPLATES('{cleanroom_name}')").collect()
        if df:
            for row in df:
                r = {k.upper(): v for k, v in row.as_dict().items()}
                t_name = r.get('TEMPLATE_NAME')
                if t_name and template_name and t_name.lower() == template_name.lower():
                    target_template = r; break
    except: pass
            
    if not target_template: return f"-- Template '{template_name}' not found."

    migrated_name = f"migrated_{target_template['TEMPLATE_NAME']}"
    raw_params = target_template.get('PARAMETERS')
    is_activation = "cleanroom.activation_" in str(target_template.get('TEMPLATE', '')).lower()
    
    args = {}
    if raw_params: args['param_1'] = "<VAL>"

    if is_activation:
        args['collaborator_name'] = "<DESTINATION_ACCOUNT_ALIAS>"
        args['segment_name'] = "output_table"
        args['activation_column'] = ["<COL>"]

    view_mappings = {"source_tables": ["<REPLACE_WITH_TABLE>"]}
    actual_template_id = f"{migrated_name}_MIGRATION_V1"

    analysis_spec = {
        "api_version": "2.0.0",
        "spec_type": "analysis",
        "name": f"run_{migrated_name}",
        "description": f"Execution of {migrated_name}",
        "template": actual_template_id, 
        "template_configuration": {
            "view_mappings": view_mappings
        },
        "arguments": args
    }
    
    yaml_str = yaml.dump(analysis_spec)
    dd = "$" + "$"
    return f"CALL samooha_by_snowflake_local_db.collaboration.run('{collab_name}', {dd}{yaml_str}{dd});"
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
        is_provider = False
        try:
            p_res = session.sql("CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.PROVIDER.VIEW_CLEANROOMS()").collect()
            for r in p_res:
                d = {k.upper(): v for k, v in r.as_dict().items()}
                c_name = d.get('CLEANROOM_NAME') or d.get('NAME')
                c_id = d.get('CLEANROOM_ID') or d.get('ID')
                c_state = d.get('STATE') or d.get('STATUS')
                if c_name and c_name.upper().replace(' ', '_') == cleanroom_name.upper().replace(' ', '_'):
                    if str(c_name).upper().replace(' ', '_') == str(c_id).upper().replace(' ', '_') and c_state == 'CREATED':
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
        return result_str

    try:
        laf_info = ""
        is_account_laf = False
        try:
             res = session.sql("CALL samooha_by_snowflake_local_db.library.is_laf_enabled_on_account()").collect()
             if res and str(res[0][0]).upper() == 'TRUE':
                 is_account_laf = True
        except: pass

        is_cr_laf = False
        try:
             # Try checking specific cleanroom LAF status (Consumer side)
             is_cr_laf = session.call("SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.CONSUMER.IS_LAF_ENABLED_FOR_CLEANROOM", cleanroom_name)
        except: pass
        
        if is_cr_laf:
            laf_info = " Note: This cleanroom utilizes Cross-Cloud Auto-Fulfillment (LAF)."
        elif is_account_laf:
            laf_info = " Note: LAF is enabled on this account. Please verify if this Cleanroom uses it."

        check = session.call("DCR_SNOWVA.MIGRATION.CHECK_PREREQUISITES", cleanroom_name)
        if isinstance(check, str): check = json.loads(check)
        prereq_warnings = check.get('warnings', [])
        if check.get('status') == 'FAIL':
             errors = check.get('errors', [])
             msg = " | ".join(errors) if errors else "Prerequisites check failed."
             return _finish(json.dumps({"status": "ERROR", "message": msg, "warnings": prereq_warnings}))
        
        is_provider = False
        try:
            p_res = session.sql("CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.PROVIDER.VIEW_CLEANROOMS()").collect()
            for r in p_res:
                d = {k.upper(): v for k, v in r.as_dict().items()}
                c_name = d.get('CLEANROOM_NAME') or d.get('NAME')
                c_id = d.get('CLEANROOM_ID') or d.get('ID')
                c_state = d.get('STATE') or d.get('STATUS')
                if c_name and c_name.upper().replace(' ', '_') == cleanroom_name.upper().replace(' ', '_'):
                    if str(c_name).upper().replace(' ', '_') == str(c_id).upper().replace(' ', '_') and c_state == 'CREATED': is_provider = True
                    break
        except: pass
        
        is_consumer = False
        if not is_provider:
             try:
                 is_consumer = session.call("SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.CONSUMER.IS_ENABLED", cleanroom_name)
             except: pass
             
        if not is_provider and not is_consumer:
             return _finish(json.dumps({"status": "ERROR", "message": f"Cleanroom '{cleanroom_name}' not found or access denied."}))

        role_type = "PROVIDER" if is_provider else "CONSUMER"
        job_role = role_type
        
        safe_collab_name = f"migrated_{cleanroom_name.replace(' ', '_')}"
        
        if action == 'TEARDOWN':
            res = session.call("DCR_SNOWVA.MIGRATION.TEARDOWN", safe_collab_name)
            return _finish(json.dumps({"status": "SUCCESS", "message": res}))

        res_tmps = session.call("DCR_SNOWVA.MIGRATION.GENERATE_TEMPLATE_SPECS", cleanroom_name)
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
        
        res_dos = session.call("DCR_SNOWVA.MIGRATION.GENERATE_DATA_OFFERING_SPECS", cleanroom_name)
        dos = json.loads(res_dos) if isinstance(res_dos, str) else res_dos

        if not dos and not tmps and action != 'TEARDOWN' and action != 'CHECK_STATUS' and action != 'JOIN':
             if role_type == 'PROVIDER':
                  laf_info += " WARNING: No data offerings or templates were found. The collaboration will be created without them."

        script_lines = []
        script_lines.append("USE ROLE SAMOOHA_APP_ROLE;")
        script_lines.append("USE SECONDARY ROLES NONE;")
        script_lines.append(f"-- MIGRATION SCRIPT FOR: {cleanroom_name} ({role_type})")
        script_lines.append("-- Generated via DCR_SNOWVA.MIGRATION Package\n")

        step_n = 0
        if py_code_spec:
            script_lines.append("-- [0] REGISTER PYTHON CODE SPEC (Collaboration custom functions / UDFs)")
            script_lines.append("-- Ref: https://docs.snowflake.com/en/user-guide/cleanrooms/v2/custom-functions")
            script_lines.append(f"CALL samooha_by_snowflake_local_db.registry.register_code_spec({dd}\n{py_code_spec}\n{dd});\n")
            step_n = 1

        if tmps:
            script_lines.append(f"-- [{step_n}] REGISTER TEMPLATES ({len(tmps)} found)")
            for y_str in tmps:
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
             prov_ds_df = session.sql(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.PROVIDER.view_provider_datasets('{cleanroom_name}')").collect()
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
             for y_str in tmps:
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
                     cr_rec = session.sql(f"SELECT * FROM SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.PUBLIC.CLEANROOM_RECORD WHERE UPPER(CLEANROOM_NAME) = '{cleanroom_name.upper()}'").collect()
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
                 pr_res = session.sql(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.LIBRARY.IS_PROVIDER_RUN_ENABLED('{cleanroom_name}')").collect()
                 if pr_res and "provider side run analysis is enabled" in str(pr_res[0][0]).lower():
                     is_prov_run = True
             except: pass

             collab_yml = session.call("DCR_SNOWVA.MIGRATION.GENERATE_COLLABORATION_SPEC", cleanroom_name, prov_ids, [], tmp_ids, has_activation)

             script_lines.append(f"\n-- [{step_n}] CREATE COLLABORATION: {safe_collab_name}")
             script_lines.append(f"CALL samooha_by_snowflake_local_db.collaboration.initialize({dd}\n{collab_yml}\n{dd});\n")
             script_lines.append("-- NOTE: Under analysis_runners.Consumer_Account.data_providers you should see BOTH")
             script_lines.append("-- Provider_Account (your linked table, e.g. CUSTOMERS) AND Consumer_Account.")
             script_lines.append("-- Consumer_Account.data_offerings is [] until the consumer registers their dataset and links it.")
             script_lines.append("-- Lookalike-style templates use source_table (provider) and my_table (consumer); both must appear in the spec.\n")
             script_lines.append(f"-- Wait for status 'CREATED' before joining")
             script_lines.append(f"CALL samooha_by_snowflake_local_db.collaboration.get_status('{safe_collab_name}');\n")
             script_lines.append(f"-- [{step_n + 1}] JOIN COLLABORATION (Self-Join for Provider)")
             script_lines.append(f"CALL samooha_by_snowflake_local_db.collaboration.join('{safe_collab_name}');\n")
             if is_prov_run:
                 script_lines.append(f"-- [5] PROVIDER-RUN ANALYSIS DETECTED")
                 script_lines.append(f"-- This legacy cleanroom has provider-run analysis enabled.")
                 script_lines.append(f"-- The consumer MUST run their own migration to register data offerings with policies,")
                 script_lines.append(f"-- then link them to this collaboration after joining.")
                 script_lines.append(f"-- Consumer data offerings are listed as empty in the collaboration spec above")
                 script_lines.append(f"-- because the provider cannot see consumer datasets.\n")
             script_lines.append(f"-- CONSUMER: After reviewing and joining, the consumer should register their data offerings")
             script_lines.append(f"-- and link them to the collaboration to update the spec:")
             script_lines.append(f"-- CALL samooha_by_snowflake_local_db.registry.register_data_offering(<data_offering_spec>);")
             script_lines.append(f"-- CALL samooha_by_snowflake_local_db.collaboration.link_data_offering('{safe_collab_name}', '<data_offering_id>', ['Provider_Account']);")
        else:
             if not tmps and not dos:
                 script_lines.append(f"\n-- NOTE: No templates or data offerings found on the consumer side.")
                 script_lines.append(f"-- The provider must run migration first to create the collaboration.")
                 script_lines.append(f"-- Once the collaboration is created, run the following steps:\n")

             owner_acct = "REPLACE_WITH_PROVIDER_ORG.ACCOUNT"
             try:
                 cr_rec = session.sql(f"SELECT * FROM SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.PUBLIC.CLEANROOM_RECORD WHERE UPPER(CLEANROOM_NAME) = '{cleanroom_name.upper()}'").collect()
                 if cr_rec:
                     cr_d = {k.upper(): v for k, v in cr_rec[0].as_dict().items()}
                     for k, v in cr_d.items():
                         if "PROVIDER" in k and "LOCATOR" in k and v:
                             owner_acct = v
                             break
             except: pass
             if owner_acct == "REPLACE_WITH_PROVIDER_ORG.ACCOUNT":
                 try:
                     curr_org = session.sql("SELECT CURRENT_ORGANIZATION_NAME()").collect()[0][0]
                     owner_acct = f"{curr_org}.REPLACE_WITH_PROVIDER_ACCOUNT"
                 except: pass

             script_lines.append(f"\n-- [3] REVIEW COLLABORATION (requires owner account identifier)")
             script_lines.append(f"CALL samooha_by_snowflake_local_db.collaboration.review('{safe_collab_name}', '{owner_acct}');\n")
             script_lines.append(f"-- [4] JOIN COLLABORATION")
             script_lines.append(f"CALL samooha_by_snowflake_local_db.collaboration.join('{safe_collab_name}');\n")

             if dos:
                 script_lines.append(f"-- [5] LINK CONSUMER DATA OFFERINGS (run after join)")
                 script_lines.append(f"-- link_data_offering shares your data with the specified analysis runner")
                 script_lines.append(f"-- and updates the collaboration spec. This enables policy enforcement.\n")
                 for y_str in dos:
                     spec = yaml.safe_load(y_str)
                     do_id = f"{spec['name']}_{spec['version']}"
                     script_lines.append(f"CALL samooha_by_snowflake_local_db.collaboration.link_data_offering('{safe_collab_name}', '{do_id}', ['Provider_Account']);\n")

        full_script_text = "\n".join(script_lines)

        if action == 'PLAN':
            t_count = len(tmps) if tmps else 0
            d_count = len(dos) if dos else 0
            py_note = ""
            if py_udf_names:
                py_note = " Python UDFs from LOAD_PYTHON_RECORD: %s." % (", ".join(py_udf_names[:20]))
            plan_payload = {
                "status": "READY_TO_MIGRATE",
                "role": role_type,
                "summary": f"Found {t_count} templates and {d_count} datasets.{py_note}{laf_info}",
                "generated_script": full_script_text,
                "next_step": "Ask user to confirm execution.",
                "details": {
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
            return _finish(json.dumps(plan_payload))

        elif action == 'EXECUTE':
            actions_taken = []
            
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
                for y_str in tmps:
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
                collab_yml = session.call("DCR_SNOWVA.MIGRATION.GENERATE_COLLABORATION_SPEC", cleanroom_name, prov_ids, [], tmp_ids, has_activation)
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
                    cr_rec = session.sql(f"SELECT * FROM SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.PUBLIC.CLEANROOM_RECORD WHERE UPPER(CLEANROOM_NAME) = '{cleanroom_name.upper()}'").collect()
                    if cr_rec:
                        cr_d = {k.upper(): v for k, v in cr_rec[0].as_dict().items()}
                        for k, v in cr_d.items():
                            if "PROVIDER" in k and "LOCATOR" in k and v:
                                owner_acct = v
                                break
                except: pass
                actions_taken.append("Consumer artifacts registered.")
                return _finish(json.dumps({
                    "status": "SUCCESS",
                    "message": f"Consumer artifacts registered for '{cleanroom_name}'.",
                    "actions": actions_taken,
                    "role": "CONSUMER",
                    "collab_name": safe_collab_name,
                    "owner_account": owner_acct,
                    "next_step": "REVIEW_AND_JOIN"
                }))

        elif action == 'CHECK_STATUS':
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
                            cr_rec = session.sql(f"SELECT * FROM SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.PUBLIC.CLEANROOM_RECORD WHERE UPPER(CLEANROOM_NAME) = '{cleanroom_name.upper()}'").collect()
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
            report = session.call("DCR_SNOWVA.MIGRATION.VALIDATE", cleanroom_name, safe_collab_name)
            return _finish(str(report))

        elif action == 'GENERATE_ANALYSIS':
            return _finish(json.dumps({"status": "SUCCESS", "message": "Analysis generation not fully supported in Consumer-empty mode yet."}))

        elif action == 'TEARDOWN':
            res = session.call("DCR_SNOWVA.MIGRATION.TEARDOWN", safe_collab_name)
            return _finish(json.dumps({"status": "SUCCESS", "message": res}))
        
        else: return _finish(json.dumps({"status": "ERROR", "message": "INVALID MODE"}))

    except Exception as e:
        return _finish(json.dumps({"status": "ERROR", "message": str(e)}))
$$;
