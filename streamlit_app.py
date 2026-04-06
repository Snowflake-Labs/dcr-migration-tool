# Copyright 2026 Snowflake Inc.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import streamlit as st
import snowflake.snowpark as snowpark
import json
import re
import pandas as pd
import time
from snowflake.snowpark.context import get_active_session

# --- PAGE CONFIG & CSS ---
st.set_page_config(layout="wide", page_title="DCR Migration Tool", page_icon="❄️")


st.markdown("""
<style>
    .stTabs [data-baseweb="tab-list"] {
        gap: 24px;
    }
    .stTabs [data-baseweb="tab"] {
        height: 50px;
        white-space: pre-wrap;
        background-color: transparent;
        border-radius: 4px 4px 0px 0px;
        gap: 1px;
        padding-top: 10px;
        padding-bottom: 10px;
    }
    .stTabs [aria-selected="true"] {
        background-color: rgba(41, 181, 232, 0.1);
        border-bottom: 2px solid #29B5E8;
    }
    .metric-container {
        border: 1px solid #e0e0e0;
        padding: 10px;
        border-radius: 5px;
        text-align: center;
        background-color: #0e1117;

    
    [data-testid="stMetricValue"] {
        font-size: 24px !important;
    }

    }
</style>
""", unsafe_allow_html=True)

# --- BACKEND FUNCTIONS ---

def get_session():
    try:
        return get_active_session()
    except:
        return None

session = get_session()

def _looks_like_uuid(name):
    """Check if a cleanroom name looks like an internal UUID rather than a human-readable name."""
    if not name:
        return True
    clean = name.replace('-', '').replace('_', '').strip()
    if re.fullmatch(r'[0-9a-fA-F]{20,}', clean):
        return True
    uuid_pattern = r'[0-9a-fA-F]{8}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{12}'
    if re.fullmatch(uuid_pattern, name.strip()):
        return True
    return False

def list_cleanrooms():
    """Fetch available P&C API cleanrooms for the picker."""
    rooms = []
    try:
        p_res = session.sql("CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.PROVIDER.VIEW_CLEANROOMS()").collect()
        if p_res:
            for r in p_res:
                d = {k.upper(): v for k, v in r.as_dict().items()}
                name = d.get('CLEANROOM_NAME') or d.get('NAME')
                cid = d.get('CLEANROOM_ID') or d.get('ID')
                state = d.get('STATE') or d.get('STATUS') or ''
                if _looks_like_uuid(name):
                    rooms.append({"name": name, "role": "PROVIDER", "state": state, "api_room": False, "reason": "internal UUID"})
                    continue
                is_api = str(name).upper().replace(' ', '_') == str(cid).upper().replace(' ', '_') if name and cid else False
                rooms.append({"name": name, "role": "PROVIDER", "state": state, "api_room": is_api})
    except:
        pass
    try:
        c_res = session.sql("CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.CONSUMER.VIEW_CLEANROOMS()").collect()
        if c_res:
            existing_names = {r['name'].upper() for r in rooms if r.get('name')}
            for r in c_res:
                d = {k.upper(): v for k, v in r.as_dict().items()}
                name = d.get('CLEANROOM_NAME') or d.get('NAME')
                state = d.get('STATE') or d.get('STATUS') or ''
                if name and name.upper() not in existing_names:
                    if _looks_like_uuid(name):
                        rooms.append({"name": name, "role": "CONSUMER", "state": state, "api_room": False, "reason": "internal UUID"})
                    else:
                        rooms.append({"name": name, "role": "CONSUMER", "state": state, "api_room": True})
    except:
        pass
    return rooms

def list_collab_dcrs():
    """Fetch migrated DCRs directly from MIGRATION_JOBS table."""
    collabs = []
    try:
        jobs = session.sql("""
            SELECT JOB_ID, CLEANROOM_NAME, ACTION, STATUS, FINISHED_AT
            FROM DCR_SNOWVA.MIGRATION.MIGRATION_JOBS
            WHERE STATUS IN ('SUCCESS', 'READY_TO_MIGRATE')
            ORDER BY FINISHED_AT DESC
        """).collect()

        seen = {}
        for j in jobs:
            d = {k.upper(): v for k, v in j.as_dict().items()}
            cr = d.get('CLEANROOM_NAME', '')
            if not cr or cr.upper() in seen:
                continue
            seen[cr.upper()] = True

            collab_name = f"migrated_{re.sub(r'[^a-zA-Z0-9]', '', cr)[:56] or 'cleanroom'}"
            try:
                rsv = session.call("DCR_SNOWVA.MIGRATION.RESOLVE_CLEANROOM_FOR_MIGRATION", cr)
                if isinstance(rsv, str):
                    try:
                        rsv = json.loads(rsv)
                    except Exception:
                        rsv = {}
                if isinstance(rsv, dict) and rsv.get("status") == "OK":
                    uid = rsv.get("cleanroom_uuid") or rsv.get("api_cleanroom_name") or cr
                    collab_name = f"migrated_{re.sub(r'[^a-zA-Z0-9]', '', str(uid))[:56] or 'cleanroom'}"
            except Exception:
                pass
            status = ''
            safe_collab = collab_name.replace("'", "''")
            try:
                st_res = session.sql(
                    f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.COLLABORATION.GET_STATUS('{safe_collab}')"
                ).collect()
                if st_res:
                    row = {k.upper(): v for k, v in st_res[0].as_dict().items()}
                    status = row.get('STATUS') or row.get('STATE') or ''
            except:
                pass

            collabs.append({
                "name": collab_name,
                "status": status or d.get('STATUS', ''),
                "owner": "",
                "created": str(d.get('FINISHED_AT', '')),
                "source_pnc": cr.upper(),
                "migration_history": {
                    "migrated_from_pnc": True,
                    "source_cleanroom": cr,
                    "migration_timestamp": str(d.get('FINISHED_AT', '')),
                    "migration_job_id": d.get('JOB_ID', ''),
                }
            })
    except Exception as e:
        st.warning(f"Could not fetch migration jobs: {str(e)[:300]}")

    return collabs

def get_migration_plan(cleanroom_name):
    try:
        res_str = session.call("DCR_SNOWVA.MIGRATION.AGENT_MIGRATE_ORCHESTRATOR", cleanroom_name, 'PLAN')
        if not res_str: return {"status": "ERROR", "message": "Empty response from backend."}
        plan = json.loads(res_str)
        
        if plan.get("status") == "ERROR":
            msg = plan.get('message', '')
            if "not found" in msg.lower() or "not installed" in msg.lower() or "CleanroomNotInstalled" in msg:
                st.error(
                    f"Cleanroom '{cleanroom_name}' was not found. Use the display name or cleanroom UUID from "
                    "'List Cleanrooms', or the consumer install UUID. Provider UI cleanrooms accept the human-readable name."
                )
            elif "laf" in msg.lower():
                st.error(f"Cleanroom '{cleanroom_name}' uses LAF (Cross-Cloud Auto-Fulfillment). LAF cleanroom migration is not supported.")
            elif "prerequisites" in msg.lower():
                st.error(f"Prerequisites failed: {msg}")
            else:
                st.error(f"Migration Error: {msg}")
            warnings = plan.get('warnings', [])
            for w in warnings:
                st.warning(w)
            return None
        
        plan['cleanroom_name'] = cleanroom_name
        det = plan.get('details') or {}
        if det.get('is_ui_cleanroom'):
            st.info(
                "UI cleanroom: the collaboration will be named from the cleanroom id. "
                "Platform-privacy templates listed under details are not auto-migrated as template specs."
            )
        return plan
    except Exception as e:
        st.error(f"Orchestration Error: {e}")
        return None

def execute_migration(cleanroom_name):
    try:
        res_str = session.call("DCR_SNOWVA.MIGRATION.AGENT_MIGRATE_ORCHESTRATOR", cleanroom_name, 'EXECUTE')
        return json.loads(res_str)
    except Exception as e:
        return {"status": "ERROR", "message": str(e)}

def initialize_collaboration(collab_spec):
    """Call INITIALIZE directly from Streamlit (not inside a stored procedure)."""
    try:
        spec = collab_spec.strip()
        res = session.sql(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.COLLABORATION.INITIALIZE($$\n{spec}\n$$)").collect()
        collab_name = ""
        msg = ""
        if res:
            rd = {k.upper(): v for k, v in res[0].as_dict().items()}
            collab_name = rd.get('COLLABORATION_NAME', '')
            msg = rd.get('MESSAGE', str(rd))
        return {"status": "SUCCESS", "message": msg, "collaboration_name": collab_name}
    except Exception as e:
        err = str(e)
        if "already exists" in err.lower():
            return {"status": "SUCCESS", "message": "Collaboration already exists.", "already_exists": True}
        return {"status": "ERROR", "message": err}

def review_collaboration(collab_name, owner_account):
    """Call REVIEW directly from Streamlit."""
    try:
        if owner_account:
            session.sql(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.COLLABORATION.REVIEW('{collab_name}', '{owner_account}')").collect()
        else:
            session.sql(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.COLLABORATION.REVIEW('{collab_name}')").collect()
        return {"status": "SUCCESS", "message": "Review complete."}
    except Exception as e:
        return {"status": "ERROR", "message": str(e)}

def join_collaboration_direct(collab_name):
    """Call JOIN directly from Streamlit (not inside a stored procedure)."""
    try:
        session.sql(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.COLLABORATION.JOIN('{collab_name}')").collect()
        return {"status": "SUCCESS", "message": "Join submitted. Check status to confirm."}
    except Exception as e:
        return {"status": "ERROR", "message": str(e)}

def check_status(cleanroom_name):
    try:
        res_str = session.call("DCR_SNOWVA.MIGRATION.AGENT_MIGRATE_ORCHESTRATOR", cleanroom_name, 'CHECK_STATUS')
        return json.loads(res_str)
    except Exception as e:
        return {"status": "ERROR", "message": str(e)}

def run_validation(cleanroom_name):
    try:
        res_str = session.call("DCR_SNOWVA.MIGRATION.AGENT_MIGRATE_ORCHESTRATOR", cleanroom_name, 'VALIDATE')
        
        # Handle potential double-encoded JSON
        try: result = json.loads(res_str)
        except: 
            import ast
            result = ast.literal_eval(res_str)
            
        if isinstance(result, str):
            result = json.loads(result)
            
        return result
    except Exception as e:
        return {"overall_status": "ERROR", "error": str(e)}

def execute_teardown(cleanroom_name):
    try:
        res_str = session.call("DCR_SNOWVA.MIGRATION.AGENT_MIGRATE_ORCHESTRATOR", cleanroom_name, 'TEARDOWN')
        return json.loads(res_str)
    except Exception as e:
        return {"status": "ERROR", "message": str(e)}

def get_manual_sql_scripts(plan):
    details = plan.get("details", {})
    collab_name = details.get("target_collaboration", f"migrated_{plan.get('cleanroom_name', '').replace(' ', '_')}")
    role = plan.get('role', 'UNKNOWN')
    
    # Finalize Script
    finalize_lines = [
        f"-- MANUAL FINALIZATION FOR {role} ({collab_name})",
        "USE ROLE SAMOOHA_APP_ROLE;",
        "",
        "-- 1. Check Status (Must be 'CREATED')",
        f"CALL samooha_by_snowflake_local_db.collaboration.get_status('{collab_name}');"
    ]
    if role == 'CONSUMER':
        finalize_lines.append(f"\n-- 2. Review\nCALL samooha_by_snowflake_local_db.collaboration.review('{collab_name}');")
    finalize_lines.append(f"\n-- 3. Join\nCALL samooha_by_snowflake_local_db.collaboration.join('{collab_name}');")

    # Cleanup Script
    cleanup_lines = [
        f"-- CLEANUP SCRIPT FOR {collab_name}",
        "USE ROLE SAMOOHA_APP_ROLE;",
        "",
        "-- 1. Teardown Collaboration",
        f"CALL samooha_by_snowflake_local_db.collaboration.teardown('{collab_name}');",
        f"CALL samooha_by_snowflake_local_db.collaboration.teardown('{collab_name}'); -- Call again to finalize",
        "",
        "-- 2. Drop Artifacts (If needed)"
    ]
    for t in details.get("templates", []):
        try: cleanup_lines.append(f"-- DROP TEMPLATE IF EXISTS ...; -- Check Name from Plan")
        except: pass
        
    return "\n".join(finalize_lines), "\n".join(cleanup_lines)


# --- MAIN APP UI ---

if not session:
    st.error("🚫 No active Snowpark session. Please run in Snowflake.")
    st.stop()

# Sidebar
with st.sidebar:
    st.title(" DCR Migration")
    st.caption("v2.1.0 Migration Toolkit")
    st.divider()

    btn_col1, btn_col2 = st.columns(2)
    if btn_col1.button("P&C Cleanrooms", use_container_width=True):
        with st.spinner("Fetching P&C cleanrooms..."):
            rooms = list_cleanrooms()
            st.session_state['available_rooms'] = rooms if rooms else []
            if not rooms:
                st.warning("No P&C cleanrooms found.")

    if btn_col2.button("Migrated DCRs", use_container_width=True):
        with st.spinner("Fetching collaboration DCRs..."):
            collabs = list_collab_dcrs()
            st.session_state['collab_dcrs'] = collabs if collabs else []
            if not collabs:
                st.info("No collaboration DCRs found.")
            else:
                migrated_count = sum(1 for c in collabs if c.get('source_pnc'))
                if migrated_count:
                    st.success(f"Found {migrated_count} migrated DCR(s)")
                else:
                    st.info(f"Found {len(collabs)} collaboration(s), none migrated from P&C.")

    # --- P&C DCR Dropdown (source for migration) ---
    if st.session_state.get('available_rooms'):
        rooms = st.session_state['available_rooms']
        api_rooms = [r for r in rooms if r.get('api_room')]
        non_api_rooms = [r for r in rooms if not r.get('api_room')]

        migrated_pnc_names = set()
        for c in st.session_state.get('collab_dcrs', []):
            if c.get('source_pnc'):
                migrated_pnc_names.add(c['source_pnc'])

        def _pnc_label(r):
            badge = ""
            if r['name'] and r['name'].upper().replace(' ', '_') in migrated_pnc_names:
                badge = " [migrated]"
            return f"{r['name']}  ({r['role']}, {r['state']}){badge}"

        room_options = ["-- Select --"] + [_pnc_label(r) for r in api_rooms]
        selected = st.selectbox("P&C Cleanrooms (source)", room_options)

        if non_api_rooms:
            ui_rooms = [r for r in non_api_rooms if r.get('reason') != 'internal UUID']
            uuid_rooms = [r for r in non_api_rooms if r.get('reason') == 'internal UUID']
            label_parts = []
            if ui_rooms:
                label_parts.append(f"{len(ui_rooms)} UI")
            if uuid_rooms:
                label_parts.append(f"{len(uuid_rooms)} UUID")
            with st.expander(f"Ineligible: {', '.join(label_parts)}"):
                for r in ui_rooms:
                    st.caption(f"{r['name']} ({r['role']}) - UI created")
                for r in uuid_rooms:
                    st.caption(f"{r['name'][:16]}... ({r['role']}) - internal UUID")

        if selected and selected != "-- Select --":
            cr_name_from_picker = selected.split("  (")[0].strip()
            cleanroom_input = st.text_input("Cleanroom Name", value=cr_name_from_picker, placeholder="e.g. mj_act_uc")
        else:
            cleanroom_input = st.text_input("Cleanroom Name", placeholder="e.g. mj_act_uc")
    else:
        cleanroom_input = st.text_input("Cleanroom Name", placeholder="e.g. mj_act_uc")

    # --- Migrated DCRs (from MIGRATION_JOBS) ---
    if st.session_state.get('collab_dcrs'):
        st.divider()
        collabs = st.session_state['collab_dcrs']
        st.caption(f"Migrated DCRs ({len(collabs)})")
        for c in collabs:
            history = c.get('migration_history', {})
            with st.expander(f"{c['name']}  —  {c['status']}"):
                st.json({"migration_history": history})

    if st.button("Generate Plan", type="primary", use_container_width=True):
        if not cleanroom_input:
            st.warning("Please enter a cleanroom name or select one from the list above.")
        else:
            with st.spinner("Analyzing environment..."):
                plan = get_migration_plan(cleanroom_input.strip())
                if plan:
                    st.session_state['plan'] = plan
                    st.session_state['active_tab'] = "Execute Setup"
                    st.session_state['collab_status'] = 'Not Started'
                    st.success("Plan Ready!")

# Main Content
if 'plan' not in st.session_state:
    st.info(" Enter P&C API Cleanroom Name in the sidebar to begin.")
    st.markdown("""
    ### Workflow
    1.  **Generate Plan:** Discover existing templates, datasets, and policies.
    2.  **Execute Setup:** Register artifacts and initialize the collaboration.
    3.  **Finalize:** Check status and join the new collaboration.
    4.  **Validate:** Verify object parity.
    """)
else:
    plan = st.session_state['plan']
    cr_name = plan['cleanroom_name']
    role = plan['role']
    details = plan.get('details', {})
    
    # Dashboard Metrics
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Role", role)
    _tpls = details.get("templates") or []
    _tpl_reg = sum(
        1
        for t in _tpls
        if not (isinstance(t, dict) and t.get("classification") == "PLATFORM_PRIVACY")
    )
    m2.metric("Templates", _tpl_reg)
    m3.metric("Data Offerings", len(details.get('provider_data', [])))
    m4.metric("Status", st.session_state.get('collab_status', 'Not Started'))

    if role == 'CONSUMER' and not details.get('provider_data'):
        st.warning("No consumer data offerings were detected. Ensure you have linked datasets and set join/column policies on the legacy cleanroom before migrating.")

    for w in plan.get('warnings', []):
        st.warning(w)

    notes = plan.get("migration_notes") or []
    if notes:
        with st.expander("Migration notes (legal terms, limits, freeform SQL)", expanded=False):
            for n in notes:
                st.markdown(f"- {n}")

    if details.get("has_python_code_spec"):
        udfs = details.get("python_udf_names") or []
        st.success(
            f"Python / custom UDFs: {len(udfs)} function(s) from LOAD_PYTHON_RECORD → REGISTER_CODE_SPEC "
            f"(inline `code_body` and/or stage `artifacts`+`imports` when `imports` is stored in metadata). UDFs: {', '.join(udfs[:15])}"
            + (" …" if len(udfs) > 15 else "")
        )
        st.caption("See: docs.snowflake.com/en/user-guide/cleanrooms/v2/custom-functions")
        sf = details.get("python_stage_files") or []
        if sf:
            with st.expander(f"Stage files @APP.CODE/V1_0P1 ({len(sf)})"):
                st.caption("Files on the cleanroom code stage (for reference).")
                st.text("\n".join(sf[:40]))

    st.divider()

    # Tabs
    tabs = st.tabs([" Review Plan", " Execute Setup", " Finalize (Join)", " Validate", " Cleanup"])
    
    # 1. REVIEW PLAN
    with tabs[0]:
        st.subheader("Generated Migration Script")
        st.caption("Review the SQL that will be executed.")
        st.code(plan.get('generated_script', '-- No script generated'), language='sql')

    # 2. EXECUTE SETUP
    with tabs[1]:
        st.subheader("Phase 1: Setup")
        st.info("This step registers templates/datasets and initializes the collaboration.")
        
        col1, col2 = st.columns([1, 2])
        if col1.button("Run Setup", type="primary"):
            with st.spinner("Registering templates and data offerings..."):
                res = execute_migration(cr_name)
            
            if res.get("status") == "SUCCESS":
                with st.expander("Registration Logs", expanded=True):
                    if res.get("message"):
                        st.info(res["message"])
                    actions = res.get("actions", [])
                    if actions:
                        for act in actions:
                            st.write(f"- {act}")
                
                collab_name = res.get("collab_name", "")
                collab_spec = res.get("collab_spec", "")
                role = res.get("role", "")
                
                if role == "PROVIDER" and collab_spec:
                    with st.spinner("Initializing collaboration..."):
                        init_res = initialize_collaboration(collab_spec)
                    if init_res.get("status") == "SUCCESS":
                        st.success(f"Collaboration initialized: {collab_name}")
                        if init_res.get("already_exists"):
                            st.info("Collaboration already existed.")
                        st.session_state['setup_complete'] = True
                        st.session_state['collab_name'] = collab_name
                    else:
                        st.error(f"Initialize failed: {init_res.get('message')}")
                elif role == "CONSUMER":
                    st.success("Consumer artifacts registered.")
                    st.session_state['setup_complete'] = True
                    st.session_state['collab_name'] = collab_name
                    st.session_state['owner_account'] = res.get("owner_account", "")
                    st.info("Proceed to **Finalize** tab to Review and Join the collaboration.")
            else:
                st.error(f"Setup Failed: {res.get('message')}")
                if res.get("actions"):
                    with st.expander("Partial Execution Logs", expanded=True):
                        for act in res["actions"]:
                            st.write(f"- {act}")

    # 3. FINALIZE
    with tabs[2]:
        st.subheader("Phase 2: Finalize")
        st.warning(
            "**Worksheet required for some steps:** `collaboration.initialize` and `collaboration.join` invoke "
            "`SYSTEM$ACCEPT_LEGAL_TERMS`, which **does not run** inside Streamlit or inside the migration stored procedure. "
            "If **Check Status** or **Run Setup** points you here with a FAIL/hint about legal terms or side effects, "
            "run the **Review Plan** SQL (or the snippet below) in a **Snowflake SQL Worksheet** as `SAMOOHA_APP_ROLE`."
        )

        final_sql, _ = get_manual_sql_scripts(plan)
        
        col1, col2 = st.columns(2)
        
        # Check Status
        if col1.button("Check Status"):
            with st.spinner("Checking Collaboration Status..."):
                res = check_status(cr_name)
                if res.get("status") == "SUCCESS":
                    status = res.get("collaboration_status", "UNKNOWN")
                    st.session_state['collab_status'] = status
                    if status == 'CREATED':
                        st.success(f"Status: **{status}** - Ready to Join!")
                    elif status == 'JOINED':
                        st.success(f"Status: **{status}** - Already joined. Proceed to Validate.")
                    elif 'FAIL' in status.upper():
                        st.error(f"Status: **{status}**")
                        if res.get("hint"):
                            st.warning(res["hint"])
                            if "legal" in res["hint"].lower() or "accept_legal" in res["hint"].lower():
                                st.info(
                                    "Open **Worksheets**, run **initialize** / **join** there, then return and **Check Status** again."
                                )
                    elif status == 'CREATING':
                        st.info(f"Status: **{status}** - Still creating. Check again in a few seconds.")
                    else:
                        st.warning(f"Status: **{status}** (Wait for CREATED)")
                    
                    if res.get("error_details"):
                        with st.expander("Error Details", expanded=True):
                            for detail in res["error_details"]:
                                st.error(detail)
                    
                    collaborators = res.get("collaborators", [])
                    if collaborators:
                        with st.expander("Collaborator Details"):
                            for c in collaborators:
                                status_icon = "+" if c['status'] in ('JOINED', 'CREATED') else "!" if 'FAIL' in c['status'].upper() else "-"
                                line = f"- **{c['name']}** ({c['account']}): {c['status']}"
                                if c.get('roles'):
                                    line += f"  _Roles: {c['roles']}_"
                                st.write(line)
                                if c.get('details'):
                                    st.caption(f"  Details: {c['details'][:500]}")
                else:
                    st.error(f"Check Failed: {res.get('message')}")
                    if res.get("hint"):
                        st.info(res["hint"])
                        if "legal" in str(res.get("hint", "")).lower() or "accept_legal" in str(res.get("hint", "")).lower():
                            st.info(
                                "Use a **SQL Worksheet** (not this app) for steps that require `SYSTEM$ACCEPT_LEGAL_TERMS`."
                            )

        # Join - must be done manually due to SYSTEM$ACCEPT_LEGAL_TERMS restriction
        collab_name = st.session_state.get('collab_name', plan.get('details', {}).get('target_collaboration', ''))
        owner_account = st.session_state.get('owner_account', '')
        role = plan.get('role', 'PROVIDER')

        st.divider()
        st.subheader("Join Collaboration")
        st.info(
            "The **JOIN** command requires `SYSTEM$ACCEPT_LEGAL_TERMS` which cannot execute from Streamlit. "
            "Please copy the SQL below and run it in a **Snowflake SQL Worksheet**."
        )

        # Build worksheet link with multiple fallback strategies
        ws_url = None
        try:
            acct_url = session.sql("SELECT CURRENT_ACCOUNT_URL()").collect()[0][0]
            if acct_url:
                ws_url = f"{acct_url.rstrip('/')}/#/worksheets"
        except:
            pass

        if not ws_url:
            try:
                org = session.sql("SELECT CURRENT_ORGANIZATION_NAME()").collect()[0][0]
                acct = session.sql("SELECT CURRENT_ACCOUNT_NAME()").collect()[0][0]
                if org and acct:
                    ws_url = f"https://app.snowflake.com/{org.lower()}/{acct.lower()}/#/worksheets"
            except:
                pass

        if not ws_url:
            try:
                locator = session.sql("SELECT CURRENT_ACCOUNT()").collect()[0][0]
                region = session.sql("SELECT CURRENT_REGION()").collect()[0][0]
                if locator and region:
                    region_lower = region.lower().replace('_', '-')
                    ws_url = f"https://{locator.lower()}.{region_lower}.snowflakecomputing.com/console#/internal/worksheet"
            except:
                pass

        if ws_url:
            st.markdown(f"**[Open Snowflake Worksheets]({ws_url})**")
        else:
            st.markdown("**[Open Snowflake](https://app.snowflake.com)** and navigate to **Worksheets**.")

        st.markdown("""
**Instructions:**
1. Open a new SQL Worksheet in Snowflake using the link above
2. Copy and paste the SQL below
3. Make sure the role is set to **SAMOOHA_APP_ROLE** (the script sets it automatically)
4. Run each statement sequentially
""")

        st.warning(
            "**If JOIN fails with `ReferenceUsageGrantMissingException`**, "
            "you need an ACCOUNTADMIN to grant REFERENCE_USAGE on the database(s) "
            "mentioned in the error to the share name shown. Example fix:\n\n"
            "```\n"
            "USE ROLE ACCOUNTADMIN;\n"
            "GRANT REFERENCE_USAGE ON DATABASE <your_db>\n"
            "  TO SHARE <share_name_from_error>;\n"
            "USE ROLE SAMOOHA_APP_ROLE;\n"
            "```\n\n"
            "Then retry the JOIN command."
        )

        join_sql_lines = ["-- Run this in a Snowflake SQL Worksheet"]
        join_sql_lines.append("USE ROLE SAMOOHA_APP_ROLE;")
        join_sql_lines.append("USE SECONDARY ROLES NONE;")
        if role == 'CONSUMER' and owner_account:
            join_sql_lines.append(f"\n-- Step 1: Review the collaboration")
            join_sql_lines.append(f"CALL samooha_by_snowflake_local_db.collaboration.review('{collab_name}', '{owner_account}');")
            join_sql_lines.append(f"\n-- Step 2: Join the collaboration")
        else:
            join_sql_lines.append(f"\n-- Join the collaboration (status must be CREATED)")
        join_sql_lines.append(f"CALL samooha_by_snowflake_local_db.collaboration.join('{collab_name}');")
        join_sql_lines.append(f"\n-- Step 3: Verify join status (wait for JOINED)")
        join_sql_lines.append(f"CALL samooha_by_snowflake_local_db.collaboration.get_status('{collab_name}');")

        st.code("\n".join(join_sql_lines), language='sql')
        # Join Button
        collab_status = st.session_state.get('collab_status', '')
        is_ready = collab_status in ('CREATED', 'INVITED')
        btn_label = "Join Collaboration" if is_ready else "Join (Wait for CREATED)"
        
        if col2.button(btn_label, disabled=not is_ready, type="primary"):
             collab_name = st.session_state.get('collab_name', plan.get('details', {}).get('target_collaboration', ''))
             owner_account = st.session_state.get('owner_account', '')
             role = plan.get('role', 'PROVIDER')
             
             if role == 'CONSUMER' and owner_account:
                 with st.spinner("Reviewing collaboration..."):
                     rev_res = review_collaboration(collab_name, owner_account)
                     if rev_res.get("status") == "SUCCESS":
                         st.info("Review complete.")
                     else:
                         st.warning(f"Review: {rev_res.get('message', 'Skipped or already reviewed.')}")
             
             with st.spinner("Joining collaboration..."):
                 res = join_collaboration_direct(collab_name)
                 if res.get("status") == "SUCCESS":
                     st.success(res.get("message"))
                     st.balloons()
                 else:
                     st.error(f"Join Failed: {res.get('message')}")

        st.divider()
        with st.expander("View Full Manual SQL Script"):
            st.code(final_sql, language='sql')

    # 4. VALIDATE
    with tabs[3]:
        st.subheader("Migration Validation")
        if st.button("Run Parity Check"):
            with st.spinner("Validating objects..."):
                report = run_validation(cr_name)
                
                status = report.get('overall_status', 'UNKNOWN')
                if status == "PASS":
                    st.success("Validation Passed: All objects match between legacy and new collaboration.")
                elif status == "WARN":
                    st.warning(
                        "Validation completed with **warnings** (e.g. platform-privacy / freeform expectations). "
                        "Review the table and remediation below."
                    )
                else:
                    st.error(f"Validation Status: {status}")
                    if report.get('error'):
                        st.error(f"Error: {report.get('error')}")
                
                steps = report.get('steps', [])
                if steps:
                    st.dataframe(
                        pd.DataFrame(steps), 
                        column_config={
                            "status": st.column_config.TextColumn("Status", width="small"),
                            "name": st.column_config.TextColumn("Check Name", width="medium"),
                            "details": st.column_config.TextColumn("Details", width="large"),
                            "fix_hint": st.column_config.TextColumn("How to Fix", width="large"),
                        },
                        use_container_width=True
                    )
                
                if report.get('missing_objects'):
                    st.error(f"Missing Objects: {report.get('missing_objects')}")

                remediation = report.get('remediation', [])
                if remediation:
                    with st.expander("Remediation Steps", expanded=True):
                        for i, hint in enumerate(remediation, 1):
                            st.markdown(f"**{i}.** {hint}")

    # 5. CLEANUP
    with tabs[4]:
        st.subheader("Teardown")
        st.warning("Destructive Action: This will remove the migrated collaboration and resources.")
        _, cleanup_sql = get_manual_sql_scripts(plan)
        st.code(cleanup_sql, language='sql')
        
        if st.button("Confirm Teardown", type="secondary"):
            with st.spinner("Tearing down..."):
                res = execute_teardown(cr_name)
                if res.get("status") == "SUCCESS":
                    st.success("Teardown Complete")
                else:
                    st.error(f"Teardown Failed: {res.get('message')}")
