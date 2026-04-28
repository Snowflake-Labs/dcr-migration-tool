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
import pandas as pd
import time
from snowflake.snowpark.context import get_active_session

st.set_page_config(layout="wide", page_title="DCR Migration Tool", page_icon="❄️")

st.markdown("""
<style>
    .stTabs [data-baseweb="tab-list"] { gap: 24px; }
    .stTabs [data-baseweb="tab"] {
        height: 50px; white-space: pre-wrap; background-color: transparent;
        border-radius: 4px 4px 0px 0px; gap: 1px; padding-top: 10px; padding-bottom: 10px;
    }
    .stTabs [aria-selected="true"] {
        background-color: rgba(41, 181, 232, 0.1); border-bottom: 2px solid #29B5E8;
    }
    .metric-container {
        border: 1px solid #e0e0e0; padding: 10px; border-radius: 5px;
        text-align: center; background-color: #0e1117;
    }
    [data-testid="stMetricValue"] { font-size: 24px !important; }
</style>
""", unsafe_allow_html=True)


def get_session():
    try:
        return get_active_session()
    except:
        return None

session = get_session()


def _classify_cleanroom(name, cid):
    is_ui = False
    if name and cid:
        is_ui = str(name).upper().replace(' ', '_') != str(cid).upper().replace(' ', '_')
    return "UI" if is_ui else "P&C"


def list_cleanrooms():
    rooms = []
    try:
        p_res = session.sql("CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.PROVIDER.VIEW_CLEANROOMS()").collect()
        if p_res:
            for r in p_res:
                d = {k.upper(): v for k, v in r.as_dict().items()}
                name = d.get('CLEANROOM_NAME') or d.get('NAME')
                cid = d.get('CLEANROOM_ID') or d.get('ID')
                state = d.get('STATE') or d.get('STATUS') or ''
                cr_class = _classify_cleanroom(name, cid)
                rooms.append({
                    "name": name, "cleanroom_id": cid, "role": "PROVIDER",
                    "state": state, "cleanroom_class": cr_class, "eligible": True
                })
    except:
        pass
    try:
        c_res = session.sql("CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.CONSUMER.VIEW_CLEANROOMS()").collect()
        if c_res:
            existing = {r['name'].upper() for r in rooms if r.get('name')}
            for r in c_res:
                d = {k.upper(): v for k, v in r.as_dict().items()}
                name = d.get('CLEANROOM_NAME') or d.get('NAME')
                cid = d.get('CLEANROOM_ID') or d.get('ID')
                state = d.get('STATE') or d.get('STATUS') or ''
                if name and name.upper() not in existing:
                    cr_class = _classify_cleanroom(name, cid)
                    rooms.append({
                        "name": name, "cleanroom_id": cid, "role": "CONSUMER",
                        "state": state, "cleanroom_class": cr_class, "eligible": True
                    })
    except:
        pass
    return rooms


def list_collab_dcrs():
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
            collab_name = f"migrated_{cr.upper()}"
            status = ''
            try:
                st_res = session.sql(
                    f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.COLLABORATION.GET_STATUS('{collab_name}')"
                ).collect()
                if st_res:
                    row = {k.upper(): v for k, v in st_res[0].as_dict().items()}
                    status = row.get('STATUS') or row.get('STATE') or ''
            except:
                pass
            collabs.append({
                "name": collab_name, "status": status or d.get('STATUS', ''),
                "source_pnc": cr.upper(),
                "migration_history": {
                    "migrated_from_pnc": True, "source_cleanroom": cr,
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
        if not res_str:
            return {"status": "ERROR", "message": "Empty response from backend."}
        plan = json.loads(res_str)
        if plan.get("status") == "ERROR":
            msg = plan.get('message', '')
            if "not found" in msg.lower() or "not installed" in msg.lower():
                st.error(f"Cleanroom '{cleanroom_name}' was not found. Verify the name or UUID.")
            elif "laf" in msg.lower():
                st.error(f"Cleanroom '{cleanroom_name}' uses LAF. LAF migration is not supported.")
            elif "prerequisites" in msg.lower():
                st.error(f"Prerequisites failed: {msg}")
            else:
                st.error(f"Migration Error: {msg}")
            for w in plan.get('warnings', []):
                st.warning(w)
            return None
        plan['cleanroom_name'] = cleanroom_name
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
    try:
        spec = collab_spec.strip()
        res = session.sql(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.COLLABORATION.INITIALIZE($$\n{spec}\n$$, 'APP_WH')").collect()
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


def check_status(cleanroom_name):
    try:
        res_str = session.call("DCR_SNOWVA.MIGRATION.AGENT_MIGRATE_ORCHESTRATOR", cleanroom_name, 'CHECK_STATUS')
        return json.loads(res_str)
    except Exception as e:
        return {"status": "ERROR", "message": str(e)}


def run_validation(cleanroom_name):
    try:
        res_str = session.call("DCR_SNOWVA.MIGRATION.AGENT_MIGRATE_ORCHESTRATOR", cleanroom_name, 'VALIDATE')
        try:
            result = json.loads(res_str)
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
    cr_name = plan.get('cleanroom_name', '').replace(' ', '_').upper()
    collab_name = details.get("target_collaboration", f"migrated_{cr_name}")
    role = plan.get('role', 'UNKNOWN')
    owner_account = st.session_state.get('owner_account', '')

    finalize_lines = [
        f"-- MANUAL FINALIZATION FOR {role} ({collab_name})",
        "USE ROLE SAMOOHA_APP_ROLE;",
        "USE SECONDARY ROLES NONE;", "",
        f"-- 1. Check Status\nCALL samooha_by_snowflake_local_db.collaboration.get_status('{collab_name}');"
    ]
    if role == 'CONSUMER':
        if owner_account:
            finalize_lines.append(f"\n-- 2. Review\nCALL samooha_by_snowflake_local_db.collaboration.review('{collab_name}', '{owner_account}');")
        else:
            finalize_lines.append(f"\n-- 2. Review (replace with provider's ORG.ACCOUNT)\nCALL samooha_by_snowflake_local_db.collaboration.review('{collab_name}', 'REPLACE_WITH_PROVIDER_ORG.ACCOUNT');")
    finalize_lines.append(f"\n-- 3. Join\nCALL samooha_by_snowflake_local_db.collaboration.join('{collab_name}');")

    cleanup_lines = [
        f"-- CLEANUP SCRIPT FOR {collab_name}", "USE ROLE SAMOOHA_APP_ROLE;", "",
        f"CALL samooha_by_snowflake_local_db.collaboration.teardown('{collab_name}');",
        f"CALL samooha_by_snowflake_local_db.collaboration.teardown('{collab_name}');",
    ]
    return "\n".join(finalize_lines), "\n".join(cleanup_lines)


def _count_by_classification(templates):
    counts = {}
    for t in templates:
        if isinstance(t, dict):
            c = t.get('classification', 'UNKNOWN')
        else:
            c = 'STANDARD'
        counts[c] = counts.get(c, 0) + 1
    return counts


# ---------------------------------------------------------------------------
# Main App
# ---------------------------------------------------------------------------

if not session:
    st.error("No active Snowpark session. Please run in Snowflake.")
    st.stop()

with st.sidebar:
    st.title("DCR Migration")
    st.caption("v2.3.0")
    st.divider()

    btn_col1, btn_col2 = st.columns(2)
    if btn_col1.button("All Cleanrooms", use_container_width=True):
        with st.spinner("Fetching cleanrooms..."):
            rooms = list_cleanrooms()
            st.session_state['available_rooms'] = rooms if rooms else []
            if not rooms:
                st.warning("No cleanrooms found.")

    if btn_col2.button("Migrated DCRs", use_container_width=True):
        with st.spinner("Fetching collaboration DCRs..."):
            collabs = list_collab_dcrs()
            st.session_state['collab_dcrs'] = collabs if collabs else []
            if not collabs:
                st.info("No collaboration DCRs found.")
            else:
                st.success(f"Found {len(collabs)} migrated DCR(s)")

    if st.session_state.get('available_rooms'):
        rooms = st.session_state['available_rooms']
        migrated_pnc_names = set()
        for c in st.session_state.get('collab_dcrs', []):
            if c.get('source_pnc'):
                migrated_pnc_names.add(c['source_pnc'])

        def _room_label(r):
            badge = ""
            if r['name'] and r['name'].upper().replace(' ', '_') in migrated_pnc_names:
                badge = " [migrated]"
            cr_type = r.get('cleanroom_class', '')
            return f"{r['name']}  ({cr_type}, {r['role']}, {r['state']}){badge}"

        pc_rooms = [r for r in rooms if r.get('cleanroom_class') == 'P&C']
        ui_rooms = [r for r in rooms if r.get('cleanroom_class') == 'UI']
        room_options = ["-- Select --"]
        if pc_rooms:
            room_options += [_room_label(r) for r in pc_rooms]
        if ui_rooms:
            room_options += [_room_label(r) for r in ui_rooms]
        selected = st.selectbox("Cleanrooms", room_options)
        st.caption(f"{len(pc_rooms)} P&C | {len(ui_rooms)} UI")
        if selected and selected != "-- Select --":
            cr_name_from_picker = selected.split("  (")[0].strip()
            cleanroom_input = st.text_input("Cleanroom Name", value=cr_name_from_picker)
        else:
            cleanroom_input = st.text_input("Cleanroom Name", placeholder="e.g. MJ_ML_DCR or UUID")
    else:
        cleanroom_input = st.text_input("Cleanroom Name", placeholder="e.g. MJ_ML_DCR or UUID")

    if st.session_state.get('collab_dcrs'):
        st.divider()
        collabs = st.session_state['collab_dcrs']
        st.caption(f"Migrated DCRs ({len(collabs)})")
        for c in collabs:
            with st.expander(f"{c['name']} — {c['status']}"):
                st.json({"migration_history": c.get('migration_history', {})})

    if st.button("Generate Plan", type="primary", use_container_width=True):
        if not cleanroom_input:
            st.warning("Please enter a cleanroom name or select one from the list.")
        else:
            with st.spinner("Analyzing environment..."):
                plan = get_migration_plan(cleanroom_input.strip())
                if plan:
                    st.session_state['plan'] = plan
                    st.session_state['collab_status'] = 'Not Started'
                    st.success("Plan Ready!")


if 'plan' not in st.session_state:
    st.info("Select a cleanroom in the sidebar and click **Generate Plan** to begin.")
    st.markdown("""
    ### Workflow
    **Provider:** Generate Plan > Execute Setup > Join (worksheet) > Validate

    **Consumer:** Generate Plan > Execute Setup > Review + Join + Link (worksheet) > Validate
    """)
else:
    plan = st.session_state['plan']
    cr_name = plan['cleanroom_name']
    role = plan['role']
    details = plan.get('details', {})
    cleanroom_type = details.get('cleanroom_type', 'UNKNOWN')
    is_ui = details.get('is_ui_cleanroom', False)
    templates = details.get('templates', [])
    data_offerings = details.get('provider_data', [])

    tmpl_classifications = _count_by_classification(templates)
    platform_privacy_count = tmpl_classifications.get('PLATFORM_PRIVACY', 0)
    normal_template_count = len(templates) - platform_privacy_count

    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("Type", cleanroom_type)
    m2.metric("Role", role)
    m3.metric("Templates", normal_template_count)
    m4.metric("Data Offerings", len(data_offerings))
    m5.metric("Status", st.session_state.get('collab_status', 'Not Started'))

    if platform_privacy_count > 0:
        st.warning(f"{platform_privacy_count} **platform privacy template(s)** detected — migrated via freeform SQL data offerings.")

    if cleanroom_type in ('UI_FREEFORM_SQL', 'PC_FREEFORM_SQL'):
        st.info("**Freeform SQL cleanroom** — Data offerings use `template_and_freeform_sql` with native policies.")

    if role == 'CONSUMER' and not data_offerings:
        st.warning("No consumer data offerings detected. Ensure you have linked datasets on the legacy cleanroom.")

    for w in plan.get('warnings', []):
        st.warning(w)

    if details.get("has_python_code_spec"):
        udfs = details.get("python_udf_names") or []
        st.success(f"Python UDFs: {len(udfs)} function(s) — {', '.join(udfs[:15])}")

    st.divider()
    tabs = st.tabs(["Review Plan", "Execute Setup", "Finalize (Join)", "Validate", "Cleanup"])

    # -----------------------------------------------------------------------
    # TAB 0: Review Plan
    # -----------------------------------------------------------------------
    with tabs[0]:
        st.subheader("Migration Plan Details")
        col_a, col_b = st.columns(2)
        with col_a:
            st.markdown("**Cleanroom Classification**")
            st.markdown(f"- **Type:** `{cleanroom_type}`")
            st.markdown(f"- **UI Cleanroom:** {'Yes' if is_ui else 'No'}")
            st.markdown(f"- **Role:** {role}")
        with col_b:
            if tmpl_classifications:
                st.markdown("**Template Classification**")
                for cls, cnt in sorted(tmpl_classifications.items()):
                    icon = "⚠️" if cls == "PLATFORM_PRIVACY" else "✅"
                    st.markdown(f"- {icon} **{cls}**: {cnt}")
        if templates:
            with st.expander("Template Details"):
                tmpl_rows = []
                for t in templates:
                    if isinstance(t, dict):
                        tmpl_rows.append({
                            "Name": t.get('template_name', ''),
                            "Classification": t.get('classification', ''),
                            "Status": t.get('status', 'WILL_MIGRATE')
                        })
                if tmpl_rows:
                    st.dataframe(pd.DataFrame(tmpl_rows), use_container_width=True)
        st.divider()
        st.subheader("Generated Migration Script")
        st.code(plan.get('generated_script', '-- No script generated'), language='sql')

    # -----------------------------------------------------------------------
    # TAB 1: Execute Setup
    # -----------------------------------------------------------------------
    with tabs[1]:
        st.subheader("Phase 1: Setup")
        if role == 'PROVIDER':
            st.info("Registers templates/datasets and initializes the collaboration.")
        else:
            st.info("Registers consumer data offerings. **REVIEW + JOIN + LINK must be run manually** in a worksheet (see Finalize tab).")

        if st.button("Run Setup", type="primary"):
            with st.spinner("Running migration setup..."):
                res = execute_migration(cr_name)

            if res.get("status") == "SUCCESS":
                with st.expander("Execution Logs", expanded=True):
                    if res.get("message"):
                        st.info(res["message"])
                    for act in res.get("actions", []):
                        if "failed" in act.lower() or "error" in act.lower():
                            st.error(f"- {act}")
                        elif "skipped" in act.lower() or "already" in act.lower():
                            st.warning(f"- {act}")
                        else:
                            st.write(f"- {act}")

                collab_name = res.get("collab_name", "")
                collab_spec = res.get("collab_spec", "")
                res_role = res.get("role", "")

                if res_role == "PROVIDER" and collab_spec:
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

                elif res_role == "CONSUMER":
                    st.session_state['setup_complete'] = True
                    st.session_state['collab_name'] = collab_name
                    st.session_state['owner_account'] = res.get("owner_account", "")
                    st.session_state['manual_join_sql'] = res.get("manual_join_sql", "")

                    st.success("Consumer data offerings registered.")
                    st.warning("**Next:** Go to the **Finalize (Join)** tab and run the SQL in a worksheet.")

                    manual_sql = res.get("manual_join_sql", "")
                    if manual_sql:
                        st.subheader("Join SQL (copy to worksheet)")
                        st.code(manual_sql, language='sql')
            else:
                st.error(f"Setup Failed: {res.get('message')}")
                if res.get("actions"):
                    with st.expander("Partial Execution Logs"):
                        for act in res["actions"]:
                            st.write(f"- {act}")

    # -----------------------------------------------------------------------
    # TAB 2: Finalize (Join)
    # -----------------------------------------------------------------------
    with tabs[2]:
        st.subheader("Phase 2: Finalize (Join)")
        col1, col2 = st.columns(2)

        if col1.button("Check Status"):
            with st.spinner("Checking..."):
                res = check_status(cr_name)
                if res.get("status") == "SUCCESS":
                    status = res.get("collaboration_status", "UNKNOWN")
                    st.session_state['collab_status'] = status
                    if status == 'JOINED':
                        st.success(f"Status: **{status}** — Proceed to Validate.")
                    elif status == 'CREATED':
                        st.success(f"Status: **{status}** — Ready to Join!")
                    elif 'JOIN_FAILED' in status.upper() or 'FAIL' in status.upper():
                        st.error(f"Status: **{status}**")
                        if res.get("hint"):
                            st.warning(res["hint"])
                        error_details = res.get("error_details", [])
                        if error_details:
                            with st.expander("Error Details", expanded=True):
                                for detail in error_details:
                                    st.error(detail)
                                    if 'ReferenceUsageGrantMissing' in detail or 'REFERENCE_USAGE' in detail.upper():
                                        import re as _re
                                        db_match = _re.search(r'Databases?:\s*(\S+?)\.?\s', detail, _re.IGNORECASE)
                                        share_match = _re.search(r'Share name:\s*(\S+?)\.?\s*$', detail, _re.IGNORECASE | _re.MULTILINE)
                                        if not share_match:
                                            share_match = _re.search(r'TO SHARE\s+(\S+)', detail, _re.IGNORECASE)
                                        db_name = db_match.group(1) if db_match else '<YOUR_DATABASE>'
                                        share_name = share_match.group(1).rstrip('.') if share_match else '<SHARE_NAME_FROM_ERROR>'
                                        collab_n = st.session_state.get("collab_name", "")
                                        st.warning("**Fix: Run these commands as ACCOUNTADMIN in a worksheet:**")
                                        fix_sql = f"""USE ROLE ACCOUNTADMIN;
GRANT REFERENCE_USAGE ON DATABASE {db_name} TO ROLE SAMOOHA_APP_ROLE WITH GRANT OPTION;
GRANT REFERENCE_USAGE ON DATABASE {db_name} TO SHARE {share_name};"""
                                        st.code(fix_sql, language='sql')
                                        st.info("**Then teardown the failed collaboration and re-run Execute Setup:**")
                                        teardown_sql = f"""USE ROLE SAMOOHA_APP_ROLE;
USE SECONDARY ROLES NONE;
CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.COLLABORATION.TEARDOWN('{collab_n}');
-- Call GET_STATUS until LOCAL_DROP_PENDING:
CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.COLLABORATION.GET_STATUS('{collab_n}');
-- Final teardown call:
CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.COLLABORATION.TEARDOWN('{collab_n}');"""
                                        st.code(teardown_sql, language='sql')
                                        st.divider()
                                        if st.button("Teardown Failed Collaboration", type="secondary", key="teardown_failed"):
                                            with st.spinner("Running teardown..."):
                                                try:
                                                    session.sql("USE SECONDARY ROLES NONE").collect()
                                                    session.sql(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.COLLABORATION.TEARDOWN('{collab_n}')").collect()
                                                    st.info("Teardown initiated. Waiting for LOCAL_DROP_PENDING...")
                                                    for _ in range(10):
                                                        time.sleep(3)
                                                        try:
                                                            st_res = session.sql(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.COLLABORATION.GET_STATUS('{collab_n}')").collect()
                                                            if st_res:
                                                                s = {k.upper(): v for k, v in st_res[0].as_dict().items()}.get('STATUS', '')
                                                                if s == 'LOCAL_DROP_PENDING':
                                                                    break
                                                        except:
                                                            break
                                                    session.sql(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.COLLABORATION.TEARDOWN('{collab_n}')").collect()
                                                    st.success("Teardown complete. Run the GRANT commands above as ACCOUNTADMIN, then click **Execute Setup** again.")
                                                except Exception as e:
                                                    st.error(f"Teardown failed: {str(e)[:300]}")
                    else:
                        st.warning(f"Status: **{status}**")
                    collaborators = res.get("collaborators", [])
                    if collaborators:
                        with st.expander("Collaborator Details"):
                            for c in collaborators:
                                st.write(f"- **{c['name']}** ({c['account']}): {c['status']}")
                else:
                    err_msg = res.get('message', '')
                    if 'not found' in err_msg.lower() or 'not exist' in err_msg.lower():
                        if role == 'CONSUMER':
                            st.warning("Collaboration not visible yet. Run the JOIN SQL below in a worksheet first.")
                        else:
                            st.error("Collaboration not found. Run Execute Setup first.")
                    else:
                        st.error(f"Check Failed: {err_msg}")
        st.divider()
        st.subheader("Join SQL — Run in Worksheet")
        st.info("**REVIEW and JOIN require `SYSTEM$ACCEPT_LEGAL_TERMS`** — copy the SQL below and run in a Snowflake SQL Worksheet.")

        ws_url = None
        try:
            acct_url = session.sql("SELECT CURRENT_ACCOUNT_URL()").collect()[0][0]
            if acct_url:
                ws_url = f"{acct_url.rstrip('/')}/#/worksheets"
        except:
            pass
        if ws_url:
            st.markdown(f"**[Open Snowflake Worksheets]({ws_url})**")

        manual_sql = st.session_state.get('manual_join_sql', '')
        if not manual_sql:
            collab_name = st.session_state.get('collab_name', details.get('target_collaboration', ''))
            owner_account = st.session_state.get('owner_account', '')
            lines = ["-- Run in a Snowflake SQL Worksheet", "USE ROLE SAMOOHA_APP_ROLE;", "USE SECONDARY ROLES NONE;"]
            if role == 'CONSUMER':
                if owner_account:
                    lines.append(f"\n-- Step 1: Review\nCALL samooha_by_snowflake_local_db.collaboration.review('{collab_name}', '{owner_account}');")
                else:
                    lines.append(f"\n-- Step 1: Review (replace with provider ORG.ACCOUNT)\nCALL samooha_by_snowflake_local_db.collaboration.review('{collab_name}', 'REPLACE_WITH_PROVIDER_ORG.ACCOUNT');")
                lines.append(f"\n-- Step 2: Join\nCALL samooha_by_snowflake_local_db.collaboration.join('{collab_name}');")
            else:
                lines.append(f"\n-- Join (status must be CREATED)\nCALL samooha_by_snowflake_local_db.collaboration.join('{collab_name}');")
            lines.append(f"\n-- Verify\nCALL samooha_by_snowflake_local_db.collaboration.get_status('{collab_name}');")
            manual_sql = "\n".join(lines)

        st.code(manual_sql, language='sql')

        st.warning(
            "**If JOIN fails with `ReferenceUsageGrantMissingException`**, run as ACCOUNTADMIN:\n\n"
            "```\n"
            "GRANT REFERENCE_USAGE ON DATABASE <your_db>\n"
            "  TO ROLE SAMOOHA_APP_ROLE WITH GRANT OPTION;\n"
            "GRANT REFERENCE_USAGE ON DATABASE <your_db>\n"
            "  TO SHARE <share_name_from_error>;\n"
            "```"
        )

        final_sql, _ = get_manual_sql_scripts(plan)
        with st.expander("View Full Manual SQL Script"):
            st.code(final_sql, language='sql')

    # -----------------------------------------------------------------------
    # TAB 3: Validate
    # -----------------------------------------------------------------------
    with tabs[3]:
        st.subheader("Migration Validation")
        if st.button("Run Parity Check"):
            with st.spinner("Validating..."):
                report = run_validation(cr_name)
                status = report.get('overall_status', 'UNKNOWN')
                if status == "PASS":
                    st.success("Validation Passed: All objects match.")
                else:
                    st.error(f"Validation Status: {status}")
                    if report.get('error'):
                        st.error(report['error'])
                steps = report.get('steps', [])
                if steps:
                    st.dataframe(pd.DataFrame(steps), use_container_width=True)
                remediation = report.get('remediation', [])
                if remediation:
                    with st.expander("Remediation Steps", expanded=True):
                        for i, hint in enumerate(remediation, 1):
                            st.markdown(f"**{i}.** {hint}")

    # -----------------------------------------------------------------------
    # TAB 4: Cleanup
    # -----------------------------------------------------------------------
    with tabs[4]:
        st.subheader("Teardown")
        st.warning("Destructive: This removes the migrated collaboration.")
        _, cleanup_sql = get_manual_sql_scripts(plan)
        st.code(cleanup_sql, language='sql')
        if st.button("Confirm Teardown", type="secondary"):
            with st.spinner("Tearing down..."):
                res = execute_teardown(cr_name)
                if res.get("status") == "SUCCESS":
                    st.success("Teardown Complete")
                else:
                    st.error(f"Teardown Failed: {res.get('message')}")


