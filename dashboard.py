import os
import streamlit as st
import pandas as pd
import sqlite3
from datetime import datetime
from src.orchestrator import ETLPipeline
from src.workflows import registry, WorkflowConfig, SourceConfig
from src.scheduler import scheduler
from src.reporter import reporter
from src.workflow_runner import runner as workflow_runner
from src.storage.workflow_history import workflow_history_store
import src.config as config
from src.utils import LOG_FILE
from src.load import load_to_csv, load_to_db
from src.extract.extract import run_extraction
from src.transform.engine import TransformEngine
from src.storage.history_store import save_snapshot, detect_price_changes
from src.alerts.alert_engine import generate_alerts
from src.pipeline.multi_source_pipeline import run_multi_source_pipeline
import time

st.set_page_config(page_title="💰 Price Monitor ETL", layout="wide", initial_sidebar_state="expanded")
st.title("💰 Competitor Price Monitor")
st.caption("Real-time multi-source price tracking with ETL pipeline")

# =============================
# SESSION STATE INIT
# =============================
defaults = {
    "pipeline_status": "Idle",
    "last_run_time": None,
    "error_message": "",
    "execution_time": None,
    "data": None,
    "extract_status": "Idle",
    "transform_status": "Idle",
    "load_status": "Idle",
    "selected_workflow": "electronics_monitoring",
    "workflow_config": registry.get("electronics_monitoring"),
    "schedule_checked": False,
}

for k, v in defaults.items():
    if k not in st.session_state:
        st.session_state[k] = v


def slugify_id(text):
    return "".join(c if c.isalnum() else "_" for c in text.strip().lower()).strip("_")


def get_due_schedules() -> list:
    return [s for s in scheduler.list_enabled() if scheduler.is_due(s)]


def run_scheduled_workflow(schedule):
    workflow = registry.get(schedule.workflow_id)
    if not workflow:
        return None, f"Workflow {schedule.workflow_id} not found"

    try:
        result = workflow_runner.execute_workflow(schedule.workflow_id)
        if result.get("status") == "failed":
            return None, result.get("error")

        comparison = result.get("comparison")
        return comparison, None
    except Exception as exc:
        return None, str(exc)


def auto_run_due_schedules():
    if st.session_state.get("schedule_checked"):
        return

    due_results = workflow_runner.execute_due_workflows()
    st.session_state.schedule_checked = True

    if not due_results:
        return

    for result in due_results:
        if result.get("status") != "failed":
            st.success(f"✅ Auto-run complete: {result.get('workflow_id')}")
            comparison = result.get("comparison")
            if comparison is not None:
                st.session_state["latest_data"] = comparison
                st.session_state.data = comparison
        else:
            st.error(f"❌ Auto-run failed: {result.get('workflow_id')} ({result.get('error')})")


def initialize_workflow_builder():
    if "workflow_builder_sources" not in st.session_state:
        st.session_state.workflow_builder_sources = [
            {
                "name": "source_1",
                "source_type": "playwright",
                "url": "",
                "selector": "",
                "mode": "Auto Detect",
                "keyword": "",
                "match_threshold": 70,
            }
        ]
    if "workflow_builder_alerts" not in st.session_state:
        st.session_state.workflow_builder_alerts = [
            {"type": "price_drop", "threshold": 5},
        ]
    if "new_workflow_name" not in st.session_state:
        st.session_state.new_workflow_name = ""
    if "new_workflow_description" not in st.session_state:
        st.session_state.new_workflow_description = ""
    if "new_workflow_enabled" not in st.session_state:
        st.session_state.new_workflow_enabled = True
    if "new_workflow_threshold" not in st.session_state:
        st.session_state.new_workflow_threshold = 70

initialize_workflow_builder()

# Register declarative workflows from the workflow runner into the dashboard registry
for workflow_id in workflow_runner.list_workflows():
    if registry.get(workflow_id) is None:
        workflow_runner.register_workflow_to_registry(workflow_id)

# Display active workflow
st.info(f"📊 **Active Workflow**: {st.session_state.workflow_config.name}")

def normalize_source_type(source_type):
    """Normalize source type string."""
    mapping = {
        "Default (Web)": "default (web)",
        "Playwright (Dynamic Web)": "playwright",
        "Selenium (Dynamic Web)": "selenium",
        "CSV": "csv",
        "Upload Dataset": "upload dataset",
        "API (Future)": "api"
    }
    return mapping.get(source_type, source_type.strip().lower())

# =============================
# SIDEBAR CONFIGURATION
# =============================
with st.sidebar:
    st.markdown("## 📋 Workflow Selection")
    
    # Workflow selection dropdown
    available_workflows = sorted(set(registry.list_workflows()))
    selected = st.selectbox(
        "Select Monitoring Workflow",
        available_workflows,
        key="workflow_selector"
    )
    
    if selected != st.session_state.selected_workflow:
        st.session_state.selected_workflow = selected
        st.session_state.workflow_config = registry.get(selected)
    
    workflow = st.session_state.workflow_config
    
    # Display workflow details
    st.markdown(f"**{workflow.name}**")
    st.caption(workflow.description)
    
    with st.expander("📌 Workflow Details"):
        st.markdown("**Sources:**")
        for source in workflow.sources:
            st.text(f"  • {source.name}")
            st.caption(f"    Type: {source.source_type}")
            st.caption(f"    Threshold: {source.match_threshold}")
        
        st.markdown(f"**Global Match Threshold:** {workflow.global_match_threshold}")
        
        if workflow.transformation_rules:
            st.markdown("**Transform Rules:**")
            for rule in workflow.transformation_rules:
                st.caption(f"  • {rule.get('type', 'unknown')}")
        
        if workflow.alert_rules:
            st.markdown("**Alert Rules:**")
            if isinstance(workflow.alert_rules, list):
                # New format: list of dictionaries
                for rule in workflow.alert_rules:
                    st.caption(f"  • {rule.get('type', 'unknown')} | threshold: {rule.get('threshold', 'n/a')}")
            elif isinstance(workflow.alert_rules, dict):
                # Old format: dictionary of key-value pairs
                for rule_type, threshold in workflow.alert_rules.items():
                    st.caption(f"  • {rule_type} | threshold: {threshold}")
            else:
                st.caption("  • Invalid alert rules format")

    with st.expander("➕ Create & Save Workflow"):
        st.markdown("### New workflow builder")
        workflow_name = st.text_input("Workflow Name", value=st.session_state.new_workflow_name, key="new_workflow_name")
        workflow_description = st.text_area("Description", value=st.session_state.new_workflow_description, key="new_workflow_description")
        workflow_enabled = st.checkbox("Enabled", value=st.session_state.new_workflow_enabled, key="new_workflow_enabled")
        workflow_threshold = st.slider("Global match threshold", 50, 100, value=st.session_state.new_workflow_threshold, key="new_workflow_threshold")
        st.markdown("---")
        st.markdown("#### Sources")

        source_types = ["playwright", "selenium", "csv", "api"]
        source_modes = ["Auto Detect", "Table Extraction", "Full Page Text", "Custom Selector"]

        for idx, source in enumerate(st.session_state.workflow_builder_sources):
            st.markdown(f"**Source {idx + 1}**")
            source["name"] = st.text_input("Name", value=source["name"], key=f"builder_source_name_{idx}")
            source["source_type"] = st.selectbox(
                "Type",
                source_types,
                index=source_types.index(source["source_type"]) if source["source_type"] in source_types else 0,
                key=f"builder_source_type_{idx}",
            )
            source["mode"] = st.selectbox(
                "Mode",
                source_modes,
                index=source_modes.index(source["mode"]) if source["mode"] in source_modes else 0,
                key=f"builder_source_mode_{idx}",
            )
            source["url"] = st.text_input("URL", value=source["url"], key=f"builder_source_url_{idx}")
            source["selector"] = st.text_input("Selector", value=source["selector"], key=f"builder_source_selector_{idx}")
            source["keyword"] = st.text_input("Keyword", value=source["keyword"], key=f"builder_source_keyword_{idx}")
            source["match_threshold"] = st.slider(
                "Match threshold",
                50,
                100,
                value=source["match_threshold"],
                key=f"builder_source_threshold_{idx}",
            )
            if st.button("Remove this source", key=f"remove_builder_source_{idx}"):
                if len(st.session_state.workflow_builder_sources) > 1:
                    st.session_state.workflow_builder_sources.pop(idx)
                    st.st.rerun()
                else:
                    st.warning("At least one source is required.")
            st.markdown("---")

        if st.button("Add source", key="add_workflow_source"):
            st.session_state.workflow_builder_sources.append(
                {
                    "name": f"source_{len(st.session_state.workflow_builder_sources) + 1}",
                    "source_type": "playwright",
                    "url": "",
                    "selector": "",
                    "mode": "Auto Detect",
                    "keyword": "",
                    "match_threshold": 70,
                }
            )
            st.st.rerun()

        st.markdown("#### Alert Rules")
        alert_types = ["price_drop", "undercut", "price_increase"]
        for idx, rule in enumerate(st.session_state.workflow_builder_alerts):
            st.markdown(f"**Rule {idx + 1}**")
            rule["type"] = st.selectbox(
                "Rule type",
                alert_types,
                index=alert_types.index(rule["type"]) if rule["type"] in alert_types else 0,
                key=f"builder_alert_type_{idx}",
            )
            rule["threshold"] = st.number_input(
                "Threshold",
                min_value=0,
                max_value=10000,
                value=rule["threshold"],
                step=1,
                key=f"builder_alert_threshold_{idx}",
            )
            if st.button("Remove this rule", key=f"remove_builder_rule_{idx}"):
                if len(st.session_state.workflow_builder_alerts) > 1:
                    st.session_state.workflow_builder_alerts.pop(idx)
                    st.st.rerun()
                else:
                    st.warning("At least one alert rule is recommended.")
            st.markdown("---")

        if st.button("Save workflow", key="save_workflow"):
            if not workflow_name.strip():
                st.error("Workflow name is required.")
            else:
                workflow_id = slugify_id(workflow_name)
                if not workflow_id:
                    workflow_id = "workflow"
                original_id = workflow_id
                counter = 1
                while workflow_id in registry.workflows:
                    workflow_id = f"{original_id}_{counter}"
                    counter += 1

                new_sources = []
                for source in st.session_state.workflow_builder_sources:
                    if not source["name"].strip() or not source["source_type"].strip():
                        st.error("Each source must have a name and a type.")
                        break
                    new_sources.append(
                        SourceConfig(
                            name=source["name"].strip(),
                            source_type=source["source_type"].strip(),
                            url=source["url"].strip() or None,
                            selector=source["selector"].strip() or None,
                            mode=source["mode"].strip(),
                            keyword=source["keyword"].strip() or None,
                            match_threshold=source["match_threshold"],
                        )
                    )
                else:
                    alert_rules = [
                        {"type": rule["type"], "threshold": rule["threshold"]}
                        for rule in st.session_state.workflow_builder_alerts
                        if rule.get("type")
                    ]

                    workflow = WorkflowConfig(
                        workflow_id=workflow_id,
                        name=workflow_name.strip(),
                        description=workflow_description.strip(),
                        sources=new_sources,
                        transformation_rules=[],
                        alert_rules=alert_rules,
                        global_match_threshold=workflow_threshold,
                        enabled=workflow_enabled,
                    )

                    registry.register(workflow)
                    workflow_store = os.path.join(os.path.dirname(__file__), "src", "workflows")
                    os.makedirs(workflow_store, exist_ok=True)
                    registry.save_to_file(workflow.workflow_id, os.path.join(workflow_store, f"{workflow.workflow_id}.json"))

                    st.success(f"Saved workflow '{workflow.name}' and selected it.")
                    st.session_state.selected_workflow = workflow.workflow_id
                    st.session_state.workflow_selector = workflow.workflow_id
                    st.session_state.workflow_config = workflow
                    st.st.rerun()

    with st.expander("⏰ Schedule Workflow"):
        st.markdown("### Automated Execution")
        
        frequency = st.selectbox(
            "Run frequency",
            ["manual", "hourly", "daily", "weekly"],
            key="schedule_frequency"
        )
        
        time_of_day = None
        day_of_week = None
        
        if frequency in ["daily", "weekly"]:
            time_of_day = st.time_input("Time of day", key="schedule_time")
        
        if frequency == "weekly":
            day_of_week = st.selectbox(
                "Day of week",
                ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"],
                key="schedule_day"
            )
        
        col1, col2 = st.columns(2)
        with col1:
            if st.button("💾 Save Schedule", key="save_schedule"):
                schedule = scheduler.create_schedule(
                    workflow_id=st.session_state.selected_workflow,
                    frequency=frequency,
                    time_of_day=str(time_of_day) if time_of_day else None,
                    day_of_week=day_of_week,
                )
                st.success(f"✅ Scheduled '{st.session_state.workflow_config.name}' to run {frequency}")
        
        with col2:
            if st.button("🗑️ Remove Schedule", key="remove_schedule"):
                if scheduler.delete_schedule(st.session_state.selected_workflow):
                    st.success("✅ Schedule removed")
                else:
                    st.warning("No schedule found for this workflow")
        
        st.divider()
        st.markdown("### Scheduled Workflows")
        schedules = scheduler.list_schedules()
        auto_run_due_schedules()

        due_schedules = get_due_schedules()
        if due_schedules:
            st.info(f"⚠️ {len(due_schedules)} scheduled workflow(s) due to run now")
            if st.button("▶️ Run Due Scheduled Workflows", key="run_due_schedules"):
                for schedule in due_schedules:
                    comparison, error = run_scheduled_workflow(schedule)
                    if comparison is not None:
                        st.success(f"✅ Scheduled run completed: {schedule.workflow_id}")
                        st.session_state["latest_data"] = comparison
                        st.session_state.data = comparison
                    else:
                        st.error(f"❌ Scheduled run failed: {schedule.workflow_id} ({error})")

        if schedules:
            for sched in schedules:
                with st.container():
                    st.caption(f"**{sched.workflow_id}**")
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Frequency", sched.frequency)
                    with col2:
                        st.metric("Runs", sched.run_count)
                    with col3:
                        status = "🟢 Enabled" if sched.enabled else "🔴 Disabled"
                        if scheduler.is_due(sched):
                            status = "⚠️ Due Now"
                        st.metric("Status", status)
                    if sched.last_run:
                        st.caption(f"Last run: {sched.last_run}")
                    next_run = scheduler.get_next_run(sched)
                    if next_run:
                        st.caption(f"Next run: {next_run.isoformat(sep=' ', timespec='minutes')}")
        else:
            st.info("No schedules configured yet")

    st.divider()
    st.markdown("## ⚙️ Configuration")
    
    dataset_option = st.selectbox("Choose Dataset", ["Bank Data (Default)", "Custom Dataset"])
    selected_config = config
    
    st.divider()
    st.markdown("### 🔗 Data Source")
    
    source_type = st.selectbox(
        "Select Data Source Type",
        ["Default (Web)", "Playwright (Dynamic Web)", "CSV", "Upload Dataset", "API (Future)"]
    )
    
    keyword = None
    selector = None
    custom_url = None
    scrape_selector = None
    
    if source_type == "Playwright (Dynamic Web)":
        custom_url = st.text_input("Target URL", placeholder="https://...")
        selector = st.text_input("CSS Selector", placeholder="e.g., article.product")
        if not selector:
            st.warning("⚠️ Playwright requires a CSS selector")
        keyword = st.text_input("Keyword Filter (optional)")
    elif source_type == "Default (Web)":
        custom_url = st.text_input("Target URL", value=config.url)
    
    st.divider()
    st.markdown("### 📋 Transformation")
    
    scrape_mode = st.selectbox("Scraping Mode", ["Auto Detect", "Table Extraction", "Full Page Text", "Custom Selector"])
    if scrape_mode == "Custom Selector":
        scrape_selector = st.text_input("Enter CSS selector")
    
    load_option = st.selectbox("Load Destination", ["CSV", "Database", "Both"])
    
    st.divider()
    st.markdown("### 🔧 Rules")
    drop_nulls = st.checkbox("Drop Null Values")
    filter_enabled = st.checkbox("Enable Filter")
    filter_condition = st.text_input("Filter Condition") if filter_enabled else None
    rename_enabled = st.checkbox("Rename Columns")
    old_col = st.text_input("Old Column Name") if rename_enabled else None
    new_col = st.text_input("New Column Name") if rename_enabled else None

# =============================
# MAIN TABS
# =============================
tab1, tab2, tab3, tab4 = st.tabs(["🚀 Pipeline", "📊 History", "📈 Monitoring", "📋 Data"])

# =============================
# TAB 1: PIPELINE
# =============================
with tab1:
    st.markdown("## Run ETL Pipeline")
    
    # Upload section
    with st.expander("📤 Upload Custom Dataset"):
        uploaded_file = st.file_uploader("Upload CSV", type=["csv"])
        if uploaded_file is not None:
            try:
                st.session_state.uploaded_df = pd.read_csv(uploaded_file, engine="python", on_bad_lines="skip")
                st.session_state.uploaded_df.columns = [col.replace("items.*.", "") for col in st.session_state.uploaded_df.columns]
                st.success(f"✅ Loaded {len(st.session_state.uploaded_df)} rows")
            except Exception as e:
                st.error(f"❌ Upload failed: {e}")
    
    col1, col2, col3, col4 = st.columns(4)
    
    # Full pipeline
    with col1:
        if st.button("🔵 Full Pipeline", use_container_width=True):
            st.session_state.pipeline_status = "Running"
            st.session_state.error_message = ""
            progress_bar = st.progress(0)
            
            try:
                clean_source_type = normalize_source_type(source_type)
                uploaded_df = st.session_state.get("uploaded_df")
                
                final_selector = None
                if scrape_mode == "Custom Selector" and scrape_selector:
                    final_selector = scrape_selector
                elif selector:
                    final_selector = selector
                
                if clean_source_type == "upload dataset" and uploaded_df is None:
                    raise Exception("Please upload a dataset first")
                
                rules = []
                if drop_nulls:
                    rules.append({"type": "drop_nulls"})
                if filter_enabled and filter_condition:
                    rules.append({"type": "filter", "condition": filter_condition})
                if rename_enabled and old_col and new_col:
                    rules.append({"type": "rename", "columns": {old_col: new_col}})
                
                if clean_source_type == "upload dataset":
                    df = uploaded_df
                    df.columns = [col.replace("items.*.", "") for col in df.columns]
                    engine = TransformEngine(df)
                    df = engine.apply(rules)

                    st.session_state.data = df
                    st.session_state.extract_status = "Success"
                    st.session_state.transform_status = "Success"
                    progress_bar.progress(100)
                    st.session_state.execution_time = "< 1 sec"
                    st.success(f"✅ Complete | Shape: {df.shape}")
                else:
                    if custom_url:
                        selected_config.url = custom_url
                    
                    st.session_state.pipeline = ETLPipeline(selected_config)
                    pipeline = st.session_state.pipeline
                    
                    start = time.time()
                    result = pipeline.run(
                        source_type=clean_source_type,
                        uploaded_df=uploaded_df,
                        mode=scrape_mode,
                        selector=final_selector,
                        rules=rules,
                        load_option=load_option
                    )
                    elapsed = round(time.time() - start, 2)
                    
                    st.session_state.data = result["data"]
                    st.session_state.extract_status = "Success"
                    st.session_state.transform_status = "Success"
                    st.session_state.load_status = "Success"
                    st.session_state.execution_time = f"{elapsed} sec"
                    st.session_state.last_run_time = datetime.now()
                    
                    progress_bar.progress(100)
                    st.success(f"✅ Complete | Shape: {result['shape']} | Time: {elapsed}s")
                    
            except Exception as e:
                st.session_state.pipeline_status = "Failed"
                st.session_state.error_message = str(e)
                st.error(f"❌ Failed: {e}")
    
    # Individual step buttons
    with col2:
        if st.button("🔵 Extract", use_container_width=True):
            try:
                with st.spinner("Extracting..."):
                    clean_source_type = normalize_source_type(source_type)
                    final_selector = scrape_selector if scrape_mode == "Custom Selector" else selector
                    if custom_url and source_type == "Playwright (Dynamic Web)":
                        selected_config.url = custom_url
                    df = run_extraction(source_type=clean_source_type, config=selected_config, 
                                      uploaded_df=st.session_state.get("uploaded_df"), mode=scrape_mode, selector=final_selector)
                    st.session_state.data = df
                    st.session_state.extract_status = "Success"
                st.success("✅ Extract complete")
            except Exception as e:
                st.session_state.extract_status = "Failed"
                st.error(f"❌ {e}")
    
    with col3:
        if st.button("🟣 Transform", use_container_width=True, disabled=(st.session_state.extract_status != "Success")):
            try:
                with st.spinner("Transforming..."):
                    df = st.session_state.data
                    rules = []
                    if drop_nulls:
                        rules.append({"type": "drop_nulls"})
                    if filter_enabled and filter_condition:
                        rules.append({"type": "filter", "condition": filter_condition})
                    if rename_enabled and old_col and new_col:
                        rules.append({"type": "rename", "columns": {old_col: new_col}})
                    engine = TransformEngine(df)
                    df = engine.apply(rules)
                    st.session_state.data = df
                    st.session_state.transform_status = "Success"
                st.success("✅ Transform complete")
            except Exception as e:
                st.session_state.transform_status = "Failed"
                st.error(f"❌ {e}")
    
    with col4:
        if st.button("🟠 Load", use_container_width=True, disabled=(st.session_state.transform_status != "Success")):
            try:
                with st.spinner("Loading..."):
                    df = st.session_state.data
                    if load_option == "CSV":
                        load_to_csv(df, config.csv_path)
                    elif load_option == "Database":
                        conn = sqlite3.connect(config.db_name)
                        load_to_db(df, conn, config.table_name)
                        conn.close()
                    elif load_option == "Both":
                        load_to_csv(df, config.csv_path)
                        conn = sqlite3.connect(config.db_name)
                        load_to_db(df, conn, config.table_name)
                        conn.close()
                    st.session_state.load_status = "Success"
                st.success("✅ Load complete")
            except Exception as e:
                st.session_state.load_status = "Failed"
                st.error(f"❌ {e}")
    
    st.divider()
    st.markdown("### 🚦 Pipeline Status")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        if st.session_state.extract_status == "Success":
            st.success("✅ Extract")
        elif st.session_state.extract_status == "Failed":
            st.error("❌ Extract")
        else:
            st.info("⚪ Extract")
    
    with col2:
        if st.session_state.transform_status == "Success":
            st.success("✅ Transform")
        elif st.session_state.transform_status == "Failed":
            st.error("❌ Transform")
        else:
            st.info("⚪ Transform")
    
    with col3:
        if st.session_state.load_status == "Success":
            st.success("✅ Load")
        elif st.session_state.load_status == "Failed":
            st.error("❌ Load")
        else:
            st.info("⚪ Load")
    
    with col4:
        if st.session_state.last_run_time:
            st.info(f"⏱️ {st.session_state.execution_time}")
        else:
            st.info("⏱️ Not run")

# =============================
# TAB 2: HISTORY
# =============================
with tab2:
    st.markdown("## 📊 Price History & Alerts")
    
    history_file = "price_history.csv"
    if os.path.exists(history_file):
        df_history = pd.read_csv(history_file)
        if not df_history.empty:
            st.markdown(f"### History ({len(df_history)} records)")
            st.dataframe(df_history.tail(20), use_container_width=True)
            csv = df_history.to_csv(index=False)
            st.download_button("📥 Download Full History", data=csv, 
                             file_name=f"price_history_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv", mime="text/csv")
    else:
        st.info("No price history yet.")

    st.divider()
    st.markdown("## 🧾 Workflow Execution History")
    execution_history = workflow_history_store.get_latest_runs(limit=20)
    if execution_history:
        history_rows = [
            {
                "run_id": row.get("run_id"),
                "workflow_id": row.get("workflow_id"),
                "status": row.get("status"),
                "started_at": row.get("start_time"),
                "ended_at": row.get("end_time"),
                "duration_seconds": row.get("total_duration"),
                "alerts_generated": row.get("alerts_generated", 0),
            }
            for row in execution_history
        ]
        st.dataframe(pd.DataFrame(history_rows), use_container_width=True)
    else:
        st.info("No workflow execution history yet.")

# =============================
# TAB 3: MONITORING
# =============================
with tab3:
    st.markdown("## 🚀 Run Price Monitoring")
    
    if st.button(f"▶️ Run Workflow Definition: {workflow.name}", use_container_width=True):
        try:
            with st.spinner(f"Executing workflow '{workflow.name}'..."):
                result = workflow_runner.execute_workflow(st.session_state.selected_workflow)
                st.session_state["latest_execution"] = result
                if result.get("status") == "failed":
                    st.error(f"❌ Workflow failed: {result.get('error')}")
                else:
                    st.success(f"✅ Workflow completed in {result.get('total_duration', 0):.2f}s")
                    comparison = result.get("comparison")
                    if comparison is not None:
                        st.session_state["latest_data"] = comparison
                        st.session_state.data = comparison
                    for step in result.get("steps", []):
                        st.text(f"- {step.get('name')} : {step.get('status')} ({step.get('duration', 0):.2f}s)")
        except Exception as e:
            st.error(f"❌ Failed: {e}")
    
    st.divider()
    
    if "latest_data" in st.session_state and st.session_state["latest_data"] is not None:
        history_file = "price_history.csv"
        if os.path.exists(history_file):
            df_history = pd.read_csv(history_file)
            if not df_history.empty:
                changes = detect_price_changes(df_history)
                alerts = generate_alerts(changes, workflow.alert_rules)
                st.markdown("### 🚨 Alerts")
                if alerts and alerts[0] not in ["No price changes detected", "No alerts triggered by workflow rules"]:
                    for alert in alerts:
                        st.warning(alert)
                    st.dataframe(changes, use_container_width=True)
                    
                    st.markdown("### 📋 Export Results")
                    col1, col2, col3, col4 = st.columns(4)
                    
                    with col1:
                        csv_file = reporter.export_alerts_csv(alerts, workflow.name)
                        st.download_button(
                            "📥 Alerts CSV",
                            data=open(csv_file, 'rb').read(),
                            file_name=os.path.basename(csv_file),
                            mime="text/csv"
                        )
                    
                    with col2:
                        comparison_data = st.session_state.data
                        if comparison_data is not None:
                            csv_file = reporter.export_comparison_csv(comparison_data, workflow.name)
                            st.download_button(
                                "📥 Comparison CSV",
                                data=open(csv_file, 'rb').read(),
                                file_name=os.path.basename(csv_file),
                                mime="text/csv"
                            )
                    
                    with col3:
                        if st.button("📄 Alerts PDF"):
                            try:
                                pdf_file = reporter.export_alerts_pdf(alerts, workflow.name)
                                if pdf_file:
                                    with open(pdf_file, 'rb') as f:
                                        st.download_button(
                                            "⬇️ Download Alerts PDF",
                                            data=f.read(),
                                            file_name=os.path.basename(pdf_file),
                                            mime="application/pdf"
                                        )
                                else:
                                    st.warning("PDF generation not available")
                            except Exception as e:
                                st.error(f"PDF generation failed: {e}")
                    
                    with col4:
                        if st.button("📄 Comparison PDF"):
                            try:
                                pdf_file = reporter.export_comparison_pdf(comparison_data, workflow.name)
                                if pdf_file:
                                    with open(pdf_file, 'rb') as f:
                                        st.download_button(
                                            "⬇️ Download Comparison PDF",
                                            data=f.read(),
                                            file_name=os.path.basename(pdf_file),
                                            mime="application/pdf"
                                        )
                                else:
                                    st.warning("PDF generation not available")
                            except Exception as e:
                                st.error(f"PDF generation failed: {e}")

                    st.markdown("---")
                    summary = reporter.generate_summary(
                        st.session_state.data,
                        alerts,
                        workflow.name
                    )
                    st.metric("Total Alerts", len(alerts))
                else:
                    st.info("✅ No alerts triggered by workflow rules")

# =============================
# TAB 4: DATA
with tab4:
    st.markdown("## 📊 Current Data")
    
    if st.session_state.data is not None:
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Rows", st.session_state.data.shape[0])
        with col2:
            st.metric("Columns", st.session_state.data.shape[1])
        with col3:
            st.metric("Memory (MB)", round(st.session_state.data.memory_usage(deep=True).sum() / 1024 / 1024, 2))
        
        st.divider()
        st.dataframe(st.session_state.data, use_container_width=True)
        
        csv = st.session_state.data.to_csv(index=False)
        st.download_button("📥 Download Data", data=csv, 
                         file_name=f"extracted_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv", mime="text/csv")
    else:
        st.info("Run the pipeline to see data")
    
    st.divider()
    st.markdown("### 📝 Logs")
    
    if os.path.exists(LOG_FILE):
        with open(LOG_FILE, "r") as f:
            logs = f.read()
        st.text_area("Pipeline Logs", logs[-2000:], height=200, disabled=True)
        if st.button("🔄 Refresh Logs"):
            st.rerun()
    else:
        st.info("No logs yet")
