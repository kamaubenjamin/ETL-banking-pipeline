#!/usr/bin/env python3
"""
Enhanced Streamlit Dashboard - Write to file
This script generates the enhanced dashboard code
"""

dashboard_code = '''import os
import streamlit as st
import pandas as pd
import sqlite3
from datetime import datetime
from src.orchestrator import ETLPipeline
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
}

for k, v in defaults.items():
    if k not in st.session_state:
        st.session_state[k] = v

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

# =============================
# TAB 3: MONITORING
# =============================
with tab3:
    st.markdown("## 🚀 Run Price Monitoring")
    
    if st.button("Start Multi-Source Monitoring", use_container_width=True):
        try:
            with st.spinner("Fetching competitor data..."):
                sources = {
                    "jumia": {"type": "playwright", "url": "https://www.jumia.co.ke/electronics/", "selector": "article.prd"},
                    "kilimall": {"type": "playwright", "url": "https://www.kilimall.co.ke/search?q=electronics", "selector": ".product-item"}
                }
                comparison = run_multi_source_pipeline(sources, selected_config)
                st.session_state["latest_data"] = comparison
                st.success("✅ Monitoring complete")
        except Exception as e:
            st.error(f"❌ Failed: {e}")
    
    st.divider()
    
    if "latest_data" in st.session_state and st.session_state["latest_data"] is not None:
        history_file = "price_history.csv"
        if os.path.exists(history_file):
            df_history = pd.read_csv(history_file)
            if not df_history.empty:
                changes = detect_price_changes(df_history)
                alerts = generate_alerts(changes)
                st.markdown("### 🚨 Alerts")
                if alerts and alerts[0] != "No price changes detected":
                    for alert in alerts:
                        st.warning(alert)
                else:
                    st.info("✅ No price changes detected")

# =============================
# TAB 4: DATA
# =============================
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
'''

# Write the dashboard code to file
with open('dashboard.py', 'w', encoding='utf-8') as f:
    f.write(dashboard_code)

print("✅ Enhanced dashboard created successfully!")
