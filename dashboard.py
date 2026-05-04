import os

import streamlit as st
import pandas as pd
import sqlite3
from src.orchestrator import ETLPipeline
import src.config as config
from src.utils import LOG_FILE
from src.load import load_to_csv, load_to_db
from src.extract.extract import run_extraction
from src.transform.engine import TransformEngine
import time
from src.storage.history_store import save_snapshot, detect_price_changes
from src.alerts.alert_engine import generate_alerts

# import your multi-source pipeline function
from test_parser import run_multi_source_pipeline


st.set_page_config(page_title="ETL Pipeline Dashboard", layout="wide")

st.write("RUNNING FILE:", __file__)
st.title("💰 Competitor Price Monitor")
st.caption("Track competitor pricing across platforms in real-time")


def normalize_source_type(source_type):
    mapping = {
        "Default (Web)": "default (web)",
        "Playwright (Dynamic Web)": "playwright",
        "Selenium (Dynamic Web)": "selenium",
        "CSV": "csv",
        "Upload Dataset": "upload dataset",
        "API (Future)": "api"
    }

    return mapping.get(source_type, source_type.strip().lower())


# -----------------------------
# SESSION STATE INIT
# -----------------------------
defaults = {
    "pipeline_status": "Idle",
    "last_run_time": None,
    "error_message": "",
    "refresh_count": 0,
    "execution_time": None,
    "data": None,
    "last_log_refresh": None,
    "extract_status": "Idle",
    "transform_status": "Idle",
    "load_status": "Idle",
}

for k, v in defaults.items():
    if k not in st.session_state:
        st.session_state[k] = v


# -----------------------------
# DATASET
# -----------------------------
st.markdown("## Dataset Selection")

dataset_option = st.selectbox(
    "Choose dataset",
    ["Bank Data (Default)", "Custom Dataset"]
)

selected_config = config


# -----------------------------
# PIPELINE INSTANCE
# -----------------------------
if "pipeline" not in st.session_state:
    st.session_state.pipeline = ETLPipeline(selected_config)
else:
    st.session_state.pipeline.config = selected_config

pipeline = st.session_state.pipeline


# -----------------------------
# SOURCE TYPE
# -----------------------------
source_type = st.selectbox(
    "Select Data Source Type",
    ["Default (Web)", "Playwright (Dynamic Web)", "CSV", "Upload Dataset", "API (Future)"]
)

keyword = None
selector = None
custom_url = None
scrape_selector = None


# -----------------------------
# PLAYWRIGHT INPUTS
# -----------------------------
if source_type == "Playwright (Dynamic Web)":
    custom_url = st.text_input("Enter Target URL")

    selector = st.text_input(
        "Enter CSS Selector (REQUIRED for Playwright)",
        placeholder="e.g. article.product_pod"
    )

    if not selector:
        st.warning("⚠️ Playwright requires a selector for now")

    keyword = st.text_input("Enter Keyword Filter (optional)")


# -----------------------------
# UPLOAD
# -----------------------------
st.markdown("### Upload Custom Dataset")

uploaded_file = st.file_uploader("Upload CSV", type=["csv"])

if uploaded_file is not None:
    try:
        st.session_state.uploaded_df = pd.read_csv(
            uploaded_file,
            engine="python",
            on_bad_lines="skip"
        )
        st.success("Custom dataset loaded ✅")
    except Exception as e:
        st.error(f"Upload failed: {e}")


# -----------------------------
# CONTROL PANEL
# -----------------------------
st.markdown("## Control Panel")

custom_url_input = None
if source_type == "Default (Web)":
    custom_url_input = st.text_input("Enter Target URL")

if custom_url_input:
    selected_config.url = custom_url_input

elif source_type == "Playwright (Dynamic Web)":
    if custom_url:
        selected_config.url = custom_url

if keyword:
    selected_config.keyword = keyword


scrape_mode = st.selectbox(
    "Scraping Mode",
    ["Auto Detect", "Table Extraction", "Full Page Text", "Custom Selector"]
)

scrape_selector = None
if scrape_mode == "Custom Selector":
    scrape_selector = st.text_input("Enter CSS selector")


load_option = st.selectbox(
    "Select Load Destination",
    ["CSV", "Database", "Both"]
)


# -----------------------------
# RULE BUILDER
# -----------------------------
st.markdown("## Transformation Rules Builder")

drop_nulls = st.checkbox("Drop Null Values")
filter_enabled = st.checkbox("Enable Filter")
filter_condition = st.text_input("Filter Condition")
rename_enabled = st.checkbox("Rename Columns")
old_col = st.text_input("Old Column Name")
new_col = st.text_input("New Column Name")


# =============================
# PRICE MONITOR (FIXED PROPERLY)
# =============================
if st.button("🚀 Run Price Monitoring"):
    try:
        st.info("Running multi-source pipeline...")

        with st.spinner("Fetching competitor data..."):

            sources = {
                "jumia": {
                 "type": "playwright",
                 "url": "https://www.jumia.co.ke/electronics/",
                 "selector": "article.prd"
                 },
                "kilimall": {
                    "type": "playwright",
                    "url": "https://www.kilimall.co.ke/search?q=electronics",
                    "selector": ".product-item"
                }
            }

            # ✅ MUST be inside spinner
            comparison = run_multi_source_pipeline(sources, selected_config)

        # ✅ Save + store AFTER success
        save_snapshot(comparison)

        st.session_state["latest_data"] = comparison

        st.success("Monitoring complete ✅")

    except Exception as e:
        st.error(f"Error: {e}")


# =============================
# ALERTS (SAFE)
# =============================
if "latest_data" in st.session_state and st.session_state["latest_data"] is not None:

    history_file = "price_history.csv"

    if os.path.exists(history_file):

        df_history = pd.read_csv(history_file)

        if not df_history.empty:

            changes = detect_price_changes(df_history)
            alerts = generate_alerts(changes)

            st.markdown("## 🚨 Alerts")

            if alerts:
                for alert in alerts:
                    st.warning(alert)
            else:
                st.info("No price changes detected")


# =============================
# TABLE DISPLAY (UNCHANGED)
# =============================
st.markdown("## 📊 Latest Price Comparison")

if "latest_data" in st.session_state and st.session_state["latest_data"] is not None:
    st.dataframe(st.session_state["latest_data"])
else:
    st.info("Run monitoring to see data")


# -----------------------------
# PIPELINE BUTTONS (UNCHANGED)
# -----------------------------
col1, col2, col3, col4 = st.columns(4)


# =============================
# FULL PIPELINE
# =============================
with col1:
    if st.button("🔵 Run Full Pipeline"):

        st.session_state.pipeline_status = "Running"
        st.session_state.error_message = ""

        try:
            progress = st.progress(0)

            clean_source_type = normalize_source_type(source_type)
            uploaded_df = st.session_state.get("uploaded_df")

            final_selector = None

            if scrape_mode == "Custom Selector":
               if scrape_selector and scrape_selector.strip():
                  final_selector = scrape_selector

            elif selector and selector.strip():
                 final_selector = selector

            if clean_source_type == "Upload Dataset" and uploaded_df is None:
                raise Exception("Please upload a dataset first")

            rules = []

            if drop_nulls:
                rules.append({"type": "drop_nulls"})
            if filter_enabled and filter_condition:
                rules.append({"type": "filter", "condition": filter_condition})
            if rename_enabled and old_col and new_col:
                rules.append({"type": "rename", "columns": {old_col: new_col}})

            result = pipeline.run(
                source_type=clean_source_type,
                uploaded_df=uploaded_df,
                mode=scrape_mode,
                selector=final_selector,
                rules=rules,
                load_option=load_option
            )

            st.write("DEBUG EXTRACT RESULT:", result["extract"])

            progress.progress(30)
            st.session_state.extract_status = "Success"

            progress.progress(70)
            st.session_state.transform_status = "Success"

            progress.progress(100)
            st.session_state.load_status = "Success"

            st.session_state.data = result["data"]
            st.session_state.execution_time = f"{result['execution_time']} sec"

            st.success(f"Pipeline complete ✅ Shape: {result['shape']}")

        except Exception as e:
            st.session_state.pipeline_status = "Failed"
            st.error(f"Pipeline failed ❌: {e}")

# =============================
# EXTRACT ONLY
# =============================
with col2:
    if st.button("🔵 Run Extract"):
        try:
            clean_source_type = normalize_source_type(source_type)

            final_selector = None

            if scrape_mode == "Custom Selector":
                if scrape_selector and scrape_selector.strip():
                    final_selector = scrape_selector

            elif selector and selector.strip():
                final_selector = selector

            df = run_extraction(
                source_type=clean_source_type,
                config=selected_config,
                uploaded_df=st.session_state.get("uploaded_df"),
                mode=scrape_mode,
                selector=final_selector
            )

            st.session_state.data = df
            st.session_state.extract_status = "Success"

            st.success("Extract complete")

        except Exception as e:
            st.session_state.extract_status = "Failed"
            st.error(str(e))


# =============================
# TRANSFORM ONLY
# =============================
with col3:
    if st.button("🟣 Run Transform", disabled=(st.session_state.extract_status != "Success")):
        try:
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

            st.success("Transform complete")

        except Exception as e:
            st.session_state.transform_status = "Failed"
            st.error(str(e))


# =============================
# LOAD ONLY
# =============================
with col4:
    if st.button("🟠 Run Load", disabled=(st.session_state.transform_status != "Success")):
        try:
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

            st.success("Load complete")

        except Exception as e:
            st.session_state.load_status = "Failed"
            st.error(str(e))
# =============================
# STATUS UI (UNCHANGED)
# =============================
st.markdown("## 🚦 Pipeline Status")


def status_box(label, status):
    if status == "Idle":
        st.info(f"{label} ⚪ Idle")
    elif status == "Running":
        st.warning(f"{label} 🟡 Running")
    elif status == "Success":
        st.success(f"{label} 🟢 Done")
    elif status == "Failed":
        st.error(f"{label} 🔴 Failed")


c1, c2, c3 = st.columns(3)

with c1:
    status_box("Extract", st.session_state.extract_status)

with c2:
    status_box("Transform", st.session_state.transform_status)

with c3:
    status_box("Load", st.session_state.load_status)


# -----------------------------
# DATA PREVIEW
# -----------------------------
st.markdown("## Data Preview")

if st.session_state.data is not None:
    st.dataframe(st.session_state.data.head())
else:
    st.info("Run pipeline to see data")