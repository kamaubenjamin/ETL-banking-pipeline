"""
Workflow Runner - Orchestration engine for declarative workflow execution.
Loads workflow definitions from JSON and executes them end-to-end.
"""
import json
import os
from datetime import datetime
from types import SimpleNamespace
from typing import Dict, Any, Optional, List
from uuid import uuid4
import pandas as pd

from src.scheduler import scheduler
from src.reporter import reporter
from src.workflows import WorkflowConfig, SourceConfig, registry
from src.pipeline.multi_source_pipeline import run_multi_source_pipeline
from src.storage.history_store import save_snapshot, detect_price_changes
from src.storage.workflow_history import workflow_history_store
from src.alerts.alert_engine import generate_alerts
from src.services.alert_manager import AlertManager
from src.telemetry.pipeline_logger import PipelineLogger
from src.transform.comparison_engine import (
    combine_datasets,
    match_products,
    build_comparison_table,
    compare_supplier_vs_market,
    detect_supplier_undercut,
)
import src.config as config


class WorkflowRunner:
    """Execute workflows from declarative JSON definitions."""

    def __init__(self, workflows_dir: str = "workflows"):
        self.workflows_dir = workflows_dir
        self.workflows: Dict[str, Dict[str, Any]] = {}
        self.execution_history: List[Dict[str, Any]] = []
        self.alert_manager = AlertManager()
        self.load_workflows()

    def load_workflows(self):
        """Load all workflow definitions from JSON files."""
        if not os.path.isdir(self.workflows_dir):
            os.makedirs(self.workflows_dir, exist_ok=True)
            return

        for filename in os.listdir(self.workflows_dir):
            if filename.endswith(".json"):
                filepath = os.path.join(self.workflows_dir, filename)
                try:
                    with open(filepath, 'r') as f:
                        workflow_def = json.load(f)
                    workflow_id = workflow_def.get("workflow_id", filename.replace(".json", ""))
                    self.workflows[workflow_id] = workflow_def
                except Exception as e:
                    print(f"❌ Failed to load workflow {filename}: {e}")

    def get_workflow(self, workflow_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve a workflow definition by ID."""
        return self.workflows.get(workflow_id)

    def list_workflows(self) -> List[str]:
        """List all available workflow IDs."""
        return list(self.workflows.keys())

    def workflow_to_config(self, workflow_def: Dict[str, Any]) -> WorkflowConfig:
        """Convert a workflow definition to a WorkflowConfig object."""
        sources = []

        # Handle internal sources
        for src in workflow_def.get("internal_sources", []):
            sources.append(SourceConfig(
                name=src["name"],
                source_type="internal",
                url=src.get("file_path"),  # Use url field for file path
                selector=None,
                keyword=None,
                match_threshold=src.get("match_threshold", 70),
                mode="internal",
            ))

        # Handle external sources
        for src in workflow_def.get("external_sources", []):
            sources.append(SourceConfig(
                name=src["name"],
                source_type=src["source_type"],
                url=src.get("url"),
                selector=src.get("selector"),
                keyword=src.get("keyword"),
                match_threshold=src.get("match_threshold", 70),
                mode="Auto Detect",
            ))

        # Legacy support for "sources" field
        for src in workflow_def.get("sources", []):
            sources.append(SourceConfig(
                name=src["name"],
                source_type=src["source_type"],
                url=src.get("url"),
                selector=src.get("selector"),
                keyword=src.get("keyword"),
                match_threshold=src.get("match_threshold", 70),
                mode="Auto Detect",
            ))

        return WorkflowConfig(
            workflow_id=workflow_def.get("workflow_id"),
            name=workflow_def.get("workflow_name", workflow_def.get("workflow_id")),
            description=workflow_def.get("description", ""),
            sources=sources,
            transformation_rules=workflow_def.get("transformation_rules", []),
            alert_rules=workflow_def.get("alert_rules", {}),
            global_match_threshold=workflow_def.get("global_match_threshold", 70),
            enabled=workflow_def.get("enabled", True),
        )

    def execute_workflow(
        self,
        workflow_id: str,
        run_id: Optional[str] = None,
        triggered_by: str = "manual",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Execute a complete workflow from definition to reporting."""
        workflow_def = self.get_workflow(workflow_id)
        if not workflow_def:
            return {
                "status": "failed",
                "error": f"Workflow {workflow_id} not found",
                "execution_time": 0,
            }

        start_time = datetime.now()
        run_id = run_id or str(uuid4())
        metadata = metadata or {}
        pipeline_logger = PipelineLogger(
            workflow_def.get("workflow_name", workflow_id),
            run_id=run_id,
        )
        pipeline_logger.start(
            metadata={
                "workflow_id": workflow_id,
                "triggered_by": triggered_by,
                # Future Airflow DAG integration can pass dag_run_id here and
                # use this FlowSync run_id as the external task correlation key.
                "orchestrator": "WorkflowRunner.execute_workflow",
                **metadata,
            },
            triggered_by=triggered_by,
        )
        execution_log = {
            "run_id": run_id,
            "workflow_id": workflow_id,
            "workflow_name": workflow_def.get("workflow_name"),
            "start_time": start_time.isoformat(),
            "status": "success",
            "error": None,
            "steps": [],
            "alerts_generated": 0,
            "report_paths": [],
            "comparison_shape": None,
            "triggered_by": triggered_by,
            "metadata": metadata,
        }

        try:
            workflow_config = self.workflow_to_config(workflow_def)
            execution_config = self._make_execution_config()

            # Execute each step
            for step in workflow_def.get("steps", []):
                step_start = datetime.now()
                step_log = {
                    "name": step,
                    "status": "success",
                    "duration": 0,
                    "message": "",
                }

                try:
                    if step == "extract":
                        pipeline_result = run_multi_source_pipeline(workflow_config, execution_config)
                        if isinstance(pipeline_result, tuple):
                            matched, comparison = pipeline_result
                            execution_log["matched"] = matched
                            execution_log["comparison"] = comparison
                        else:
                            # Fallback for older pipeline versions
                            execution_log["comparison"] = pipeline_result
                        step_log["message"] = f"Extracted from {len(workflow_config.sources)} sources"
                        if hasattr(comparison, "shape"):
                            execution_log["comparison_shape"] = list(comparison.shape)

                    elif step == "normalize":
                        step_log["message"] = "Normalized product data"

                    elif step == "fuzzy_match":
                        step_log["message"] = "Applied fuzzy matching and confidence scoring"

                    elif step == "compare":
                        step_log["message"] = "Compared products across sources"

                    elif step == "compare_supplier_vs_market":
                        if "matched" in execution_log:
                            supplier_analysis = compare_supplier_vs_market(execution_log["matched"])
                            execution_log["supplier_analysis"] = supplier_analysis
                            step_log["message"] = f"Analyzed {len(supplier_analysis)} supplier vs market comparisons"
                        else:
                            step_log["message"] = "No matched data for supplier analysis"

                    elif step == "detect_undercut":
                        if "supplier_analysis" in execution_log:
                            undercut_threshold = workflow_def.get("alert_rules", {}).get("undercut_threshold", 2000)
                            undercut_opportunities = detect_supplier_undercut(
                                execution_log["supplier_analysis"],
                                threshold=undercut_threshold
                            )
                            execution_log["undercut_opportunities"] = undercut_opportunities
                            step_log["message"] = f"Found {len(undercut_opportunities)} undercut opportunities"
                        else:
                            execution_log["undercut_opportunities"] = pd.DataFrame()
                            step_log["message"] = "No supplier analysis for undercut detection"
                        history_file = "price_history.csv"
                        if os.path.exists(history_file):
                            df_history = pd.read_csv(history_file)
                            changes = detect_price_changes(df_history)
                            execution_log["changes"] = changes
                            step_log["message"] = f"Detected {len(changes)} price changes"
                        else:
                            step_log["message"] = "No price history for comparison"

                    elif step == "generate_alerts":
                        if "changes" in execution_log:
                            alert_rules = workflow_def.get("alert_rules", {})
                            alerts = generate_alerts(
                                execution_log["changes"],
                                [alert_rules] if alert_rules else []
                            )
                            execution_log["alerts"] = alerts
                            execution_log["alerts_generated"] = len(alerts) if isinstance(alerts, list) else 0
                            step_log["message"] = f"Generated {len(alerts)} alerts"
                            for alert in alerts:
                                if isinstance(alert, str) and not alert.lower().startswith("no "):
                                    self.alert_manager.publish(
                                        alert,
                                        alert_type="price_monitoring",
                                        severity="warning",
                                        pipeline_name=workflow_def.get("workflow_name", workflow_id),
                                        pipeline_run_id=run_id,
                                        metadata={"workflow_id": workflow_id},
                                    )
                        else:
                            execution_log["alerts"] = []
                            execution_log["alerts_generated"] = 0
                            step_log["message"] = "No alerts to generate"

                    elif step == "generate_reports":
                        if "comparison" in execution_log:
                            csv_file = reporter.export_comparison_csv(
                                execution_log["comparison"],
                                workflow_def.get("workflow_name", workflow_id),
                            )
                            step_log["message"] = f"Generated report: {os.path.basename(csv_file)}"
                            execution_log["report_file"] = csv_file
                            execution_log["report_paths"].append(csv_file)

                            if workflow_def.get("reporting", {}).get("export_pdf"):
                                pdf_file = reporter.export_comparison_pdf(
                                    execution_log["comparison"],
                                    workflow_def.get("workflow_name", workflow_id),
                                )
                                if pdf_file:
                                    execution_log["pdf_file"] = pdf_file
                                    execution_log["report_paths"].append(pdf_file)

                    step_log["duration"] = (datetime.now() - step_start).total_seconds()
                    execution_log["steps"].append(step_log)

                except Exception as e:
                    step_log["status"] = "failed"
                    step_log["message"] = str(e)
                    step_log["duration"] = (datetime.now() - step_start).total_seconds()
                    execution_log["steps"].append(step_log)
                    execution_log["status"] = "partial"
                    if execution_log["error"] is None:
                        execution_log["error"] = f"Step '{step}' failed: {e}"

            # Record execution in history
            execution_log["end_time"] = datetime.now().isoformat()
            execution_log["total_duration"] = (datetime.now() - start_time).total_seconds()
            workflow_history_store.record_execution(execution_log)
            self.execution_history.append(execution_log)
            pipeline_logger.finalize(
                execution_log["status"],
                records_processed=execution_log["comparison_shape"][0]
                if execution_log.get("comparison_shape")
                else 0,
                error_message=execution_log.get("error"),
                metadata={
                    "workflow_id": workflow_id,
                    "alerts_generated": execution_log["alerts_generated"],
                    "report_paths": execution_log["report_paths"],
                },
            )

            # Update scheduler
            schedule = scheduler.get_schedule(workflow_id)
            if schedule:
                scheduler.record_run(workflow_id)

            return execution_log

        except Exception as e:
            execution_log["status"] = "failed"
            execution_log["error"] = str(e)
            execution_log["end_time"] = datetime.now().isoformat()
            execution_log["total_duration"] = (datetime.now() - start_time).total_seconds()
            workflow_history_store.record_execution(execution_log)
            self.execution_history.append(execution_log)
            pipeline_logger.failure(
                e,
                metadata={
                    "workflow_id": workflow_id,
                    "total_duration": execution_log["total_duration"],
                },
            )
            return execution_log

    def execute_due_workflows(self) -> List[Dict[str, Any]]:
        """Execute all workflows that are due to run."""
        results = []
        due_schedules = [s for s in scheduler.list_enabled() if scheduler.is_due(s)]

        for schedule in due_schedules:
            result = self.execute_workflow(schedule.workflow_id)
            results.append(result)

        return results

    def get_execution_history(self, workflow_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get execution history, optionally filtered by workflow ID."""
        if workflow_id:
            return [e for e in self.execution_history if e["workflow_id"] == workflow_id]
        return self.execution_history

    def register_workflow_to_registry(self, workflow_id: str) -> bool:
        """Register a workflow definition to the global registry."""
        workflow_def = self.get_workflow(workflow_id)
        if not workflow_def:
            return False

        config = self.workflow_to_config(workflow_def)
        registry.register(config)
        return True

    def _make_execution_config(self):
        """
        Create a run-scoped config snapshot.

        Existing connectors mutate config.url/keyword while processing sources.
        Keeping those mutations inside a per-run namespace preserves run_id
        isolation today and prepares the engine for async workers later.
        """
        return SimpleNamespace(
            **{
                key: value
                for key, value in vars(config).items()
                if not key.startswith("__")
            }
        )


# Global runner instance
runner = WorkflowRunner()
