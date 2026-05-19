"""
API-facing workflow execution service.

This service is the control-plane boundary for FlowSync. It owns run isolation,
status synchronization, async-safe execution, and error propagation. UI code
should call this through HTTP endpoints, while future Kafka consumers, Airflow
operators, or worker queues can reuse the same service methods.
"""

from __future__ import annotations

import json
import os
import threading
from concurrent.futures import Future, ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, List, Optional
from uuid import uuid4

from src.contracts.api import (
    ConnectorTestRequest,
    RunStatusRecord,
    SourceSyncRequest,
    WorkflowCreateRequest,
    WorkflowRunRequest,
    WorkflowRunResponse,
    public_dict,
    utc_now_iso,
)
from src.extract.extract import run_extraction
from src.storage.workflow_history import workflow_history_store
from src.telemetry.pipeline_logger import PipelineLogger
from src.workflow_runner import WorkflowRunner, runner as global_runner
import src.config as config


class RunStatusStore:
    """Small persistent status store for API polling and async workers."""

    def __init__(self, filepath: Optional[str] = None):
        self.filepath = filepath or os.path.join("src", "storage", "workflow_runs.json")
        self._lock = threading.RLock()
        os.makedirs(os.path.dirname(self.filepath), exist_ok=True)

    def upsert(self, record: RunStatusRecord) -> RunStatusRecord:
        with self._lock:
            records = self._load()
            records[record.run_id] = public_dict(record)
            self._save(records)
        return record

    def update(self, run_id: str, **changes: Any) -> Optional[RunStatusRecord]:
        with self._lock:
            records = self._load()
            existing = records.get(run_id)
            if not existing:
                return None
            existing.update(changes)
            existing["updated_at"] = utc_now_iso()
            records[run_id] = existing
            self._save(records)
            return RunStatusRecord(**existing)

    def get(self, run_id: str) -> Optional[RunStatusRecord]:
        record = self._load().get(run_id)
        return RunStatusRecord(**record) if record else None

    def list(self, workflow_id: Optional[str] = None, limit: int = 50) -> List[RunStatusRecord]:
        records = list(self._load().values())
        if workflow_id:
            records = [record for record in records if record.get("workflow_id") == workflow_id]
        records = sorted(records, key=lambda item: item.get("updated_at", ""), reverse=True)
        return [RunStatusRecord(**record) for record in records[:limit]]

    def _load(self) -> Dict[str, Dict[str, Any]]:
        if not os.path.exists(self.filepath):
            return {}
        try:
            with open(self.filepath, "r", encoding="utf-8") as f:
                data = json.load(f)
            return data if isinstance(data, dict) else {}
        except (json.JSONDecodeError, OSError):
            return {}

    def _save(self, records: Dict[str, Dict[str, Any]]) -> None:
        with open(self.filepath, "w", encoding="utf-8") as f:
            json.dump(records, f, indent=2, default=str)


class WorkflowExecutionService:
    """Public service used by API routes; keeps UI detached from internals."""

    def __init__(
        self,
        runner: WorkflowRunner = global_runner,
        status_store: Optional[RunStatusStore] = None,
        max_workers: int = 4,
    ):
        self.runner = runner
        self.status_store = status_store or RunStatusStore()
        self.executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="flowsync-workflow")
        self._futures: Dict[str, Future] = {}
        self._futures_lock = threading.RLock()

    def run_workflow(self, request: WorkflowRunRequest) -> WorkflowRunResponse:
        if not self.runner.get_workflow(request.workflow_id):
            return WorkflowRunResponse(
                run_id=request.run_id or str(uuid4()),
                workflow_id=request.workflow_id,
                status="failed",
                accepted=False,
                message=f"Workflow {request.workflow_id} not found",
                error=f"Workflow {request.workflow_id} not found",
            )

        run_id = request.run_id or str(uuid4())
        submitted_at = utc_now_iso()
        self.status_store.upsert(
            RunStatusRecord(
                run_id=run_id,
                workflow_id=request.workflow_id,
                status="queued" if request.async_execution else "running",
                submitted_at=submitted_at,
                updated_at=submitted_at,
                triggered_by=request.triggered_by,
                metadata=request.metadata,
            )
        )

        if request.async_execution:
            future = self.executor.submit(self._execute_workflow, request, run_id)
            with self._futures_lock:
                self._futures[run_id] = future
            return WorkflowRunResponse(
                run_id=run_id,
                workflow_id=request.workflow_id,
                status="queued",
                accepted=True,
                message="Workflow execution queued",
                submitted_at=submitted_at,
            )

        result = self._execute_workflow(request, run_id)
        status = result.get("status", "unknown")
        return WorkflowRunResponse(
            run_id=run_id,
            workflow_id=request.workflow_id,
            status=status,
            accepted=status != "failed",
            message="Workflow execution completed",
            submitted_at=submitted_at,
            result=self._summarize_result(result),
            error=result.get("error"),
        )

    def _execute_workflow(self, request: WorkflowRunRequest, run_id: str) -> Dict[str, Any]:
        self.status_store.update(run_id, status="running")
        try:
            result = self.runner.execute_workflow(
                request.workflow_id,
                run_id=run_id,
                triggered_by=request.triggered_by,
                metadata={
                    **request.metadata,
                    # Future Airflow/Kafka workers can preserve this source.
                    "api_boundary": "WorkflowExecutionService",
                },
            )
            self.status_store.update(
                run_id,
                status=result.get("status", "unknown"),
                result=self._summarize_result(result),
                error=result.get("error"),
            )
            return result
        except Exception as exc:
            error = str(exc)
            self.status_store.update(run_id, status="failed", error=error)
            raise

    def create_workflow(self, request: WorkflowCreateRequest) -> Dict[str, Any]:
        workflow_def = request.to_workflow_definition()
        workflow_path = Path(self.runner.workflows_dir) / f"{request.workflow_id}.json"
        workflow_path.parent.mkdir(parents=True, exist_ok=True)
        with open(workflow_path, "w", encoding="utf-8") as f:
            json.dump(workflow_def, f, indent=2)
        self.runner.load_workflows()
        return {
            "workflow_id": request.workflow_id,
            "status": "created",
            "path": str(workflow_path),
        }

    def get_workflow_history(
        self,
        workflow_id: Optional[str] = None,
        run_id: Optional[str] = None,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        history = workflow_history_store.get_history(workflow_id)
        if run_id:
            history = [entry for entry in history if entry.get("run_id") == run_id]
        return history[-limit:][::-1]

    def get_run_status(self, run_id: str) -> Optional[Dict[str, Any]]:
        record = self.status_store.get(run_id)
        return public_dict(record) if record else None

    def list_run_statuses(self, workflow_id: Optional[str] = None, limit: int = 50) -> List[Dict[str, Any]]:
        return [public_dict(record) for record in self.status_store.list(workflow_id, limit)]

    def test_connector(self, request: ConnectorTestRequest) -> Dict[str, Any]:
        logger = PipelineLogger("connector_test")
        logger.start(
            metadata={
                "source_type": request.source_type,
                "url": request.url,
                "selector": request.selector,
                "api_boundary": "connectors/test",
            }
        )
        try:
            execution_config = self._make_execution_config(
                url=request.url,
                keyword=request.keyword,
            )
            df = run_extraction(
                source_type=request.source_type,
                config=execution_config,
                mode=request.mode,
                selector=request.selector,
            )
            rows = len(df)
            logger.success(records_processed=rows, metadata={"operation": "connector_test"})
            return {
                "status": "success",
                "source_type": request.source_type,
                "rows": rows,
                "columns": list(df.columns),
                "sample": df.head(request.sample_limit).to_dict(orient="records"),
            }
        except Exception as exc:
            logger.failure(exc, metadata={"operation": "connector_test"})
            return {"status": "failed", "source_type": request.source_type, "error": str(exc)}

    def sync_source(self, request: SourceSyncRequest) -> Dict[str, Any]:
        test_request = ConnectorTestRequest(
            source_type=request.source_type,
            url=request.url,
            selector=request.selector,
            mode=request.mode,
            keyword=request.keyword,
            sample_limit=0,
            metadata=request.metadata,
        )
        result = self.test_connector(test_request)
        result.update(
            {
                "run_id": request.run_id,
                "workflow_id": request.workflow_id,
                "source_name": request.source_name,
            }
        )
        return result

    def get_latest_reports(self, reports_dir: str = "reports", limit: int = 10) -> List[Dict[str, Any]]:
        if not os.path.isdir(reports_dir):
            return []
        files = []
        for path in Path(reports_dir).glob("*"):
            if not path.is_file():
                continue
            stat = path.stat()
            files.append(
                {
                    "name": path.name,
                    "path": str(path),
                    "size_bytes": stat.st_size,
                    "updated_at": stat.st_mtime,
                }
            )
        return sorted(files, key=lambda item: item["updated_at"], reverse=True)[:limit]

    def get_source_health(self) -> List[Dict[str, Any]]:
        health = []
        for workflow_id in self.runner.list_workflows():
            workflow = self.runner.get_workflow(workflow_id) or {}
            for source in workflow.get("internal_sources", []) + workflow.get("external_sources", []) + workflow.get("sources", []):
                health.append(
                    {
                        "workflow_id": workflow_id,
                        "source_name": source.get("name"),
                        "source_type": source.get("source_type") or source.get("type"),
                        "url": source.get("url") or source.get("file_path"),
                        "status": "configured",
                    }
                )
        return health

    def _summarize_result(self, result: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "run_id": result.get("run_id"),
            "workflow_id": result.get("workflow_id"),
            "workflow_name": result.get("workflow_name"),
            "status": result.get("status"),
            "error": result.get("error"),
            "alerts_generated": result.get("alerts_generated", 0),
            "report_paths": result.get("report_paths", []),
            "comparison_shape": result.get("comparison_shape"),
            "total_duration": result.get("total_duration"),
        }

    def _make_execution_config(self, **overrides: Any):
        values = {
            key: value
            for key, value in vars(config).items()
            if not key.startswith("__")
        }
        values.update({key: value for key, value in overrides.items() if value is not None})
        return SimpleNamespace(**values)


workflow_execution_service = WorkflowExecutionService()
