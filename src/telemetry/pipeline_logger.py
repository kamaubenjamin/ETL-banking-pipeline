"""
Structured pipeline logging and FlowSync telemetry helpers.

PipelineLogger emits local JSON logs for operations teams and sends best-effort
records to Supabase. The interface is deliberately sync today, but the call
sites are isolated so future async workers can enqueue these same events instead
of writing inline.
"""

from __future__ import annotations

import json
import logging
import os
from contextlib import contextmanager
from datetime import datetime
from time import perf_counter
from typing import Any, Dict, Iterator, Optional
from uuid import uuid4

from src.contracts.telemetry import (
    IngestionLogEvent,
    OperationalAlertEvent,
    PipelineRunEvent,
    PipelineStatusUpdate,
    utc_now,
)
from src.telemetry.telemetry_manager import TelemetryManager, telemetry_manager


class JsonLogFormatter(logging.Formatter):
    """Format logs as JSON so FlowSync or log shippers can parse them."""

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        extra = getattr(record, "telemetry", None)
        if isinstance(extra, dict):
            payload.update(extra)
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def get_structured_logger(name: str = "flowsync.telemetry") -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(os.getenv("FLOWSYNC_LOG_LEVEL", "INFO").upper())
    handler = logging.StreamHandler()
    handler.setFormatter(JsonLogFormatter())
    logger.addHandler(handler)
    logger.propagate = False
    return logger


class PipelineLogger:
    """Lifecycle instrumentation for ETL and workflow pipelines."""

    def __init__(
        self,
        pipeline_name: str,
        *,
        run_id: Optional[str] = None,
        telemetry: Optional[TelemetryManager] = None,
        logger: Optional[logging.Logger] = None,
    ):
        self.pipeline_name = pipeline_name
        self.run_id = run_id or str(uuid4())
        self.telemetry = telemetry or telemetry_manager
        self.logger = logger or get_structured_logger()
        self.started_at = utc_now()
        self._timer_started_at = perf_counter()

    def start(self, *, metadata: Optional[Dict[str, Any]] = None, triggered_by: str = "etl-backend") -> None:
        event = PipelineRunEvent(
            run_id=self.run_id,
            pipeline_name=self.pipeline_name,
            status="running",
            started_at=self.started_at,
            triggered_by=triggered_by,
            metadata=metadata or {},
        )
        self.telemetry.log_pipeline_run(event)
        self._log("pipeline_started", "running", metadata=metadata)

    def success(
        self,
        *,
        records_processed: int = 0,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.finalize("success", records_processed=records_processed, metadata=metadata)

    def failure(
        self,
        error: Exception | str,
        *,
        records_processed: int = 0,
        metadata: Optional[Dict[str, Any]] = None,
        status: str = "failed",
    ) -> None:
        error_message = str(error)
        self.finalize(
            status,
            records_processed=records_processed,
            error_message=error_message,
            metadata=metadata,
        )
        self.log_alert(
            message=error_message,
            alert_type="pipeline_failure",
            severity="critical" if status == "failed" else "warning",
            metadata=metadata,
        )

    def finalize(
        self,
        status: str,
        *,
        records_processed: int = 0,
        error_message: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        duration = self.duration_seconds
        completed_at = utc_now()
        update = PipelineStatusUpdate(
            pipeline_run_id=self.run_id,
            status=status,
            duration_seconds=duration,
            records_processed=records_processed,
            error_message=error_message,
            completed_at=completed_at,
            metadata=metadata or {},
        )
        result = self.telemetry.update_pipeline_status(update)

        # If the status update was skipped or could not match a row, insert the
        # terminal event too. This makes manual runs visible even when the start
        # event failed because of transient network or RLS behavior.
        if result.skipped or not result.ok:
            self.telemetry.log_pipeline_run(
                PipelineRunEvent(
                    run_id=self.run_id,
                    pipeline_name=self.pipeline_name,
                    status=status,
                    duration_seconds=duration,
                    records_processed=records_processed,
                    error_message=error_message,
                    started_at=self.started_at,
                    completed_at=completed_at,
                    metadata=metadata or {},
                )
            )

        self._log("pipeline_finished", status, records_processed, duration, error_message, metadata)

    def log_ingestion_batch(
        self,
        *,
        source_name: str,
        source_type: str,
        status: str,
        records_processed: int = 0,
        duration_seconds: Optional[float] = None,
        batch_id: Optional[str] = None,
        error_message: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        event = IngestionLogEvent(
            pipeline_run_id=self.run_id,
            pipeline_name=self.pipeline_name,
            source_name=source_name,
            source_type=source_type,
            status=status,
            records_processed=records_processed,
            duration_seconds=duration_seconds,
            batch_id=batch_id,
            error_message=error_message,
            metadata=metadata or {},
        )
        self.telemetry.log_ingestion(event)
        self._log(
            "ingestion_batch",
            status,
            records_processed=records_processed,
            duration_seconds=duration_seconds,
            error_message=error_message,
            metadata={
                "source_name": source_name,
                "source_type": source_type,
                "batch_id": batch_id,
                **(metadata or {}),
            },
        )

    def log_alert(
        self,
        *,
        message: str,
        alert_type: str = "pipeline",
        severity: str = "warning",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        event = OperationalAlertEvent(
            pipeline_run_id=self.run_id,
            pipeline_name=self.pipeline_name,
            message=message,
            alert_type=alert_type,
            severity=severity,
            metadata=metadata or {},
        )
        self.telemetry.log_alert(event)
        self._log("operational_alert", "open", error_message=message, metadata=metadata)

    @contextmanager
    def ingestion_timer(
        self,
        *,
        source_name: str,
        source_type: str,
        batch_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Iterator[None]:
        """Context manager useful for future Airflow task wrappers."""
        start = perf_counter()
        self.log_ingestion_batch(
            source_name=source_name,
            source_type=source_type,
            status="running",
            batch_id=batch_id,
            metadata=metadata,
        )
        try:
            yield
        except Exception as exc:
            self.log_ingestion_batch(
                source_name=source_name,
                source_type=source_type,
                status="failed",
                duration_seconds=perf_counter() - start,
                error_message=str(exc),
                batch_id=batch_id,
                metadata=metadata,
            )
            raise

    @property
    def duration_seconds(self) -> float:
        return perf_counter() - self._timer_started_at

    def _log(
        self,
        event_name: str,
        status: str,
        records_processed: int = 0,
        duration_seconds: Optional[float] = None,
        error_message: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        payload = {
            "event": event_name,
            "pipeline_name": self.pipeline_name,
            "pipeline_run_id": self.run_id,
            "status": status,
            "duration_seconds": duration_seconds,
            "records_processed": records_processed,
            "error_message": error_message,
            "metadata": metadata or {},
        }
        level = logging.ERROR if status == "failed" else logging.INFO
        self.logger.log(level, event_name, extra={"telemetry": payload})
