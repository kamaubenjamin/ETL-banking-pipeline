"""
Typed telemetry contracts shared by pipeline, integration, and service layers.

These dataclasses intentionally avoid any Supabase-specific SDK types. The
contracts can be reused later by Kafka producers, Airflow DAG callbacks, async
workers, or another telemetry sink without changing ETL business logic.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from uuid import uuid4


TelemetryMetadata = Dict[str, Any]


def utc_now() -> datetime:
    """Return a timezone-aware UTC timestamp for cross-service consistency."""
    return datetime.now(timezone.utc)


def iso_timestamp(value: Optional[datetime]) -> Optional[str]:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.isoformat()


def clean_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Remove null values so optional DB columns do not receive noisy blanks."""
    return {key: value for key, value in payload.items() if value is not None}


@dataclass(slots=True)
class TelemetryWriteResult:
    """Result returned by telemetry writes without leaking HTTP internals."""

    ok: bool
    table: str
    status_code: Optional[int] = None
    data: Any = None
    error: Optional[str] = None
    skipped: bool = False


@dataclass(slots=True)
class PipelineRunEvent:
    """A single pipeline run lifecycle event for FlowSync."""

    pipeline_name: str
    status: str
    run_id: str = field(default_factory=lambda: str(uuid4()))
    duration_seconds: Optional[float] = None
    records_processed: int = 0
    error_message: Optional[str] = None
    started_at: datetime = field(default_factory=utc_now)
    completed_at: Optional[datetime] = None
    triggered_by: str = "etl-backend"
    metadata: TelemetryMetadata = field(default_factory=dict)

    def idempotency_key(self) -> str:
        return f"pipeline_runs:{self.run_id}:{self.status}"

    def to_payload(self) -> Dict[str, Any]:
        duration_ms = int(self.duration_seconds * 1000) if self.duration_seconds is not None else None
        payload = asdict(self)
        payload.update(
            {
                "id": self.run_id,
                "pipeline_run_id": self.run_id,
                "duration": self.duration_seconds,
                "duration_ms": duration_ms,
                "started_at": iso_timestamp(self.started_at),
                "completed_at": iso_timestamp(self.completed_at),
                "timestamp": iso_timestamp(self.completed_at or self.started_at),
                "created_at": iso_timestamp(self.started_at),
                "updated_at": iso_timestamp(self.completed_at or utc_now()),
                "idempotency_key": self.idempotency_key(),
            }
        )
        return clean_payload(payload)


@dataclass(slots=True)
class IngestionLogEvent:
    """Batch/source-level ingestion telemetry."""

    pipeline_name: str
    source_name: str
    source_type: str
    status: str
    pipeline_run_id: Optional[str] = None
    ingestion_id: str = field(default_factory=lambda: str(uuid4()))
    batch_id: Optional[str] = None
    records_processed: int = 0
    duration_seconds: Optional[float] = None
    error_message: Optional[str] = None
    metadata: TelemetryMetadata = field(default_factory=dict)
    created_at: datetime = field(default_factory=utc_now)

    def idempotency_key(self) -> str:
        stable_batch = self.batch_id or self.ingestion_id
        return f"ingestion_logs:{self.pipeline_run_id}:{self.source_name}:{stable_batch}:{self.status}"

    def to_payload(self) -> Dict[str, Any]:
        duration_ms = int(self.duration_seconds * 1000) if self.duration_seconds is not None else None
        payload = asdict(self)
        payload.update(
            {
                "id": self.ingestion_id,
                "duration": self.duration_seconds,
                "duration_ms": duration_ms,
                "timestamp": iso_timestamp(self.created_at),
                "created_at": iso_timestamp(self.created_at),
                "idempotency_key": self.idempotency_key(),
            }
        )
        return clean_payload(payload)


@dataclass(slots=True)
class OperationalAlertEvent:
    """Operational alert sent to FlowSync dashboards."""

    message: str
    alert_type: str = "pipeline"
    severity: str = "warning"
    status: str = "open"
    pipeline_name: Optional[str] = None
    pipeline_run_id: Optional[str] = None
    alert_id: str = field(default_factory=lambda: str(uuid4()))
    error_message: Optional[str] = None
    metadata: TelemetryMetadata = field(default_factory=dict)
    created_at: datetime = field(default_factory=utc_now)

    def idempotency_key(self) -> str:
        return f"operational_alerts:{self.alert_id}:{self.status}"

    def to_payload(self) -> Dict[str, Any]:
        payload = asdict(self)
        payload.update(
            {
                "id": self.alert_id,
                "timestamp": iso_timestamp(self.created_at),
                "created_at": iso_timestamp(self.created_at),
                "idempotency_key": self.idempotency_key(),
            }
        )
        return clean_payload(payload)


@dataclass(slots=True)
class PipelineStatusUpdate:
    """Mutable status update for an existing pipeline run."""

    pipeline_run_id: str
    status: str
    duration_seconds: Optional[float] = None
    records_processed: Optional[int] = None
    error_message: Optional[str] = None
    completed_at: Optional[datetime] = None
    metadata: TelemetryMetadata = field(default_factory=dict)

    def to_payload(self) -> Dict[str, Any]:
        duration_ms = int(self.duration_seconds * 1000) if self.duration_seconds is not None else None
        payload = asdict(self)
        payload.update(
            {
                "duration": self.duration_seconds,
                "duration_ms": duration_ms,
                "completed_at": iso_timestamp(self.completed_at),
                "updated_at": iso_timestamp(utc_now()),
                "timestamp": iso_timestamp(self.completed_at or utc_now()),
            }
        )
        return clean_payload(payload)
