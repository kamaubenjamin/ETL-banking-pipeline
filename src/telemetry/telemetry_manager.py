"""
FlowSync telemetry service layer.

The manager exposes stable ETL-facing methods while hiding Supabase table names,
HTTP behavior, schema fallback, and retry policy. Keeping this layer small makes
it easy to add Kafka publishing or Airflow callbacks later without changing
pipeline code.
"""

from __future__ import annotations

import os
from typing import Optional

from src.contracts.telemetry import (
    IngestionLogEvent,
    OperationalAlertEvent,
    PipelineRunEvent,
    PipelineStatusUpdate,
    TelemetryWriteResult,
)
from src.integrations.supabase_client import SupabaseClient


class TelemetryManager:
    """Coordinates writes to FlowSync operational telemetry tables."""

    def __init__(
        self,
        client: Optional[SupabaseClient] = None,
        *,
        enabled: Optional[bool] = None,
        raise_on_error: bool = False,
    ):
        self.client = client
        self.enabled = self._resolve_enabled(enabled)
        self.raise_on_error = raise_on_error
        self.pipeline_runs_table = os.getenv("FLOWSYNC_PIPELINE_RUNS_TABLE", "pipeline_runs")
        self.ingestion_logs_table = os.getenv("FLOWSYNC_INGESTION_LOGS_TABLE", "ingestion_logs")
        self.operational_alerts_table = os.getenv("FLOWSYNC_OPERATIONAL_ALERTS_TABLE", "operational_alerts")

    @classmethod
    def from_env(cls) -> "TelemetryManager":
        return cls(
            client=SupabaseClient.from_env(),
            enabled=None,
            raise_on_error=os.getenv("FLOWSYNC_TELEMETRY_RAISE_ON_ERROR", "false").lower() == "true",
        )

    def _resolve_enabled(self, enabled: Optional[bool]) -> bool:
        if enabled is not None:
            return enabled
        configured = os.getenv("FLOWSYNC_TELEMETRY_ENABLED")
        if configured is not None:
            return configured.lower() in {"1", "true", "yes", "on"}
        return self.client is not None

    @property
    def is_ready(self) -> bool:
        return self.enabled and self.client is not None and self.client.is_configured

    def validate_connection(self) -> TelemetryWriteResult:
        if not self.is_ready:
            return TelemetryWriteResult(
                ok=True,
                table=self.pipeline_runs_table,
                skipped=True,
                error="Supabase telemetry is not configured",
            )

        response = self.client.validate_connection(self.pipeline_runs_table)
        return self._result(self.pipeline_runs_table, response.ok, response.status_code, response.data, response.error)

    def log_pipeline_run(self, event: PipelineRunEvent) -> TelemetryWriteResult:
        return self._insert(self.pipeline_runs_table, event.to_payload(), upsert_on="idempotency_key")

    def log_ingestion(self, event: IngestionLogEvent) -> TelemetryWriteResult:
        return self._insert(self.ingestion_logs_table, event.to_payload(), upsert_on="idempotency_key")

    def log_alert(self, event: OperationalAlertEvent) -> TelemetryWriteResult:
        return self._insert(self.operational_alerts_table, event.to_payload(), upsert_on="idempotency_key")

    def update_pipeline_status(self, update: PipelineStatusUpdate) -> TelemetryWriteResult:
        if not self.is_ready:
            return self._skipped(self.pipeline_runs_table)

        payload = update.to_payload()
        payload.pop("pipeline_run_id", None)
        response = self.client.update(
            self.pipeline_runs_table,
            filters={"id": update.pipeline_run_id},
            payload=payload,
        )

        # Some schemas store run IDs as pipeline_run_id instead of id.
        if not response.ok and response.status_code in {400, 404}:
            response = self.client.update(
                self.pipeline_runs_table,
                filters={"pipeline_run_id": update.pipeline_run_id},
                payload=payload,
            )

        return self._result(self.pipeline_runs_table, response.ok, response.status_code, response.data, response.error)

    def _insert(self, table: str, payload: dict, *, upsert_on: Optional[str] = None) -> TelemetryWriteResult:
        if not self.is_ready:
            return self._skipped(table)

        response = self.client.insert(table, payload, upsert_on=upsert_on)
        return self._result(table, response.ok, response.status_code, response.data, response.error)

    def _skipped(self, table: str) -> TelemetryWriteResult:
        return TelemetryWriteResult(ok=True, table=table, skipped=True, error="Telemetry disabled or unconfigured")

    def _result(
        self,
        table: str,
        ok: bool,
        status_code: Optional[int],
        data: object,
        error: Optional[str],
    ) -> TelemetryWriteResult:
        result = TelemetryWriteResult(ok=ok, table=table, status_code=status_code, data=data, error=error)
        if not ok and self.raise_on_error:
            raise RuntimeError(f"Telemetry write failed for {table}: {error}")
        return result


telemetry_manager = TelemetryManager.from_env()
