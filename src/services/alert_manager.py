"""
Operational alert service for FlowSync.

This service centralizes alert persistence today and leaves a clean extension
point for notification fan-out later, such as Slack, email, PagerDuty, or Kafka
topics consumed by FlowSync workers.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from src.contracts.telemetry import OperationalAlertEvent, TelemetryWriteResult
from src.telemetry.telemetry_manager import TelemetryManager, telemetry_manager


class AlertManager:
    """Publish operational alerts to FlowSync telemetry tables."""

    def __init__(self, telemetry: Optional[TelemetryManager] = None):
        self.telemetry = telemetry or telemetry_manager

    def publish(
        self,
        message: str,
        *,
        alert_type: str = "pipeline",
        severity: str = "warning",
        status: str = "open",
        pipeline_name: Optional[str] = None,
        pipeline_run_id: Optional[str] = None,
        error_message: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> TelemetryWriteResult:
        event = OperationalAlertEvent(
            message=message,
            alert_type=alert_type,
            severity=severity,
            status=status,
            pipeline_name=pipeline_name,
            pipeline_run_id=pipeline_run_id,
            error_message=error_message,
            metadata=metadata or {},
        )
        return self.telemetry.log_alert(event)


alert_manager = AlertManager()
