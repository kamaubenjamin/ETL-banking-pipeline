from datetime import datetime, timezone

from src.contracts.telemetry import IngestionLogEvent, PipelineRunEvent
from src.telemetry.telemetry_manager import TelemetryManager


def test_pipeline_run_payload_contains_structured_fields():
    event = PipelineRunEvent(
        run_id="run-123",
        pipeline_name="test_pipeline",
        status="success",
        duration_seconds=1.25,
        records_processed=42,
        started_at=datetime(2026, 5, 19, tzinfo=timezone.utc),
    )

    payload = event.to_payload()

    assert payload["id"] == "run-123"
    assert payload["pipeline_name"] == "test_pipeline"
    assert payload["duration"] == 1.25
    assert payload["duration_ms"] == 1250
    assert payload["records_processed"] == 42
    assert payload["idempotency_key"] == "pipeline_runs:run-123:success"


def test_ingestion_log_payload_uses_stable_batch_idempotency_key():
    event = IngestionLogEvent(
        pipeline_run_id="run-123",
        pipeline_name="test_pipeline",
        source_name="jumia",
        source_type="playwright",
        status="success",
        batch_id="batch-1",
        records_processed=10,
    )

    payload = event.to_payload()

    assert payload["pipeline_run_id"] == "run-123"
    assert payload["source_name"] == "jumia"
    assert payload["records_processed"] == 10
    assert payload["idempotency_key"] == "ingestion_logs:run-123:jumia:batch-1:success"


def test_telemetry_manager_skips_when_unconfigured():
    manager = TelemetryManager(client=None, enabled=True)
    result = manager.log_pipeline_run(
        PipelineRunEvent(
            run_id="run-123",
            pipeline_name="test_pipeline",
            status="running",
        )
    )

    assert result.ok is True
    assert result.skipped is True
