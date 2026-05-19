# Changelog

## Unreleased

- Added FlowSync Supabase telemetry integration with reusable contracts, retry-aware REST client, and `.env`-driven credentials.
- Added structured pipeline, ingestion, and alert telemetry services for `pipeline_runs`, `ingestion_logs`, and `operational_alerts`.
- Instrumented the classic ETL orchestrator, multi-source pipeline, and workflow runner without making telemetry a hard runtime dependency.
- Added extension points and comments for future Kafka publishing, Airflow orchestration, async workers, and distributed ingestion.
- Added telemetry tests and `.env.example` for production configuration.
- Added API-first FlowSync control-plane boundary with typed contracts, async-safe workflow execution service, run status tracking, source health, connector test, source sync, alerts, telemetry run, and latest report endpoints.
- Added optional FastAPI/uvicorn API deployment dependencies.
