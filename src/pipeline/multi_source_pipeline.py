import pandas as pd
from time import perf_counter
from src.storage.history_store import save_snapshot
from src.extract.extract import run_extraction
from src.transform.engine import TransformEngine
from src.transform.comparison_engine import (
    combine_datasets,
    match_products,
    build_comparison_table
)
from src.workflows import WorkflowConfig
from src.telemetry.pipeline_logger import PipelineLogger


def run_multi_source_pipeline(sources: dict | WorkflowConfig, config) -> pd.DataFrame:
    """
    Run full pipeline across multiple sources.

    Sources may be passed as a raw dict or a WorkflowConfig instance.
    """

    workflow_id = getattr(sources, "workflow_id", "ad_hoc_multi_source")
    workflow_name = getattr(sources, "name", workflow_id)
    pipeline_logger = PipelineLogger(f"multi_source_{workflow_name}".replace(" ", "_").lower())
    pipeline_logger.start(
        metadata={
            "workflow_id": workflow_id,
            "source_count": len(getattr(sources, "sources", sources)),
            # Airflow DAG operators can map this value to dag_id or task_id.
            "orchestrator": "run_multi_source_pipeline",
        }
    )

    threshold = 70
    source_thresholds = None
    if isinstance(sources, WorkflowConfig):
        workflow = sources
        threshold = workflow.global_match_threshold
        source_thresholds = {
            source.name: source.match_threshold
            for source in workflow.sources
        }
        sources = {
            source.name: {
                "type": source.source_type,
                "url": source.url,
                "selector": source.selector,
                "keyword": source.keyword,
                "mode": source.mode,
                "match_threshold": source.match_threshold,
            }
            for source in workflow.sources
        }

    datasets = {}

    for name, cfg in sources.items():
        print(f"\n🔍 Processing source: {name}")

        source_type = cfg.get("type", "playwright").strip().lower()
        source_start = perf_counter()
        pipeline_logger.log_ingestion_batch(
            source_name=name,
            source_type=source_type,
            status="running",
            metadata={
                "workflow_id": workflow_id,
                "url": cfg.get("url"),
            },
        )
        if cfg.get("url"):
            config.url = cfg.get("url")
        config.keyword = cfg.get("keyword")

        selector = cfg.get("selector")

        try:
            # -------------------------
            # EXTRACT
            # -------------------------
            mode = cfg.get("mode", "Auto Detect")
            if cfg.get("url"):
                config.url = cfg.get("url")
            config.keyword = cfg.get("keyword")

            df = run_extraction(
                source_type=source_type,
                config=config,
                mode=mode,
                selector=selector,
            )

            # -------------------------
            # TRANSFORM + PARSE
            # -------------------------
            engine = TransformEngine(df)
            df_clean = engine.apply([])
            datasets[name] = df_clean

            pipeline_logger.log_ingestion_batch(
                source_name=name,
                source_type=source_type,
                status="success",
                records_processed=len(df_clean),
                duration_seconds=perf_counter() - source_start,
                metadata={
                    "workflow_id": workflow_id,
                    # Kafka producers can publish this event as a durable
                    # source-batch message for distributed ingestion later.
                    "batch_boundary": "multi_source_extract_transform",
                },
            )

            print(f"✅ {name}: {len(df_clean)} rows extracted")

        except Exception as e:
            pipeline_logger.log_ingestion_batch(
                source_name=name,
                source_type=source_type,
                status="failed",
                duration_seconds=perf_counter() - source_start,
                error_message=str(e),
                metadata={"workflow_id": workflow_id},
            )
            print(f"❌ {name} failed: {e}")

    if not datasets:
        pipeline_logger.failure(
            "No valid datasets extracted",
            metadata={"workflow_id": workflow_id},
        )
        raise Exception("No valid datasets extracted")

    try:
        # -------------------------
        # COMBINE
        # -------------------------
        # Create source type map from workflow config if available
        source_type_map = None
        if isinstance(sources, WorkflowConfig):
            source_type_map = {src.name: src.source_type for src in sources.sources}
            sources = {source.name: {"type": source.source_type, "url": source.url, "selector": source.selector, "keyword": source.keyword, "mode": source.mode, "match_threshold": source.match_threshold} for source in sources.sources}

        combined = combine_datasets(datasets, source_type_map)

        # -------------------------
        # MATCH
        # -------------------------
        matched = match_products(
            combined,
            threshold=threshold,
            source_thresholds=source_thresholds,
        )

        # -------------------------
        # COMPARE
        # -------------------------
        comparison = build_comparison_table(matched)
         
        #save final comparison to history for future reference
        save_snapshot(comparison)
        pipeline_logger.success(
            records_processed=len(matched),
            metadata={
                "workflow_id": workflow_id,
                "comparison_rows": len(comparison) if hasattr(comparison, "__len__") else 0,
            },
        )
        return matched, comparison
    except Exception as e:
        pipeline_logger.failure(
            e,
            records_processed=sum(len(df) for df in datasets.values()),
            metadata={"workflow_id": workflow_id},
        )
        raise
