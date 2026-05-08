import pandas as pd
from src.storage.history_store import save_snapshot
from src.extract.extract import run_extraction
from src.transform.engine import TransformEngine
from src.transform.comparison_engine import (
    combine_datasets,
    match_products,
    build_comparison_table
)
from src.workflows import WorkflowConfig


def run_multi_source_pipeline(sources: dict | WorkflowConfig, config) -> pd.DataFrame:
    """
    Run full pipeline across multiple sources.

    Sources may be passed as a raw dict or a WorkflowConfig instance.
    """

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

            print(f"✅ {name}: {len(df_clean)} rows extracted")

        except Exception as e:
            print(f"❌ {name} failed: {e}")

    if not datasets:
        raise Exception("No valid datasets extracted")

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
    return matched, comparison