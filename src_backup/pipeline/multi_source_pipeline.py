import pandas as pd
from src.storage.history_store import save_snapshot
from src.extract.extract import run_extraction
from src.transform.engine import TransformEngine
from src.transform.comparison_engine import (
    combine_datasets,
    match_products,
    build_comparison_table
)


def run_multi_source_pipeline(sources: dict, config) -> pd.DataFrame:
    """
    Run full pipeline across multiple sources

    sources = {
        "jumia": {"url": "...", "selector": "..."},
        "kilimall": {"url": "...", "selector": "..."}
    }
    """

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
            df = run_extraction(
                source_type=source_type,
                config=config,
                selector=selector
            )

            # -------------------------
            # TRANSFORM + PARSE
            # -------------------------
            engine = TransformEngine(df)
            df_clean = engine.apply([])
            df_clean = df_clean[
                df_clean["product_name"].str.contains(r"\b(tv|television|qled|oled)\b", case=False, na=False)
            ]

            datasets[name] = df_clean

            print(f"✅ {name}: {len(df_clean)} rows extracted")

        except Exception as e:
            print(f"❌ {name} failed: {e}")

    if not datasets:
        raise Exception("No valid datasets extracted")

    # -------------------------
    # COMBINE
    # -------------------------
    combined = combine_datasets(datasets)

    # -------------------------
    # MATCH
    # -------------------------
    matched = match_products(combined)

    # -------------------------
    # COMPARE
    # -------------------------
    comparison = build_comparison_table(matched)
     
    #save final comparison to history for future reference
    save_snapshot(comparison)
    return comparison