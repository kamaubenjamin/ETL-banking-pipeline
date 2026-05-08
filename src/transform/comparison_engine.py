from typing import Dict

import pandas as pd
from rapidfuzz import fuzz
from src.transform.intelligence_engine import (
    normalizer,
    matcher,
    scorer,
    duplicate_reducer,
    MatchResult,
    ProductCanonical,
)


# -----------------------------
# 🔗 COMBINE DATASETS
# -----------------------------
def combine_datasets(datasets: dict, source_type_map: Dict[str, str] = None) -> pd.DataFrame:
    combined = []

    for source, df in datasets.items():
        temp = df.copy()
        temp["source"] = source
        # Set source type if provided, otherwise infer from existing data or default to external
        if source_type_map and source in source_type_map:
            temp["_source_type"] = source_type_map[source]
        elif "_source_type" not in temp.columns:
            # If not already set (e.g., by internal connector), default to external
            temp["_source_type"] = "external"
        combined.append(temp)

    return pd.concat(combined, ignore_index=True)


# -----------------------------
# 🔍 ENHANCED MATCH PRODUCTS WITH CONFIDENCE
# -----------------------------
def match_products(
    df: pd.DataFrame,
    threshold: int = 70,
    source_thresholds: dict[str, int] | None = None,
) -> pd.DataFrame:
    """
    Enhanced product matching with confidence scoring and canonical product identification.
    """
    df = df.copy().reset_index(drop=True)

    if "product_name" not in df.columns:
        raise ValueError("DataFrame must contain a 'product_name' column.")

    if df.empty:
        return df

    # Initialize new columns
    df["match_id"] = -1
    df["confidence_score"] = 0.0
    df["match_type"] = "no_match"
    df["canonical_id"] = None
    df["canonical_name"] = None

    # Extract advanced features for all products
    product_features = []
    for name in df["product_name"].astype(str):
        features = normalizer.extract_features_advanced(name)
        product_features.append(features)

    df["features"] = product_features

    # Build similarity matrix for efficient matching
    product_names = df["product_name"].tolist()
    similarity_matrix = matcher.calculate_similarity_matrix(product_names)

    match_id = 0
    processed_indices = set()

    for i in range(len(df)):
        if i in processed_indices:
            continue

        # Start a new match group
        match_group = [i]
        processed_indices.add(i)
        df.loc[i, "match_id"] = match_id

        # Find similar products
        for j in range(i + 1, len(df)):
            if j in processed_indices:
                continue

            similarity_score = similarity_matrix[i, j]

            # Calculate confidence score
            context = {
                'same_source': df.loc[i, "source"] == df.loc[j, "source"],
                'recent_data': True,  # Assume recent for now
            }
            confidence = scorer.score_match_confidence(
                product_names[i],
                product_names[j],
                context
            )

            # Determine effective threshold
            source_a = df.loc[i, "source"]
            source_b = df.loc[j, "source"]
            effective_threshold = threshold
            if source_thresholds:
                effective_threshold = max(
                    threshold,
                    source_thresholds.get(source_a, threshold),
                    source_thresholds.get(source_b, threshold),
                )

            # Check if this is a valid match
            if confidence >= effective_threshold:
                match_group.append(j)
                processed_indices.add(j)
                df.loc[j, "match_id"] = match_id
                df.loc[j, "confidence_score"] = confidence
                df.loc[j, "match_type"] = scorer.classify_match_type(confidence)

        # Update confidence for the base product
        if len(match_group) > 1:
            # Use the highest confidence in the group
            group_confidences = [df.loc[idx, "confidence_score"] for idx in match_group]
            max_confidence = max(group_confidences) if group_confidences else 0.0
            df.loc[i, "confidence_score"] = max_confidence
            df.loc[i, "match_type"] = scorer.classify_match_type(max_confidence)

        match_id += 1

    # Apply canonical product identification
    df = _apply_canonical_products(df)

    return df


def _apply_canonical_products(df: pd.DataFrame) -> pd.DataFrame:
    """Apply canonical product identification to matched groups."""
    # Process unique product names for canonical identification
    unique_products = df["product_name"].drop_duplicates().tolist()
    canonicals = duplicate_reducer.process_products(unique_products)

    # Map products to their canonicals
    for idx, row in df.iterrows():
        product_name = row["product_name"]
        canonical = duplicate_reducer.find_canonical_match(product_name)

        if canonical:
            df.loc[idx, "canonical_id"] = canonical.canonical_id
            df.loc[idx, "canonical_name"] = canonical.canonical_name

    return df


# -----------------------------
# 📊 BUILD COMPARISON TABLE WITH CONFIDENCE
# -----------------------------
def build_comparison_table(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build comparison table with enhanced confidence scoring.
    """
    if df.empty:
        return pd.DataFrame()

    # Group by match_id and pivot by source
    comparison_data = []

    for match_id in df["match_id"].unique():
        if match_id == -1:
            continue

        group = df[df["match_id"] == match_id]

        # Get canonical information
        canonical_name = group["canonical_name"].dropna().iloc[0] if not group["canonical_name"].dropna().empty else group["product_name"].iloc[0]
        canonical_id = group["canonical_id"].dropna().iloc[0] if not group["canonical_id"].dropna().empty else f"match_{match_id}"

        # Calculate average confidence for the group
        avg_confidence = group["confidence_score"].mean()

        # Build price comparison by source
        prices_by_source = {}
        for _, row in group.iterrows():
            source = row["source"]
            # Try different price column names
            price = None
            for price_col in ["price", "current_price", "sale_price"]:
                if price_col in row and pd.notna(row[price_col]):
                    price = row[price_col]
                    break
            if price is not None:
                prices_by_source[source] = price

        if prices_by_source:
            # Find cheapest source
            cheapest_source = min(prices_by_source, key=prices_by_source.get)
            cheapest_price = prices_by_source[cheapest_source]

            comparison_data.append({
                "canonical_id": canonical_id,
                "product_name": canonical_name,
                "match_confidence": avg_confidence,
                "match_type": group["match_type"].iloc[0],
                **prices_by_source,
                "cheapest_source": cheapest_source,
                "cheapest_price": cheapest_price,
                "source_count": len(prices_by_source),
            })

    if not comparison_data:
        return pd.DataFrame()

    result_df = pd.DataFrame(comparison_data)

    # Sort by confidence score (highest first)
    result_df = result_df.sort_values("match_confidence", ascending=False)

    return result_df


# -----------------------------
# 🔍 ADVANCED PRODUCT SEARCH
# -----------------------------
def find_similar_products(
    target_product: str,
    df: pd.DataFrame,
    threshold: float = 70.0,
    limit: int = 10
) -> pd.DataFrame:
    """
    Find similar products to a target product with confidence scores.
    """
    if df.empty or not target_product:
        return pd.DataFrame()

    product_names = df["product_name"].tolist()
    matches = matcher.find_best_matches(target_product, product_names, threshold, limit)

    results = []
    for matched_name, similarity_score in matches:
        # Find the row for this product
        product_row = df[df["product_name"] == matched_name].iloc[0]

        # Calculate confidence
        confidence = scorer.score_match_confidence(target_product, matched_name)

        results.append({
            "target_product": target_product,
            "matched_product": matched_name,
            "similarity_score": similarity_score,
            "confidence_score": confidence,
            "match_type": scorer.classify_match_type(confidence),
            "source": product_row.get("source", "unknown"),
            "canonical_id": product_row.get("canonical_id"),
            "canonical_name": product_row.get("canonical_name"),
        })

    return pd.DataFrame(results)


# -----------------------------
# 📈 QUALITY METRICS
# -----------------------------
def calculate_matching_quality(df: pd.DataFrame) -> Dict:
    """
    Calculate quality metrics for the matching process.
    """
    if df.empty:
        return {}

    total_products = len(df)
    matched_products = len(df[df["match_id"] != -1])
    match_rate = matched_products / total_products if total_products > 0 else 0

    # Confidence distribution
    confidence_stats = df["confidence_score"].describe()

    # Match type distribution
    match_types = df["match_type"].value_counts().to_dict()

    # Canonical coverage
    canonical_coverage = len(df.dropna(subset=["canonical_id"])) / total_products if total_products > 0 else 0

    return {
        "total_products": total_products,
        "matched_products": matched_products,
        "match_rate": match_rate,
        "canonical_coverage": canonical_coverage,
        "confidence_mean": confidence_stats.get("mean", 0),
        "confidence_std": confidence_stats.get("std", 0),
        "match_types": match_types,
    }


# -----------------------------
# 🏢 SUPPLIER VS MARKET ANALYSIS
# -----------------------------
def compare_supplier_vs_market(df: pd.DataFrame) -> pd.DataFrame:
    """
    Specialized analysis for comparing internal supplier prices vs external market prices.
    """
    if df.empty:
        return pd.DataFrame()

    # Identify internal vs external sources
    internal_sources = []
    external_sources = []

    for source in df["source"].unique():
        if "_source_type" in df.columns and (df[df["source"] == source]["_source_type"] == "internal").any():
            internal_sources.append(source)
        else:
            external_sources.append(source)

    if not internal_sources or not external_sources:
        # Fallback to regular comparison if no clear internal/external distinction
        return build_comparison_table(df)

    analysis_data = []

    for match_id in df["match_id"].unique():
        if match_id == -1:
            continue

        group = df[df["match_id"] == match_id]

        # Get canonical information
        canonical_name = group["canonical_name"].dropna().iloc[0] if not group["canonical_name"].dropna().empty else group["product_name"].iloc[0]
        canonical_id = group["canonical_id"].dropna().iloc[0] if not group["canonical_id"].dropna().empty else f"match_{match_id}"

        # Separate internal and external prices
        internal_prices = {}
        external_prices = {}

        for _, row in group.iterrows():
            source = row["source"]
            price = None
            for price_col in ["price", "supplier_price", "unit_cost", "current_price", "sale_price"]:
                if price_col in row and pd.notna(row[price_col]):
                    price = row[price_col]
                    break

            if price is not None:
                if source in internal_sources:
                    internal_prices[source] = price
                elif source in external_sources:
                    external_prices[source] = price

        if internal_prices and external_prices:
            # Calculate supplier vs market metrics
            internal_avg = sum(internal_prices.values()) / len(internal_prices)
            external_min = min(external_prices.values())
            external_avg = sum(external_prices.values()) / len(external_prices)
            external_max = max(external_prices.values())

            # Calculate competitiveness metrics
            price_difference = internal_avg - external_min
            price_difference_pct = (price_difference / external_min) * 100 if external_min > 0 else 0

            # Determine if supplier is competitive
            is_competitive = internal_avg <= external_avg * 1.05  # Within 5% of market average
            undercut_opportunity = external_max - internal_avg > 0

            analysis_data.append({
                "canonical_id": canonical_id,
                "product_name": canonical_name,
                "supplier_avg_price": internal_avg,
                "market_min_price": external_min,
                "market_avg_price": external_avg,
                "market_max_price": external_max,
                "price_difference": price_difference,
                "price_difference_pct": price_difference_pct,
                "is_competitive": is_competitive,
                "undercut_opportunity": undercut_opportunity,
                "supplier_sources": list(internal_prices.keys()),
                "market_sources": list(external_prices.keys()),
                "match_confidence": group["confidence_score"].mean(),
            })

    return pd.DataFrame(analysis_data)


# -----------------------------
# 🎯 DETECT SUPPLIER UNDERCUT OPPORTUNITIES
# -----------------------------
def detect_supplier_undercut(df: pd.DataFrame, threshold: float = 2000) -> pd.DataFrame:
    """
    Detect opportunities where supplier prices could undercut market prices.
    """
    if df.empty:
        return pd.DataFrame()

    opportunities = []

    for _, row in df.iterrows():
        if row.get("undercut_opportunity", False):
            market_max = row["market_max_price"]
            supplier_price = row["supplier_avg_price"]
            potential_profit = market_max - supplier_price

            if potential_profit >= threshold:
                opportunities.append({
                    "product_name": row["product_name"],
                    "supplier_price": supplier_price,
                    "market_max_price": market_max,
                    "potential_profit": potential_profit,
                    "profit_margin_pct": (potential_profit / supplier_price) * 100,
                    "market_sources": row["market_sources"],
                    "supplier_sources": row["supplier_sources"],
                    "confidence": row["match_confidence"],
                })

    return pd.DataFrame(opportunities)
