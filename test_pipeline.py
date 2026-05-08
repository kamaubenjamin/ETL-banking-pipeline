#!/usr/bin/env python3
"""
Test pipeline source types.
"""
from src.workflow_runner import WorkflowRunner
from src.pipeline.multi_source_pipeline import run_multi_source_pipeline
import src.config as config

def test_pipeline():
    """Test the pipeline source types."""
    runner = WorkflowRunner()
    wf = runner.get_workflow('supplier_vs_market')
    workflow_config = runner.workflow_to_config(wf)

    print("Testing pipeline...")
    pipeline_result = run_multi_source_pipeline(workflow_config, config)
    print(f"Pipeline result type: {type(pipeline_result)}")

    if isinstance(pipeline_result, tuple):
        matched, comparison = pipeline_result
        print(f"Matched data: {len(matched)} rows")
        if hasattr(matched, 'columns') and '_source_type' in matched.columns:
            print(f'Source types: {matched["_source_type"].value_counts().to_dict()}')
            print(f'Sources: {matched["source"].value_counts().to_dict()}')

            # Check matched products
            matched_products = matched[matched['match_id'] != -1]
            print(f"Products with matches: {len(matched_products)}")
            print(f"Unique match_ids: {matched_products['match_id'].nunique()}")

            # Check price columns
            price_cols = [col for col in matched.columns if 'price' in col.lower()]
            print(f"Price columns: {price_cols}")

            # Sample of matched data
            if len(matched_products) > 0:
                print("Sample matched products:")
                print(matched_products[['product_name', 'source', '_source_type', 'match_id', 'supplier_price', 'price']].head(10).to_string())

                # Check for external sources in matched data
                external_matched = matched_products[matched_products['_source_type'] == 'external']
                print(f"External matched products: {len(external_matched)}")
                if len(external_matched) > 0:
                    print("Sample external matched:")
                    print(external_matched[['product_name', 'source', 'match_id', 'price']].head(3).to_string())

                # Check match groups
                match_groups = matched_products.groupby('match_id')
                print(f"Match groups: {len(match_groups)}")
                for match_id, group in match_groups:
                    sources_in_group = group['_source_type'].unique()
                    if len(sources_in_group) > 1:
                        print(f"Match {match_id}: {dict(group['_source_type'].value_counts())}")
                        print(group[['product_name', 'source', '_source_type']].to_string())
                        break  # Just show first mixed group

            # Test supplier analysis
            from src.transform.comparison_engine import compare_supplier_vs_market
            analysis = compare_supplier_vs_market(matched)
            print(f'Supplier analysis result: {len(analysis)} rows')
            if len(analysis) > 0:
                print('Sample analysis:')
                print(analysis.head(2).to_string())
        else:
            print("No _source_type column found")

if __name__ == "__main__":
    test_pipeline()