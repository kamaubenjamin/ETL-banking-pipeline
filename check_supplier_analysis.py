#!/usr/bin/env python3
"""
Check workflow execution history for supplier analysis results.
"""
from src.storage.workflow_history import workflow_history_store
import pandas as pd

def check_supplier_analysis():
    """Check the latest workflow execution for supplier analysis results."""
    history = workflow_history_store.get_history()

    if not history:
        print("No execution history found")
        return

    latest = history[-1]
    print(f"Latest execution: {latest['workflow_id']} - {latest['status']}")
    print(".2f")
    print(f"Alerts generated: {latest['alerts_generated']}")

    # Check execution log for supplier analysis
    exec_log = latest.get('execution_log', {})
    print(f"\nExecution log keys: {list(exec_log.keys())}")

    if 'matched' in exec_log:
        matched = exec_log['matched']
        print(f"Matched data: {len(matched)} rows")
        if hasattr(matched, 'columns'):
            print(f"Matched columns: {list(matched.columns)}")
            # Check for internal sources
            if '_source_type' in matched.columns:
                internal_count = (matched['_source_type'] == 'internal').sum()
                external_count = (matched['_source_type'] != 'internal').sum()
                print(f"Internal sources: {internal_count}, External sources: {external_count}")
    else:
        print("No matched data found")

    if 'comparison' in exec_log:
        comparison = exec_log['comparison']
        print(f"Comparison data: {len(comparison)} rows")
    else:
        print("No comparison data found")

    if 'supplier_analysis' in exec_log:
        analysis = exec_log['supplier_analysis']
        print(f"\nSupplier analysis: {len(analysis)} products analyzed")
        if isinstance(analysis, pd.DataFrame) and not analysis.empty:
            print("Sample analysis:")
            print(analysis.head(2).to_string())
        elif isinstance(analysis, list):
            print(f"Analysis is a list with {len(analysis)} items")
            if analysis:
                print(f"First item: {analysis[0]}")
    else:
        print("No supplier analysis found in execution log")

    if 'undercut_opportunities' in exec_log:
        opportunities = exec_log['undercut_opportunities']
        print(f"\nUndercut opportunities: {len(opportunities)} found")
        if isinstance(opportunities, pd.DataFrame) and not opportunities.empty:
            print("Sample opportunities:")
            print(opportunities.head(2).to_string())
        elif isinstance(opportunities, list):
            print(f"Opportunities is a list with {len(opportunities)} items")
            if opportunities:
                print(f"First item: {opportunities[0]}")
    else:
        print("No undercut opportunities found in execution log")

    # Check steps
    if 'steps' in exec_log:
        print(f"\nExecuted steps: {[s['name'] for s in exec_log['steps']]}")
        for step in exec_log['steps']:
            if 'compare_supplier_vs_market' in step['name'] or 'detect_undercut' in step['name']:
                print(f"Analysis step: {step}")

if __name__ == "__main__":
    check_supplier_analysis()