#!/usr/bin/env python3
"""
Test script for supplier vs market workflow execution.
"""
import json
from src.workflow_runner import WorkflowRunner

def test_supplier_vs_market_workflow():
    """Test the supplier vs market workflow execution."""
    print("Testing supplier vs market workflow execution...")

    # Create workflow runner instance
    runner = WorkflowRunner()

    # Execute the supplier_vs_market workflow
    result = runner.execute_workflow("supplier_vs_market")

    print(f"Workflow execution completed with status: {result['status']}")
    print(".2f")
    print(f"Alerts generated: {result['alerts_generated']}")

    # Check if supplier analysis was performed
    if 'supplier_analysis' in result.get('execution_log', {}):
        analysis = result['execution_log']['supplier_analysis']
        print(f"Supplier analysis completed: {len(analysis)} products analyzed")

    if 'undercut_opportunities' in result.get('execution_log', {}):
        opportunities = result['execution_log']['undercut_opportunities']
        print(f"Undercut opportunities found: {len(opportunities)}")

    return result

if __name__ == "__main__":
    test_supplier_vs_market_workflow()