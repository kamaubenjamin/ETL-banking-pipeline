from src.contracts.api import WorkflowRunRequest, WorkflowCreateRequest


def test_workflow_run_request_defaults_to_async_api_execution():
    request = WorkflowRunRequest.from_dict({"workflow_id": "supplier_vs_market"})

    assert request.workflow_id == "supplier_vs_market"
    assert request.triggered_by == "flowsync-ui"
    assert request.async_execution is True


def test_workflow_create_request_serializes_public_definition():
    request = WorkflowCreateRequest.from_dict(
        {
            "workflow_id": "daily_prices",
            "workflow_name": "Daily Prices",
            "external_sources": [
                {
                    "name": "jumia",
                    "source_type": "playwright",
                    "url": "https://example.com",
                }
            ],
        }
    )

    definition = request.to_workflow_definition()

    assert definition["workflow_id"] == "daily_prices"
    assert definition["workflow_name"] == "Daily Prices"
    assert definition["external_sources"][0]["name"] == "jumia"
    assert "metadata" not in definition
