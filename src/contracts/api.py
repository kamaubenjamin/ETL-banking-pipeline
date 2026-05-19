"""
Typed API contracts for FlowSync control-plane integration.

These dataclasses define the public boundary between FlowSync and the ETL
execution plane. The frontend should send and receive these shapes through HTTP
only; pipeline internals remain behind services.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from uuid import uuid4


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def public_dict(value: Any) -> Dict[str, Any]:
    return asdict(value)


@dataclass(slots=True)
class WorkflowRunRequest:
    workflow_id: str
    run_id: Optional[str] = None
    triggered_by: str = "flowsync-ui"
    async_execution: bool = True
    metadata: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "WorkflowRunRequest":
        return cls(
            workflow_id=payload["workflow_id"],
            run_id=payload.get("run_id"),
            triggered_by=payload.get("triggered_by", "flowsync-ui"),
            async_execution=payload.get("async_execution", payload.get("async", True)),
            metadata=payload.get("metadata") or {},
        )


@dataclass(slots=True)
class WorkflowRunResponse:
    run_id: str
    workflow_id: str
    status: str
    accepted: bool
    message: str
    submitted_at: str = field(default_factory=utc_now_iso)
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None


@dataclass(slots=True)
class WorkflowCreateRequest:
    workflow_id: str
    workflow_name: str
    description: str = ""
    internal_sources: List[Dict[str, Any]] = field(default_factory=list)
    external_sources: List[Dict[str, Any]] = field(default_factory=list)
    sources: List[Dict[str, Any]] = field(default_factory=list)
    steps: List[str] = field(default_factory=lambda: ["extract", "compare", "generate_alerts", "generate_reports"])
    transformation_rules: List[Dict[str, Any]] = field(default_factory=list)
    alert_rules: Dict[str, Any] | List[Dict[str, Any]] = field(default_factory=dict)
    global_match_threshold: int = 70
    enabled: bool = True
    metadata: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "WorkflowCreateRequest":
        return cls(
            workflow_id=payload["workflow_id"],
            workflow_name=payload.get("workflow_name") or payload["workflow_id"],
            description=payload.get("description", ""),
            internal_sources=payload.get("internal_sources") or [],
            external_sources=payload.get("external_sources") or [],
            sources=payload.get("sources") or [],
            steps=payload.get("steps") or ["extract", "compare", "generate_alerts", "generate_reports"],
            transformation_rules=payload.get("transformation_rules") or [],
            alert_rules=payload.get("alert_rules") or {},
            global_match_threshold=payload.get("global_match_threshold", 70),
            enabled=payload.get("enabled", True),
            metadata=payload.get("metadata") or {},
        )

    def to_workflow_definition(self) -> Dict[str, Any]:
        data = public_dict(self)
        data.pop("metadata", None)
        return data


@dataclass(slots=True)
class ConnectorTestRequest:
    source_type: str
    url: Optional[str] = None
    selector: Optional[str] = None
    mode: Optional[str] = "Auto Detect"
    keyword: Optional[str] = None
    sample_limit: int = 5
    metadata: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "ConnectorTestRequest":
        return cls(
            source_type=payload["source_type"],
            url=payload.get("url"),
            selector=payload.get("selector"),
            mode=payload.get("mode", "Auto Detect"),
            keyword=payload.get("keyword"),
            sample_limit=payload.get("sample_limit", 5),
            metadata=payload.get("metadata") or {},
        )


@dataclass(slots=True)
class SourceSyncRequest:
    workflow_id: Optional[str] = None
    source_name: Optional[str] = None
    source_type: str = "playwright"
    url: Optional[str] = None
    selector: Optional[str] = None
    mode: Optional[str] = "Auto Detect"
    keyword: Optional[str] = None
    run_id: str = field(default_factory=lambda: str(uuid4()))
    metadata: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "SourceSyncRequest":
        return cls(
            workflow_id=payload.get("workflow_id"),
            source_name=payload.get("source_name"),
            source_type=payload.get("source_type", "playwright"),
            url=payload.get("url"),
            selector=payload.get("selector"),
            mode=payload.get("mode", "Auto Detect"),
            keyword=payload.get("keyword"),
            run_id=payload.get("run_id") or str(uuid4()),
            metadata=payload.get("metadata") or {},
        )


@dataclass(slots=True)
class RunStatusRecord:
    run_id: str
    workflow_id: str
    status: str
    submitted_at: str
    updated_at: str
    triggered_by: str = "flowsync-ui"
    error: Optional[str] = None
    result: Optional[Dict[str, Any]] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
