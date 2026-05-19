"""
Supabase PostgREST client for FlowSync telemetry.

The ETL backend writes directly to Supabase REST endpoints so the core pipeline
does not depend on a heavyweight SDK. This boundary is intentionally narrow:
future Kafka publishers can subscribe to the same contracts while this client
continues to serve dashboard persistence.
"""

from __future__ import annotations

import json
import os
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

try:
    import requests
except ImportError:  # pragma: no cover - exercised only in stripped runtimes
    requests = None


class SupabaseConfigurationError(RuntimeError):
    """Raised when Supabase credentials are required but missing."""


class SupabaseRequestError(RuntimeError):
    """Raised after retryable Supabase calls are exhausted."""


@dataclass(slots=True)
class SupabaseResponse:
    ok: bool
    status_code: int
    data: Any = None
    error: Optional[str] = None


def _load_dotenv_file(dotenv_path: str | os.PathLike[str] = ".env") -> None:
    """
    Load local .env values without overwriting real environment variables.

    python-dotenv is used when present; this lightweight parser keeps the ETL
    backend deployable in minimal worker images where optional dependencies have
    not been installed.
    """
    try:
        from dotenv import load_dotenv  # type: ignore

        load_dotenv(dotenv_path=dotenv_path, override=False)
        return
    except Exception:
        pass

    path = Path(dotenv_path)
    if not path.exists():
        return

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


class SupabaseClient:
    """Small, retry-aware Supabase REST client."""

    MISSING_COLUMN_RE = re.compile(r"Could not find the '([^']+)' column")

    def __init__(
        self,
        url: str,
        key: str,
        schema: str = "public",
        timeout_seconds: float = 15,
        max_retries: int = 3,
        retry_backoff_seconds: float = 0.5,
    ):
        if not url or not key:
            raise SupabaseConfigurationError("Supabase URL and API key are required")
        if requests is None:
            raise SupabaseConfigurationError("The requests package is required for Supabase telemetry")

        self.url = url.rstrip("/")
        self.key = key
        self.schema = schema
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self.retry_backoff_seconds = retry_backoff_seconds
        self.session = requests.Session()

    @classmethod
    def from_env(cls, dotenv_path: str | os.PathLike[str] = ".env") -> Optional["SupabaseClient"]:
        _load_dotenv_file(dotenv_path)

        url = os.getenv("FLOWSYNC_SUPABASE_URL") or os.getenv("SUPABASE_URL")
        key = (
            os.getenv("FLOWSYNC_SUPABASE_SERVICE_ROLE_KEY")
            or os.getenv("SUPABASE_SERVICE_ROLE_KEY")
            or os.getenv("FLOWSYNC_SUPABASE_KEY")
            or os.getenv("SUPABASE_KEY")
            or os.getenv("SUPABASE_ANON_KEY")
        )

        if not url or not key:
            return None
        if requests is None:
            return None

        return cls(
            url=url,
            key=key,
            schema=os.getenv("SUPABASE_SCHEMA", "public"),
            timeout_seconds=float(os.getenv("SUPABASE_TIMEOUT_SECONDS", "15")),
            max_retries=int(os.getenv("SUPABASE_MAX_RETRIES", "3")),
            retry_backoff_seconds=float(os.getenv("SUPABASE_RETRY_BACKOFF_SECONDS", "0.5")),
        )

    @property
    def is_configured(self) -> bool:
        return bool(self.url and self.key)

    def _headers(self, prefer: str = "return=representation") -> Dict[str, str]:
        return {
            "apikey": self.key,
            "Authorization": f"Bearer {self.key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Prefer": prefer,
            "Accept-Profile": self.schema,
            "Content-Profile": self.schema,
        }

    def validate_connection(self, table: str = "pipeline_runs") -> SupabaseResponse:
        """Validate credentials and RLS with a lightweight read."""
        return self.request(
            method="GET",
            table=table,
            params={"select": "*", "limit": "1"},
            retry=False,
        )

    def insert(
        self,
        table: str,
        payload: Dict[str, Any],
        *,
        upsert_on: Optional[str] = None,
    ) -> SupabaseResponse:
        """
        Insert telemetry with retry and schema fallback.

        Passing an idempotency key prepares the architecture for exactly-once
        semantics once a unique index exists in Supabase. Until that constraint
        is added, retries are at-least-once and bounded to transient failures.
        """
        payload = dict(payload)
        prefer = "return=representation"
        params = None
        if upsert_on:
            prefer = "resolution=merge-duplicates,return=representation"
            params = {"on_conflict": upsert_on}

        missing_columns_seen: set[str] = set()

        while True:
            response = self.request(
                method="POST",
                table=table,
                payload=payload,
                params=params,
                prefer=prefer,
                retry=True,
            )

            if response.ok:
                return response

            if upsert_on and response.error and "unique or exclusion constraint" in response.error.lower():
                upsert_on = None
                prefer = "return=representation"
                params = None
                continue

            missing_column = self._extract_missing_column(response.error)
            if missing_column and missing_column in payload and missing_column not in missing_columns_seen:
                missing_columns_seen.add(missing_column)
                payload.pop(missing_column, None)
                if upsert_on == missing_column:
                    upsert_on = None
                    prefer = "return=representation"
                    params = None
                continue

            return response

    def update(
        self,
        table: str,
        filters: Dict[str, Any],
        payload: Dict[str, Any],
    ) -> SupabaseResponse:
        params = {key: f"eq.{value}" for key, value in filters.items()}
        return self.request(
            method="PATCH",
            table=table,
            payload=payload,
            params=params,
            prefer="return=representation",
            retry=True,
        )

    def request(
        self,
        method: str,
        table: str,
        *,
        payload: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
        prefer: str = "return=representation",
        retry: bool = True,
    ) -> SupabaseResponse:
        endpoint = f"{self.url}/rest/v1/{table}"
        attempts = self.max_retries if retry else 1
        last_error: Optional[str] = None
        status_code = 0

        for attempt in range(attempts):
            try:
                response = self.session.request(
                    method=method,
                    url=endpoint,
                    headers=self._headers(prefer=prefer),
                    json=payload,
                    params=params,
                    timeout=self.timeout_seconds,
                )
                status_code = response.status_code
                data = self._decode_response(response)

                if 200 <= response.status_code < 300:
                    return SupabaseResponse(ok=True, status_code=response.status_code, data=data)

                last_error = self._extract_error(data)
                if response.status_code < 500 and response.status_code != 429:
                    return SupabaseResponse(False, response.status_code, data=data, error=last_error)

            except requests.RequestException as exc:
                last_error = str(exc)

            if attempt < attempts - 1:
                time.sleep(self.retry_backoff_seconds * (2**attempt))

        return SupabaseResponse(False, status_code, error=last_error)

    def _decode_response(self, response: requests.Response) -> Any:
        if not response.text:
            return None
        try:
            return response.json()
        except json.JSONDecodeError:
            return response.text

    def _extract_error(self, data: Any) -> str:
        if isinstance(data, dict):
            return str(data.get("message") or data.get("error") or data)
        return str(data)

    def _extract_missing_column(self, error: Optional[str]) -> Optional[str]:
        if not error:
            return None
        match = self.MISSING_COLUMN_RE.search(error)
        if match:
            return match.group(1)
        return None
