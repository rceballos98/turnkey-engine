"""Core type definitions for the resilient fetch pipeline."""

from __future__ import annotations

from typing import Any, Literal
from pydantic import BaseModel, Field


class SourceResult(BaseModel):
    name: str
    status: Literal["ok", "degraded", "failed", "skipped"] = "ok"
    data: Any = None
    record_count: int = 0
    duration_ms: int = 0
    error: str | None = None
    retry_attempts: int = 0
    http_status: int | None = None


class SourceManifest(BaseModel):
    total_sources: int = 0
    succeeded: int = 0
    degraded: int = 0
    failed: int = 0
    skipped: int = 0
    failed_sources: list[str] = Field(default_factory=list)
    total_duration_ms: int = 0
    sources: list[SourceResult] = Field(default_factory=list)


class ResilientFetchResult(BaseModel):
    ok: bool = False
    data: Any = None
    http_status: int = 0
    retry_attempts: int = 0
    duration_ms: int = 0
    error: str | None = None


class FetchOptions(BaseModel):
    url: str
    method: Literal["GET", "POST"] = "GET"
    headers: dict[str, str] | None = None
    body: str | None = None
    timeout_ms: int = 15000
    max_retries: int = 2
    retry_delay_ms: int = 1000
    retry_on: list[int] = Field(default_factory=lambda: [429, 500, 502, 503])
    rate_limit_key: str | None = None
    label: str = "unknown"


class ParsedAddress(BaseModel):
    house_number: str = ""
    street: str = ""
    unit: str = ""
    borough: str = "MN"
    zip: str = ""
    city: str = "New York"
    state: str = "NY"


class AINarratives(BaseModel):
    executive_summary: str = "Detailed analysis not available for this section."
    key_findings: list[dict[str, str]] = Field(default_factory=list)
    violation_narrative: str = "Detailed analysis not available for this section."
    registration_narrative: str = "Detailed analysis not available for this section."
    tax_narrative: str = "Detailed analysis not available for this section."
    neighborhood_narrative: str = "Detailed analysis not available for this section."
    red_flags: list[dict[str, Any]] = Field(default_factory=list)
    positive_factors: list[dict[str, Any]] = Field(default_factory=list)
    comparable_analysis: list[str] = Field(default_factory=list)
    neighbor_profile_narrative: str = "Detailed analysis not available for this section."
    work_permit_observations: list[str] = Field(default_factory=list)
    comp_comments: list[dict[str, str]] = Field(default_factory=list)


class UnifiedComp(BaseModel):
    address: str = ""
    price: int = 0
    sqft: int | None = None
    price_per_sqft: int | None = None
    is_subject: bool = False
    comment: str | None = None


def build_manifest(sources: list[SourceResult]) -> SourceManifest:
    succeeded = sum(1 for s in sources if s.status == "ok")
    degraded = sum(1 for s in sources if s.status == "degraded")
    failed = sum(1 for s in sources if s.status == "failed")
    skipped = sum(1 for s in sources if s.status == "skipped")
    total_duration_ms = max((s.duration_ms for s in sources), default=0)

    return SourceManifest(
        total_sources=len(sources),
        succeeded=succeeded,
        degraded=degraded,
        failed=failed,
        skipped=skipped,
        failed_sources=[s.name for s in sources if s.status == "failed"],
        total_duration_ms=total_duration_ms,
        sources=sources,
    )
