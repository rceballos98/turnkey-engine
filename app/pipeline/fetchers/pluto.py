"""PLUTO lookup with 6 street name variants + BBL fallback."""

from __future__ import annotations

import logging
import re
import time

from app.pipeline.contracts.socrata import get_contract
from app.pipeline.fetch_engine import resilient_fetch
from app.pipeline.fetchers.socrata import socrata_url
from app.pipeline.types import FetchOptions, SourceResult

logger = logging.getLogger(__name__)


async def fetch_pluto(house_number: str, street: str, borough: str, zip_code: str | None = None) -> SourceResult:
    contract = get_contract("pluto")
    if not contract:
        return SourceResult(name="pluto", status="failed", error="No PLUTO contract")

    start = time.monotonic()
    base = street.upper()
    variants: set[str] = {base}

    # Expand abbreviations
    expanded = base
    for abbr, full in [("ST", "STREET"), ("AVE", "AVENUE"), ("BLVD", "BOULEVARD"), ("PL", "PLACE"), ("DR", "DRIVE")]:
        expanded = re.sub(rf"\b{abbr}\b", full, expanded)
    variants.add(expanded)

    # Contract abbreviations
    contracted = base
    for full, abbr in [("STREET", "ST"), ("AVENUE", "AVE"), ("BOULEVARD", "BLVD"), ("PLACE", "PL"), ("DRIVE", "DR")]:
        contracted = re.sub(rf"\b{full}\b", abbr, contracted)
    variants.add(contracted)

    # First word only (if long enough)
    first_word = base.split()[0] if base.split() else ""
    if len(first_word) > 3:
        variants.add(first_word)

    opts = FetchOptions(
        url="",
        timeout_ms=contract.timeout_ms,
        max_retries=contract.max_retries,
        retry_on=contract.retry_on,
        rate_limit_key=contract.rate_limit_key,
        label="pluto",
    )

    for variant in variants:
        url = socrata_url(
            contract.endpoint,
            f"address LIKE '{house_number} {variant}%' AND borough='{borough}'",
            limit=5,
        )
        logger.info("[PLUTO] query: %s", url)
        opts.url = url
        result = await resilient_fetch(opts)
        if result.ok and isinstance(result.data, list) and len(result.data) > 0:
            return SourceResult(
                name="pluto", status="ok", data=result.data[0], record_count=1,
                duration_ms=int((time.monotonic() - start) * 1000),
                retry_attempts=result.retry_attempts, http_status=result.http_status,
            )

    # Zip fallback
    if zip_code:
        url = socrata_url(
            contract.endpoint,
            f"address LIKE '{house_number}%' AND zipcode='{zip_code}'",
            limit=5,
        )
        logger.info("[PLUTO] zip fallback: %s", url)
        opts.url = url
        opts.label = "pluto-zip"
        result = await resilient_fetch(opts)
        if result.ok and isinstance(result.data, list) and len(result.data) > 0:
            return SourceResult(
                name="pluto", status="ok", data=result.data[0], record_count=1,
                duration_ms=int((time.monotonic() - start) * 1000),
                retry_attempts=result.retry_attempts, http_status=result.http_status,
            )

    logger.warning("[PLUTO] no results for any variant")
    return SourceResult(
        name="pluto", status="failed", data=None,
        duration_ms=int((time.monotonic() - start) * 1000),
        error="No PLUTO results for any street variant",
    )


async def fetch_pluto_by_bbl(bbl: str) -> SourceResult:
    contract = get_contract("pluto")
    if not contract:
        return SourceResult(name="pluto", status="failed", error="No PLUTO contract")

    url = socrata_url(contract.endpoint, f"bbl='{bbl}'", limit=1)
    result = await resilient_fetch(FetchOptions(
        url=url,
        timeout_ms=contract.timeout_ms,
        max_retries=contract.max_retries,
        retry_on=contract.retry_on,
        rate_limit_key=contract.rate_limit_key,
        label="pluto-bbl",
    ))

    if result.ok and isinstance(result.data, list) and len(result.data) > 0:
        return SourceResult(
            name="pluto", status="ok", data=result.data[0], record_count=1,
            duration_ms=result.duration_ms, retry_attempts=result.retry_attempts,
            http_status=result.http_status,
        )
    return SourceResult(
        name="pluto", status="failed", data=None,
        duration_ms=result.duration_ms,
        error=result.error or "No PLUTO results for BBL",
        retry_attempts=result.retry_attempts, http_status=result.http_status,
    )
