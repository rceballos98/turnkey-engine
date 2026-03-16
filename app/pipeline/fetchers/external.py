"""FEMA flood, NYS environmental, NYC zoning API, 311, address parsing."""

from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from urllib.parse import urlencode

from app.config import settings
from app.pipeline.constants import BOROUGH_MAP
from app.pipeline.contracts.non_socrata import get_non_socrata_contract
from app.pipeline.contracts.socrata import get_contract
from app.pipeline.fetch_engine import resilient_fetch
from app.pipeline.types import FetchOptions, ParsedAddress, SourceResult

logger = logging.getLogger(__name__)


async def fetch_fema_flood_zone(lat: float, lon: float) -> SourceResult:
    contract = get_non_socrata_contract("floodZone")
    if not contract:
        return SourceResult(name="floodZone", status="failed", error="No contract")

    url = (f"{contract.endpoint}?geometry={lon},{lat}&geometryType=esriGeometryPoint"
           f"&inSR=4326&spatialRel=esriSpatialRelIntersects"
           f"&outFields=FLD_ZONE,ZONE_SUBTY,DFIRM_ID&returnGeometry=false&f=json")

    result = await resilient_fetch(FetchOptions(
        url=url, timeout_ms=contract.timeout_ms, max_retries=contract.max_retries,
        retry_on=contract.retry_on, label="floodZone",
    ))

    if not result.ok or not isinstance(result.data, dict) or not result.data.get("features"):
        return SourceResult(
            name="floodZone", status="ok" if result.ok else "failed", data=None,
            duration_ms=result.duration_ms, error=result.error, retry_attempts=result.retry_attempts,
        )

    attrs = result.data["features"][0].get("attributes", {})
    zone = attrs.get("FLD_ZONE", "Unknown")
    labels = {
        "X": "Minimal Flood Hazard",
        "A": "High Risk \u2014 100-Year Flood",
        "AE": "High Risk \u2014 Base Flood Elevation Determined",
        "AO": "High Risk \u2014 Shallow Flooding",
        "VE": "High Risk \u2014 Coastal Flood with Velocity",
        "D": "Undetermined Risk",
    }

    return SourceResult(
        name="floodZone", status="ok",
        data={"zone": zone, "zoneLabel": labels.get(zone, f"Flood Zone {zone}"),
              "panelNumber": attrs.get("DFIRM_ID")},
        record_count=1, duration_ms=result.duration_ms, retry_attempts=result.retry_attempts,
    )


async def fetch_environmental_sites(lat: float, lon: float, radius_meters: int = 500) -> SourceResult:
    contract = get_contract("envSites")
    if not contract:
        return SourceResult(name="envSites", status="failed", error="No contract")

    params = urlencode({
        "$where": f"within_circle(georeference,{lat},{lon},{radius_meters})",
        "$limit": "20",
    })
    url = f"{contract.endpoint}?{params}"

    result = await resilient_fetch(FetchOptions(
        url=url, timeout_ms=contract.timeout_ms, max_retries=contract.max_retries,
        retry_on=contract.retry_on, rate_limit_key=contract.rate_limit_key, label="envSites",
    ))

    data = result.data if result.ok and isinstance(result.data, list) else []
    return SourceResult(
        name="envSites", status="ok" if result.ok else "failed", data=data,
        record_count=len(data), duration_ms=result.duration_ms,
        error=result.error, retry_attempts=result.retry_attempts,
    )


async def fetch_zoning_districts(bbl: str) -> SourceResult:
    contract = get_non_socrata_contract("zoningDistricts")
    if not contract:
        return SourceResult(name="zoningDistricts", status="failed", error="No contract")

    start = time.monotonic()
    results = await asyncio.gather(
        resilient_fetch(FetchOptions(
            url=f"{contract.endpoint}/{bbl}/zoning-districts",
            timeout_ms=contract.timeout_ms, max_retries=contract.max_retries,
            retry_on=contract.retry_on, label="zoningDistricts",
        )),
        resilient_fetch(FetchOptions(
            url=f"{contract.endpoint}/{bbl}/zoning-districts/classes",
            timeout_ms=contract.timeout_ms, max_retries=contract.max_retries,
            retry_on=contract.retry_on, label="zoningClasses",
        )),
        return_exceptions=True,
    )

    districts = []
    classes = []
    if isinstance(results[0], Exception) is False and results[0].ok and isinstance(results[0].data, dict):
        districts = results[0].data.get("zoningDistricts", [])
    if isinstance(results[1], Exception) is False and results[1].ok and isinstance(results[1].data, dict):
        classes = results[1].data.get("zoningDistrictClasses", [])

    return SourceResult(
        name="zoningDistricts", status="ok",
        data={"zoningDistricts": districts, "zoningDistrictClasses": classes},
        record_count=len(districts), duration_ms=int((time.monotonic() - start) * 1000),
    )


async def fetch_311_requests(bbl: str, lat: float | None = None, lon: float | None = None) -> SourceResult:
    contract = get_contract("sr311")
    if not contract:
        return SourceResult(name="sr311", status="failed", error="No contract")

    start = time.monotonic()
    params = urlencode({"$where": f"bbl='{bbl}'", "$order": "created_date DESC", "$limit": "200"})
    bbl_url = f"{contract.endpoint}?{params}"

    bbl_result = await resilient_fetch(FetchOptions(
        url=bbl_url, timeout_ms=contract.timeout_ms, max_retries=contract.max_retries,
        retry_on=contract.retry_on, rate_limit_key=contract.rate_limit_key, label="sr311-bbl",
    ))

    if bbl_result.ok and isinstance(bbl_result.data, list) and bbl_result.data:
        return SourceResult(
            name="sr311", status="ok", data=bbl_result.data,
            record_count=len(bbl_result.data),
            duration_ms=int((time.monotonic() - start) * 1000),
            retry_attempts=bbl_result.retry_attempts,
        )

    # Geo fallback
    if lat and lon:
        geo_params = urlencode({
            "$where": f"within_circle(location,{lat},{lon},100)",
            "$order": "created_date DESC", "$limit": "200",
        })
        geo_url = f"{contract.endpoint}?{geo_params}"
        geo_result = await resilient_fetch(FetchOptions(
            url=geo_url, timeout_ms=contract.timeout_ms, max_retries=contract.max_retries,
            retry_on=contract.retry_on, rate_limit_key=contract.rate_limit_key, label="sr311-geo",
        ))
        records = geo_result.data if geo_result.ok and isinstance(geo_result.data, list) else []
        return SourceResult(
            name="sr311", status="ok", data=records,
            record_count=len(records),
            duration_ms=int((time.monotonic() - start) * 1000),
            retry_attempts=geo_result.retry_attempts,
        )

    return SourceResult(name="sr311", status="ok", data=[],
                        duration_ms=int((time.monotonic() - start) * 1000))


async def parse_address_with_ai(raw: str) -> ParsedAddress:
    if not settings.anthropic_api_key:
        return parse_address_regex(raw)

    contract = get_non_socrata_contract("claudeHaiku")
    if not contract:
        return parse_address_regex(raw)

    try:
        result = await resilient_fetch(FetchOptions(
            url=contract.endpoint, method="POST",
            headers={
                "Content-Type": "application/json",
                "x-api-key": settings.anthropic_api_key,
                "anthropic-version": "2023-06-01",
            },
            body=json.dumps({
                "model": "claude-haiku-4-5-20251001",
                "max_tokens": 200,
                "messages": [{"role": "user", "content": f'''Parse this NYC property address into structured JSON. The input may be messy, abbreviated, or informal.

Input: "{raw}"

Return ONLY valid JSON with these exact fields:
{{
  "houseNumber": "street number only",
  "street": "street name without house number, unit, city, or zip",
  "unit": "apartment/unit/floor number if any, just the number/letter. Empty string if none.",
  "borough": "NYC borough code: MN, BK, BX, QN, or SI. Default MN if unclear.",
  "city": "City name",
  "state": "NY",
  "zip": "5-digit zip code if provided, empty string if not"
}}'''}],
            }),
            timeout_ms=contract.timeout_ms, max_retries=contract.max_retries,
            retry_on=contract.retry_on, label="claudeHaiku-address",
        ))

        if not result.ok:
            logger.error("[ADDRESS-AI] Haiku error: %s", result.error)
            return parse_address_regex(raw)

        text = ""
        if isinstance(result.data, dict):
            content = result.data.get("content", [])
            if content and isinstance(content[0], dict):
                text = content[0].get("text", "")

        cleaned = re.sub(r"^```(?:json)?\s*\n?", "", text, flags=re.M)
        cleaned = re.sub(r"\n?```\s*$", "", cleaned, flags=re.M).strip()
        parsed = json.loads(cleaned)

        addr = ParsedAddress(
            house_number=str(parsed.get("houseNumber", "")).strip(),
            street=str(parsed.get("street", "")).strip(),
            unit=str(parsed.get("unit", "")).strip(),
            borough=BOROUGH_MAP.get(str(parsed.get("borough", "MN")).lower(),
                                    str(parsed.get("borough", "MN")).upper()),
            city=str(parsed.get("city", "New York")).strip(),
            state=str(parsed.get("state", "NY")).strip(),
            zip=str(parsed.get("zip", "")).strip(),
        )

        if not addr.house_number or not addr.street:
            logger.warning("[ADDRESS-AI] Incomplete parse, falling back to regex")
            return parse_address_regex(raw)

        logger.info("[ADDRESS-AI] Parsed: %s", addr)
        return addr
    except Exception as e:
        logger.error("[ADDRESS-AI] Failed, falling back to regex: %s", e)
        return parse_address_regex(raw)


def parse_address_regex(raw: str) -> ParsedAddress:
    cleaned = raw.strip()

    # Extract unit
    unit_match = (re.search(r"[,\s]+(\d+(?:st|nd|rd|th))\s*(?:floor|fl)\b", cleaned, re.I) or
                  re.search(r"[,\s]+(?:floor|fl)\s*(\d+[A-Za-z]?)\b", cleaned, re.I) or
                  re.search(r"[,\s]+(?:unit|apt|#|suite)\s*([^\s,]+)", cleaned, re.I))
    unit = re.sub(r"\s*(floor|fl)$", "", unit_match.group(1), flags=re.I).strip() if unit_match else ""
    without_unit = cleaned.replace(unit_match.group(0), "") if unit_match else cleaned

    # Extract zip
    zip_match = re.search(r"\b(\d{5})(?:\s|,|$)", without_unit)
    without_zip = without_unit.replace(zip_match.group(0), " ").strip() if zip_match else without_unit

    parts = [p.strip() for p in without_zip.split(",")]
    street_part = parts[0] if parts else ""
    house_match = re.match(r"^(\d+[-\d]*)\s+(.+)$", street_part)
    house_number = house_match.group(1) if house_match else ""
    street = house_match.group(2) if house_match else street_part
    street = re.sub(r"\s+(new york|manhattan|brooklyn|bronx|queens|staten island|ny|nyc)\s*$", "", street, flags=re.I).strip()

    borough = "MN"
    city = "New York"
    state = "NY"
    zip_code = zip_match.group(1) if zip_match else ""

    for part in parts[1:]:
        lower = part.lower().strip()
        if lower in BOROUGH_MAP:
            borough = BOROUGH_MAP[lower]
            city = part.strip()
            continue
        state_zip = re.match(r"([A-Z]{2})\s*(\d{5})", part.strip(), re.I)
        if state_zip:
            state = state_zip.group(1).upper()
            if not zip_code:
                zip_code = state_zip.group(2)
            continue
        if re.match(r"^\d{5}", part.strip()):
            if not zip_code:
                zip_code = part.strip()[:5]
            continue
        if "new york" in lower or "manhattan" in lower or "nyc" in lower:
            borough = "MN"
            city = "New York"
        elif "brooklyn" in lower:
            borough = "BK"
            city = "Brooklyn"
        elif "bronx" in lower:
            borough = "BX"
            city = "Bronx"
        elif "queens" in lower:
            borough = "QN"
            city = "Queens"
        elif "staten island" in lower:
            borough = "SI"
            city = "Staten Island"
        else:
            city = part.strip()

    return ParsedAddress(
        house_number=house_number, street=street, unit=unit,
        borough=borough, city=city, state=state, zip=zip_code,
    )
