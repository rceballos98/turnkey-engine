"""Firecrawl listing search, comps extraction, DOF enrichment, same-building."""

from __future__ import annotations

import json
import logging
import re
import time

from app.config import settings
from app.pipeline.contracts.non_socrata import get_non_socrata_contract
from app.pipeline.fetch_engine import resilient_fetch
from app.pipeline.types import FetchOptions, SourceResult

logger = logging.getLogger(__name__)


async def _firecrawl_search(query: str) -> list:
    contract = get_non_socrata_contract("firecrawlSearch")
    if not contract:
        return []
    result = await resilient_fetch(FetchOptions(
        url=contract.endpoint, method="POST",
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {settings.firecrawl_api_key}"},
        body=json.dumps({"query": query, "limit": 5}),
        timeout_ms=contract.timeout_ms, max_retries=contract.max_retries,
        retry_on=contract.retry_on, rate_limit_key=contract.rate_limit_key,
        label="firecrawlSearch",
    ))
    if result.ok and isinstance(result.data, dict):
        return result.data.get("data", [])
    return []


async def _firecrawl_scrape(url: str, formats: list[str], extract: dict | None = None) -> dict | None:
    contract = get_non_socrata_contract("firecrawlScrape")
    if not contract:
        return None
    body: dict = {"url": url, "formats": formats}
    if extract:
        body["extract"] = extract
    result = await resilient_fetch(FetchOptions(
        url=contract.endpoint, method="POST",
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {settings.firecrawl_api_key}"},
        body=json.dumps(body),
        timeout_ms=contract.timeout_ms, max_retries=contract.max_retries,
        retry_on=contract.retry_on, rate_limit_key=contract.rate_limit_key,
        label="firecrawlScrape",
    ))
    if result.ok and isinstance(result.data, dict):
        return result.data.get("data")
    return None


def _parse_listing(l: dict) -> dict:
    return {
        "address": l.get("address", "Unknown"),
        "price": l["price"] if isinstance(l.get("price"), (int, float)) else 0,
        "beds": l["beds"] if isinstance(l.get("beds"), (int, float)) else 0,
        "baths": l["baths"] if isinstance(l.get("baths"), (int, float)) else 0,
        "sqft": l["sqft"] if isinstance(l.get("sqft"), (int, float)) and l["sqft"] > 0 else None,
        "pricePerSqft": l["pricePerSqft"] if isinstance(l.get("pricePerSqft"), (int, float)) and l["pricePerSqft"] > 0 else None,
        "commonCharges": l["commonCharges"] if isinstance(l.get("commonCharges"), (int, float)) and l["commonCharges"] > 0 else None,
        "propertyTaxes": l["propertyTaxes"] if isinstance(l.get("propertyTaxes"), (int, float)) and l["propertyTaxes"] > 0 else None,
        "daysOnMarket": l["daysOnMarket"] if isinstance(l.get("daysOnMarket"), (int, float)) else None,
        "buildingType": l.get("buildingType", "Unknown"),
        "amenities": [a for a in l.get("amenities", []) if isinstance(a, str)],
    }


def _validate_listings(listings: list[dict]) -> list[dict]:
    fake_streets = re.compile(
        r"\b(main|elm|oak|pine|maple|cedar|birch|walnut|cherry|spruce|ash|fir|maplewood|oakwood|willow|example|test)\b", re.I
    )
    validated = [l for l in listings if l.get("price", 0) >= 10000 and not fake_streets.search(l.get("address", ""))
                 and l.get("address", "Unknown") != "Unknown" and len(l.get("address", "")) >= 5]
    if len(listings) > 3 and len(validated) < len(listings) * 0.5:
        logger.warning("[FIRECRAWL] comps: %d/%d listings look hallucinated — rejecting batch",
                       len(listings) - len(validated), len(listings))
        return []
    return validated


LISTING_EXTRACT_SCHEMA = {
    "type": "object",
    "properties": {
        "price": {"type": "number"}, "beds": {"type": "number"}, "baths": {"type": "number"},
        "sqft": {"type": "number"}, "yearBuilt": {"type": "number"},
        "commonCharges": {"type": "number"}, "propertyTaxes": {"type": "number"},
        "daysOnMarket": {"type": "number"}, "buildingType": {"type": "string"},
        "features": {"type": "array", "items": {"type": "string"}},
        "description": {"type": "string"}, "totalRooms": {"type": "number"},
        "listingBrokerage": {"type": "string"},
        "listingAgents": {"type": "array", "items": {"type": "string"}},
    },
}

COMPS_EXTRACT_SCHEMA = {
    "type": "object",
    "properties": {
        "listings": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "address": {"type": "string"}, "price": {"type": "number"},
                    "beds": {"type": "number"}, "baths": {"type": "number"},
                    "sqft": {"type": "number"}, "pricePerSqft": {"type": "number"},
                    "saleDate": {"type": "string"}, "commonCharges": {"type": "number"},
                    "propertyTaxes": {"type": "number"}, "daysOnMarket": {"type": "number"},
                    "buildingType": {"type": "string"}, "status": {"type": "string"},
                    "amenities": {"type": "array", "items": {"type": "string"}},
                },
            },
        },
    },
}

COMPS_EXTRACT_PROMPT = "Extract up to 15 recently closed/sold residential property listings from this StreetEasy page. For each listing, get the address, sale price (the final closed price, not asking price), bedrooms, bathrooms, square footage, price per sqft, closing/sale date, monthly common charges, monthly property taxes, days on market, building type (Condo, Co-op, etc.), status, and amenities. Focus on CLOSED/SOLD listings."


async def fetch_listing_data(
    house_number: str, street: str, unit: str, city: str, state: str, address: str,
) -> SourceResult:
    start = time.monotonic()
    unit_part = f" Unit {unit}" if unit else ""
    search_query = f"{house_number} {street}{unit_part} {city or ''} {state or 'NY'} for sale"
    logger.info("[FIRECRAWL] Searching: %s", search_query)

    search_results = await _firecrawl_search(search_query)
    if not search_results:
        search_results = await _firecrawl_search(f"{address} for sale NYC")
    if not search_results:
        return SourceResult(name="listing", status="ok", data={}, duration_ms=int((time.monotonic() - start) * 1000))

    def is_unit_page(url: str) -> bool:
        if "streeteasy.com/building/" in url and re.search(r"/\d+[a-zA-Z]?$", url):
            return True
        return any(s in url for s in ["zillow.com/homedetails/", "realtor.com/realestateandhomes-detail/", "compass.com/listing/"])

    def is_listing_site(url: str) -> bool:
        return any(s in url for s in ["streeteasy.com", "zillow.com", "compass.com", "realtor.com"])

    listing_result = (
        next((r for r in search_results if r.get("url") and is_unit_page(r["url"])), None) or
        next((r for r in search_results if r.get("url") and is_listing_site(r["url"])), None) or
        search_results[0]
    )

    if not listing_result or not listing_result.get("url"):
        return SourceResult(name="listing", status="ok", data={}, duration_ms=int((time.monotonic() - start) * 1000))

    scraped = await _firecrawl_scrape(listing_result["url"], ["extract"], {
        "prompt": "Extract the real estate listing data for this specific unit/property. Only extract the asking price for this unit — NOT the building's assessed value. All prices must be plain numbers.",
        "schema": LISTING_EXTRACT_SCHEMA,
    })

    extracted = scraped.get("extract") if scraped else None
    if not extracted:
        return SourceResult(name="listing", status="ok", data={}, duration_ms=int((time.monotonic() - start) * 1000))

    listing: dict = {"_sourceUrl": listing_result["url"]}
    if isinstance(extracted.get("price"), (int, float)) and 0 < extracted["price"] < 500_000_000:
        listing["price"] = extracted["price"]
    if isinstance(extracted.get("beds"), (int, float)) and 0 <= extracted["beds"] <= 20:
        listing["beds"] = extracted["beds"]
    if isinstance(extracted.get("baths"), (int, float)) and 0 <= extracted["baths"] <= 20:
        listing["baths"] = extracted["baths"]
    if isinstance(extracted.get("sqft"), (int, float)) and 0 < extracted["sqft"] < 100_000:
        listing["sqft"] = extracted["sqft"]
    if isinstance(extracted.get("yearBuilt"), (int, float)) and extracted["yearBuilt"] > 1700:
        listing["yearBuilt"] = extracted["yearBuilt"]
    if isinstance(extracted.get("commonCharges"), (int, float)) and extracted["commonCharges"] > 0:
        listing["commonCharges"] = extracted["commonCharges"]
    if isinstance(extracted.get("propertyTaxes"), (int, float)) and extracted["propertyTaxes"] > 0:
        listing["propertyTaxes"] = extracted["propertyTaxes"]
    if isinstance(extracted.get("daysOnMarket"), (int, float)) and extracted["daysOnMarket"] >= 0:
        listing["daysOnMarket"] = extracted["daysOnMarket"]
    if isinstance(extracted.get("buildingType"), str):
        listing["buildingType"] = extracted["buildingType"]
    if isinstance(extracted.get("features"), list):
        listing["features"] = [f for f in extracted["features"] if isinstance(f, str)][:15]
    if isinstance(extracted.get("description"), str):
        listing["description"] = extracted["description"][:500]
    if isinstance(extracted.get("totalRooms"), (int, float)):
        listing["totalRooms"] = extracted["totalRooms"]
    if isinstance(extracted.get("listingBrokerage"), str):
        listing["listingBrokerage"] = extracted["listingBrokerage"]
    if isinstance(extracted.get("listingAgents"), list):
        listing["listingAgents"] = [a for a in extracted["listingAgents"] if isinstance(a, str)]

    return SourceResult(name="listing", status="ok", data=listing, record_count=1,
                        duration_ms=int((time.monotonic() - start) * 1000))


async def fetch_streeteasy_comps(
    street_name: str, zip_code: str, beds: int, building_type: str,
    price: int, neighborhood: str | None = None,
) -> SourceResult:
    start = time.monotonic()
    type_slug = "coops" if "co-op" in (building_type or "condo").lower() else "condos"
    bed_label = f"{beds}-bedroom-" if beds > 0 else ""
    area_slug = re.sub(r"[^a-z0-9]+", "-", (neighborhood or "").lower()).strip("-") if neighborhood else ""

    urls: list[str | None] = []
    if area_slug:
        urls.append(f"https://streeteasy.com/recently-completed-sales/{area_slug}")
        urls.append(f"https://streeteasy.com/{bed_label}{type_slug}-for-sale/{area_slug}/status:closed")
        urls.append(f"https://streeteasy.com/{type_slug}-for-sale/{area_slug}/status:closed")
    urls.append(None)  # Google search fallback

    for url in urls:
        if url is None:
            query = f"site:streeteasy.com {zip_code} {beds} bedroom {type_slug} recently closed sold"
            try:
                search_results = await _firecrawl_search(query)
                se_url = next((r["url"] for r in (search_results or [])
                               if r.get("url") and "streeteasy.com" in r["url"]
                               and any(w in r["url"] for w in ["/closed", "/sold", "/sale/"])), None)
                if not se_url:
                    continue
                data = await _firecrawl_scrape(se_url, ["extract"], {"prompt": COMPS_EXTRACT_PROMPT, "schema": COMPS_EXTRACT_SCHEMA})
                listings = data.get("extract", {}).get("listings") if data else None
                if isinstance(listings, list) and listings:
                    validated = _validate_listings([_parse_listing(l) for l in listings[:15]])
                    if validated:
                        return SourceResult(name="streeteasyComps", status="ok", data=validated,
                                            record_count=len(validated),
                                            duration_ms=int((time.monotonic() - start) * 1000))
            except Exception as e:
                logger.error("[FIRECRAWL] comps search fallback error: %s", e)
            continue

        try:
            data = await _firecrawl_scrape(url, ["extract", "markdown"], {"prompt": COMPS_EXTRACT_PROMPT, "schema": COMPS_EXTRACT_SCHEMA})
            if not data:
                continue
            markdown = data.get("markdown", "")
            if len(markdown) < 200 or re.search(r"captcha|verify|blocked|access denied", markdown, re.I):
                continue
            listings = data.get("extract", {}).get("listings") if data else None
            if not isinstance(listings, list) or not listings:
                continue
            validated = _validate_listings([_parse_listing(l) for l in listings[:15]])
            if validated:
                return SourceResult(name="streeteasyComps", status="ok", data=validated,
                                    record_count=len(validated),
                                    duration_ms=int((time.monotonic() - start) * 1000))
        except Exception as e:
            logger.error("[FIRECRAWL] comps error for %s: %s", url, e)

    return SourceResult(name="streeteasyComps", status="ok", data=[],
                        duration_ms=int((time.monotonic() - start) * 1000))


async def enrich_dof_sales_via_streeteasy(dof_sales: list, beds: int, borough: str) -> list[dict]:
    if not dof_sales:
        return []

    seen: set[str] = set()
    building_addresses: list[dict] = []
    for s in dof_sales:
        addr = (s.get("address") or "").strip()
        if not addr or len(addr) < 5:
            continue
        parts = addr.split(",")[0].strip()
        match = re.match(r"^(\d[\d-]*)\s+(.+)", parts)
        if not match:
            continue
        key = f"{match.group(1)} {match.group(2)}".upper()
        if key in seen:
            continue
        seen.add(key)
        building_addresses.append({"address": addr, "houseNumber": match.group(1), "streetName": match.group(2)})
        if len(building_addresses) >= 5:
            break

    if not building_addresses:
        return []

    all_comps: list[dict] = []
    for bldg in building_addresses:
        try:
            query = f'site:streeteasy.com/building "{bldg["houseNumber"]} {bldg["streetName"]}" New York'
            search_results = await _firecrawl_search(query)
            building_url = next((r["url"] for r in (search_results or [])
                                 if r.get("url") and "streeteasy.com/building/" in r["url"]), None)
            if not building_url:
                continue
            data = await _firecrawl_scrape(building_url, ["extract"], {
                "prompt": "Extract ALL recently closed/sold units from this StreetEasy building page.",
                "schema": COMPS_EXTRACT_SCHEMA,
            })
            listings = data.get("extract", {}).get("listings") if data else None
            if not isinstance(listings, list):
                continue
            for l in listings:
                if isinstance(l.get("price"), (int, float)) and l["price"] > 10000 and l.get("address") and len(l["address"]) > 5:
                    all_comps.append(_parse_listing(l))
            if len(all_comps) >= 10:
                break
        except Exception as e:
            logger.error("[FIRECRAWL] enrich error for %s: %s", bldg["address"], e)

    matching = [c for c in all_comps if c.get("beds") == beds]
    return matching if len(matching) >= 3 else all_comps


async def fetch_same_building_listings(house_number: str, street_name: str, city: str) -> SourceResult:
    start = time.monotonic()
    try:
        query = f'site:streeteasy.com "{house_number} {street_name}" {city or "New York"}'
        search_results = await _firecrawl_search(query)
        building_url = next((r["url"] for r in (search_results or [])
                             if r.get("url") and "streeteasy.com/building/" in r["url"]), None)
        listing_urls = [r["url"] for r in (search_results or [])
                        if r.get("url") and "streeteasy.com" in r["url"]
                        and ("/sale/" in r["url"] or "/for-sale" in r["url"])][:3]
        target_url = building_url or (listing_urls[0] if listing_urls else None)
        if not target_url:
            return SourceResult(name="sameBuildingComps", status="ok", data=[],
                                duration_ms=int((time.monotonic() - start) * 1000))

        data = await _firecrawl_scrape(target_url, ["extract"], {
            "prompt": "Extract all property listings shown on this StreetEasy page — both active and recently sold units.",
            "schema": COMPS_EXTRACT_SCHEMA,
        })
        listings = data.get("extract", {}).get("listings") if data else None
        if not isinstance(listings, list) or not listings:
            return SourceResult(name="sameBuildingComps", status="ok", data=[],
                                duration_ms=int((time.monotonic() - start) * 1000))

        comps = [_parse_listing(l) for l in listings[:10]]
        return SourceResult(name="sameBuildingComps", status="ok", data=comps,
                            record_count=len(comps),
                            duration_ms=int((time.monotonic() - start) * 1000))
    except Exception as e:
        logger.error("[FIRECRAWL] same-building error: %s", e)
        return SourceResult(name="sameBuildingComps", status="failed", data=[],
                            duration_ms=int((time.monotonic() - start) * 1000), error=str(e))
