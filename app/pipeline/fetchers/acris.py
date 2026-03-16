"""ACRIS legals lookup with condo lot range expansion + chained master/parties enrichment."""

from __future__ import annotations

import asyncio
import logging
import time

from app.pipeline.constants import borough_to_id, to_num
from app.pipeline.contracts.socrata import get_contract
from app.pipeline.fetch_engine import resilient_fetch
from app.pipeline.fetchers.socrata import socrata_url
from app.pipeline.types import FetchOptions, SourceResult

logger = logging.getLogger(__name__)


async def _fetch_json(url: str, contract, label: str) -> list | None:
    result = await resilient_fetch(FetchOptions(
        url=url,
        timeout_ms=contract.timeout_ms,
        max_retries=contract.max_retries,
        retry_on=contract.retry_on,
        rate_limit_key=contract.rate_limit_key,
        label=label,
    ))
    if result.ok and isinstance(result.data, list):
        return result.data
    return None


async def fetch_acris_legals(
    borough_code: str, block: str, lot: str,
    units_res: int | None = None, house_number: str | None = None, street_name: str | None = None,
) -> SourceResult:
    contract = get_contract("acrisLegals")
    if not contract:
        return SourceResult(name="acrisLegals", status="failed", error="No contract")

    boro_id = borough_to_id(borough_code)
    clean_block = block.lstrip("0") or block
    clean_lot = lot.lstrip("0") or lot
    lot_num = to_num(clean_lot) or 0
    start = time.monotonic()

    address_filter = ""
    if house_number and street_name:
        normalized = street_name.strip().split()[0].upper() if street_name.strip() else ""
        address_filter = f" AND street_number='{house_number}' AND street_name LIKE '{normalized}%'"

    all_results: list[dict] = []
    seen: set[str] = set()

    def add_results(data: list | None):
        for r in (data or []):
            doc_id = r.get("document_id")
            if doc_id and doc_id not in seen:
                seen.add(doc_id)
                all_results.append(r)

    # Try 1: exact lot
    add_results(await _fetch_json(
        socrata_url(contract.endpoint,
                    f"borough='{boro_id}' AND block='{clean_block}' AND lot='{clean_lot}'{address_filter}",
                    order="good_through_date DESC", limit=200),
        contract, "acrisLegals-exact",
    ))

    # Try 2: condo range for lot > 1000
    if lot_num > 1000:
        range_base = (int(lot_num) // 100) * 100
        add_results(await _fetch_json(
            socrata_url(contract.endpoint,
                        f"borough='{boro_id}' AND block='{clean_block}' AND lot>='{range_base}' AND lot<='{range_base + 99}'{address_filter}",
                        order="good_through_date DESC", limit=200),
            contract, "acrisLegals-range",
        ))

    # Try 3: common condo ranges if too few
    if lot_num > 1000 and len(all_results) < 5:
        for range_start_str in ["1001", "1101", "1201", "1301"]:
            start_num = int(range_start_str)
            rb = (start_num // 100) * 100
            if (int(lot_num) // 100) * 100 == rb:
                continue
            add_results(await _fetch_json(
                socrata_url(contract.endpoint,
                            f"borough='{boro_id}' AND block='{clean_block}' AND lot>='{range_start_str}' AND lot<='{rb + 99}'{address_filter}",
                            order="good_through_date DESC", limit=200),
                contract, "acrisLegals-condo-range",
            ))
            if len(all_results) >= 5:
                break

    # Try 4: base lot for condo buildings with low lot number
    if lot_num < 1000 and (units_res or 0) > 1:
        for r in [("1001", "1099"), ("1101", "1199"), ("1201", "1299"), ("1301", "1399")]:
            add_results(await _fetch_json(
                socrata_url(contract.endpoint,
                            f"borough='{boro_id}' AND block='{clean_block}' AND lot>='{r[0]}' AND lot<='{r[1]}'{address_filter}",
                            order="good_through_date DESC", limit=200),
                contract, "acrisLegals-base-lot",
            ))
            if len(all_results) >= 10:
                break

    # Try 5: building base lot for common-area documents
    if lot_num >= 1000:
        add_results(await _fetch_json(
            socrata_url(contract.endpoint,
                        f"borough='{boro_id}' AND block='{clean_block}' AND lot<'100'",
                        order="good_through_date DESC", limit=50),
            contract, "acrisLegals-base",
        ))

    # Fallback: retry without address filter
    if address_filter and len(all_results) < 3 and lot_num > 1000:
        logger.info("[ACRIS] address filter returned only %d results — retrying without", len(all_results))
        range_base = (int(lot_num) // 100) * 100
        add_results(await _fetch_json(
            socrata_url(contract.endpoint,
                        f"borough='{boro_id}' AND block='{clean_block}' AND lot>='{range_base}' AND lot<='{range_base + 99}'",
                        order="good_through_date DESC", limit=200),
            contract, "acrisLegals-no-addr",
        ))

    logger.info("[ACRIS] legals: %d total records for block %s", len(all_results), clean_block)
    return SourceResult(
        name="acrisLegals", status="ok", data=all_results,
        record_count=len(all_results),
        duration_ms=int((time.monotonic() - start) * 1000),
    )


async def fetch_acris_details(document_ids: list[str]) -> tuple[SourceResult, SourceResult]:
    if not document_ids:
        return (
            SourceResult(name="acrisMasters", status="ok", data=[]),
            SourceResult(name="acrisParties", status="ok", data=[]),
        )

    master_contract = get_contract("acrisMaster")
    parties_contract = get_contract("acrisParties")
    if not master_contract or not parties_contract:
        return (
            SourceResult(name="acrisMasters", status="failed", error="No contract"),
            SourceResult(name="acrisParties", status="failed", error="No contract"),
        )

    id_list = ",".join(f"'{did}'" for did in document_ids)
    start = time.monotonic()

    masters_task = _fetch_json(
        socrata_url(master_contract.endpoint, f"document_id in({id_list})", limit=200),
        master_contract, "acrisMaster",
    )
    parties_task = _fetch_json(
        socrata_url(parties_contract.endpoint, f"document_id in({id_list})", limit=500),
        parties_contract, "acrisParties",
    )

    results = await asyncio.gather(masters_task, parties_task, return_exceptions=True)
    elapsed = int((time.monotonic() - start) * 1000)

    masters_data = results[0] if isinstance(results[0], list) else []
    parties_data = results[1] if isinstance(results[1], list) else []

    return (
        SourceResult(
            name="acrisMasters",
            status="ok" if isinstance(results[0], list) else "failed",
            data=masters_data, record_count=len(masters_data), duration_ms=elapsed,
            error=str(results[0]) if isinstance(results[0], Exception) else None,
        ),
        SourceResult(
            name="acrisParties",
            status="ok" if isinstance(results[1], list) else "failed",
            data=parties_data, record_count=len(parties_data), duration_ms=elapsed,
            error=str(results[1]) if isinstance(results[1], Exception) else None,
        ),
    )
