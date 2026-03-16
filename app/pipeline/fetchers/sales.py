"""NYC Sales with progressive date windows + DOF financials."""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timedelta

from app.pipeline.constants import borough_to_id, bbl_to_dashed, to_num
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


async def fetch_nyc_sales(
    borough_code: str, zip_code: str, block: str | None = None,
    months: int = 6, building_class_filter: str | None = None,
) -> SourceResult:
    contract = get_contract("nycSales")
    if not contract:
        return SourceResult(name="nycSales", status="failed", error="No contract")

    boro_id = borough_to_id(borough_code)
    start = time.monotonic()
    results: list[dict] = []
    seen: set[str] = set()

    def add_sales(data: list | None):
        for s in (data or []):
            key = f"{s.get('address', '')}-{s.get('sale_date', '')}-{s.get('sale_price', '')}"
            if key not in seen:
                seen.add(key)
                results.append(s)

    condo_filter = f" AND building_class_category LIKE '%{building_class_filter}%'" if building_class_filter else ""
    date_windows = [months, 12, 24]
    used_window = date_windows[0]

    if block:
        padded_block = block.zfill(5)
        for window_months in date_windows:
            cutoff = (datetime.utcnow() - timedelta(days=window_months * 30)).strftime("%Y-%m-%d")
            url = socrata_url(
                contract.endpoint,
                f"borough='{boro_id}' AND block='{padded_block}' AND sale_price > '100000' AND sale_date >= '{cutoff}'{condo_filter}",
                order="sale_date DESC", limit=20,
            )
            add_sales(await _fetch_json(url, contract, f"nycSales-block-{window_months}mo"))
            used_window = window_months
            if len(results) >= 3:
                break
            logger.info("[SALES] only %d results in %d months — expanding", len(results), window_months)

    # Zip code broader comps
    zip_months = max(used_window, 24)
    zip_cutoff = (datetime.utcnow() - timedelta(days=zip_months * 30)).strftime("%Y-%m-%d")
    zip_url = socrata_url(
        contract.endpoint,
        f"borough='{boro_id}' AND zip_code='{zip_code}' AND sale_price > '100000' AND sale_date >= '{zip_cutoff}'{condo_filter}",
        order="sale_date DESC", limit=100,
    )
    add_sales(await _fetch_json(zip_url, contract, "nycSales-zip"))

    final_window = f"{zip_months} months"
    logger.info("[SALES] %d total results (%s)", len(results), final_window)
    return SourceResult(
        name="nycSales", status="ok",
        data={"sales": results, "dateWindow": final_window},
        record_count=len(results),
        duration_ms=int((time.monotonic() - start) * 1000),
    )


async def fetch_dof_financials(bbl: str) -> SourceResult:
    condo_contract = get_contract("dofCondoIncome")
    coop_contract = get_contract("dofCoopIncome")
    if not condo_contract or not coop_contract:
        return SourceResult(name="dofFinancials", status="failed", error="No contract")

    dashed_bbl = bbl_to_dashed(bbl)
    start = time.monotonic()

    results = await asyncio.gather(
        _fetch_json(
            socrata_url(condo_contract.endpoint, f"boro_block_lot='{dashed_bbl}'",
                        order="report_year DESC", limit=5),
            condo_contract, "dofCondoIncome",
        ),
        _fetch_json(
            socrata_url(coop_contract.endpoint, f"boro_block_lot='{dashed_bbl}'",
                        order="report_year DESC", limit=5),
            coop_contract, "dofCoopIncome",
        ),
        return_exceptions=True,
    )

    condo_records = results[0] if isinstance(results[0], list) else []
    coop_records = results[1] if isinstance(results[1], list) else []

    if not condo_records and not coop_records:
        return SourceResult(name="dofFinancials", status="ok", data=None,
                            duration_ms=int((time.monotonic() - start) * 1000))

    is_condo = len(condo_records) > 0
    records = condo_records if is_condo else coop_records
    latest = records[0]

    data = {
        "type": "condo" if is_condo else "coop",
        "reportYear": latest.get("report_year"),
        "address": latest.get("address"),
        "neighborhood": latest.get("neighborhood"),
        "buildingClassification": latest.get("building_classification"),
        "totalUnits": to_num(latest.get("total_units")),
        "yearBuilt": to_num(latest.get("year_built")),
        "grossSqft": to_num(latest.get("gross_sqft")),
        "estimatedGrossIncome": to_num(latest.get("estimated_gross_income")),
        "grossIncomePerSqft": to_num(latest.get("gross_income_per_sqft")),
        "estimatedExpense": to_num(latest.get("estimated_expense")),
        "expensePerSqft": to_num(latest.get("expense_per_sqft")),
        "netOperatingIncome": to_num(latest.get("net_operating_income")),
        "fullMarketValue": to_num(latest.get("full_market_value")),
        "marketValuePerSqft": to_num(latest.get("market_value_per_sqft")),
        "historicalRecords": [
            {
                "reportYear": r.get("report_year"),
                "estimatedGrossIncome": to_num(r.get("estimated_gross_income")),
                "estimatedExpense": to_num(r.get("estimated_expense")),
                "netOperatingIncome": to_num(r.get("net_operating_income")),
                "fullMarketValue": to_num(r.get("full_market_value")),
                "expensePerSqft": to_num(r.get("expense_per_sqft")),
            }
            for r in records
        ],
    }

    return SourceResult(name="dofFinancials", status="ok", data=data, record_count=1,
                        duration_ms=int((time.monotonic() - start) * 1000))
