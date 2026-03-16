"""Generic contract-driven Socrata fetcher."""

from __future__ import annotations

import logging
from urllib.parse import urlencode

from app.pipeline.contracts.socrata import get_contract, SocrataContract
from app.pipeline.fetch_engine import resilient_fetch, to_source_result
from app.pipeline.types import FetchOptions, SourceResult

logger = logging.getLogger(__name__)


def socrata_url(endpoint: str, where: str, order: str | None = None,
                limit: int | None = None, select: str | None = None) -> str:
    params: dict[str, str] = {"$where": where}
    if order:
        params["$order"] = order
    if limit:
        params["$limit"] = str(limit)
    if select:
        params["$select"] = select
    return f"{endpoint}?{urlencode(params)}"


def build_where(template: str, params: dict, contract: SocrataContract) -> str:
    where = template
    if params.get("bbl"):
        where = where.replace("{bbl}", params["bbl"])
    if params.get("bin"):
        where = where.replace("{bin}", params["bin"])
    if params.get("boro"):
        where = where.replace("{boro}", params["boro"])
    if params.get("block"):
        padded = params["block"].zfill(contract.pad_block) if contract.pad_block else params["block"]
        where = where.replace("{block}", padded)
    if params.get("lot"):
        padded = params["lot"].zfill(contract.pad_lot) if contract.pad_lot else params["lot"]
        where = where.replace("{lot}", padded)
    if params.get("parid"):
        where = where.replace("{parid}", params["parid"])
    if params.get("dashedBbl"):
        where = where.replace("{dashedBbl}", params["dashedBbl"])
    if params.get("lat") is not None:
        where = where.replace("{lat}", str(params["lat"]))
    if params.get("lon") is not None:
        where = where.replace("{lon}", str(params["lon"]))
    if params.get("radius") is not None:
        where = where.replace("{radius}", str(params["radius"]))
    if params.get("registrationId"):
        where = where.replace("{registrationId}", params["registrationId"])
    if params.get("boroFullName"):
        where = where.replace("{boroFullName}", params["boroFullName"])
    if params.get("documentIds"):
        id_list = ",".join(f"'{did}'" for did in params["documentIds"])
        where = where.replace("{documentIds}", id_list)
    return where


async def fetch_socrata_source(contract_name: str, params: dict) -> SourceResult:
    contract = get_contract(contract_name)
    if not contract:
        logger.error("[fetchSocrataSource] Unknown contract: %s", contract_name)
        return SourceResult(name=contract_name, status="failed", data=[], error=f"Unknown contract: {contract_name}")

    where = build_where(contract.where_template, params, contract)
    url = socrata_url(contract.endpoint, where, order=contract.order_by, limit=contract.limit)

    result = await resilient_fetch(FetchOptions(
        url=url,
        timeout_ms=contract.timeout_ms,
        max_retries=contract.max_retries,
        retry_on=contract.retry_on,
        rate_limit_key=contract.rate_limit_key,
        label=contract_name,
    ))

    sr = to_source_result(contract_name, result)
    if sr.status == "ok" and isinstance(sr.data, list):
        sr.record_count = len(sr.data)
    return sr


async def fetch_socrata_one(contract_name: str, params: dict) -> SourceResult:
    sr = await fetch_socrata_source(contract_name, params)
    if sr.status == "ok" and isinstance(sr.data, list) and len(sr.data) > 0:
        sr.data = sr.data[0]
        sr.record_count = 1
    return sr
