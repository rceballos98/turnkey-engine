"""DOB BIS HTML scraping: property profile, boiler, elevator."""

from __future__ import annotations

import asyncio
import logging
import re
import time

import httpx

from app.pipeline.constants import borough_to_id, to_num
from app.pipeline.contracts.non_socrata import get_non_socrata_contract
from app.pipeline.contracts.socrata import get_contract
from app.pipeline.fetch_engine import get_client
from app.pipeline.types import SourceResult

logger = logging.getLogger(__name__)


def _clean_html(s: str) -> str:
    return re.sub(r"&nbsp;", " ", re.sub(r"<[^>]*>", "", s)).strip()


async def _fetch_dob_bis_html(url: str, max_retries: int = 3, timeout_ms: int = 20000) -> str | None:
    client = await get_client()
    for attempt in range(max_retries):
        try:
            logger.info("[DOB-BIS] fetch HTML (attempt %d/%d): %s", attempt + 1, max_retries, url)
            response = await client.get(url, timeout=httpx.Timeout(timeout_ms / 1000.0, connect=10.0))
            if not response.is_success:
                logger.error("[DOB-BIS] HTTP error %d for %s", response.status_code, url)
                return None
            html = response.text
            if "Visitor Prioritization" in html:
                logger.info("[DOB-BIS] visitor queue (attempt %d/%d), retrying in 6s...", attempt + 1, max_retries)
                await asyncio.sleep(6)
                continue
            return html
        except Exception as e:
            logger.error("[DOB-BIS] fetch error (attempt %d/%d): %s", attempt + 1, max_retries, e)
    logger.error("[DOB-BIS] all %d attempts failed for: %s", max_retries, url)
    return None


async def fetch_dob_bis_profile(boro_code: str, block: str, lot: str) -> SourceResult:
    contract = get_non_socrata_contract("dobBISProfile")
    boro_num = borough_to_id(boro_code)
    padded_block = block.zfill(5)
    padded_lot = lot.zfill(5)
    url = f"https://a810-bisweb.nyc.gov/bisweb/PropertyProfileOverviewServlet?boro={boro_num}&block={padded_block}&lot={padded_lot}"
    start = time.monotonic()

    try:
        html = await _fetch_dob_bis_html(url, contract.max_retries if contract else 3, contract.timeout_ms if contract else 20000)
        if not html:
            return SourceResult(name="dobBISProfile", status="failed", data=None,
                                duration_ms=int((time.monotonic() - start) * 1000),
                                error="Failed to fetch DOB BIS HTML")

        bin_match = re.search(r"BIN#&nbsp;&nbsp;(\d+)", html)
        complaints_match = re.search(r"Complaints</a></b></td>\s*<td[^>]*>(\d+)</td>", html, re.I)
        dob_viol_match = re.search(r"Violations-DOB</a></b></td>\s*<td[^>]*>(\d+)</td>", html, re.I)
        ecb_viol_match = re.search(r"Violations-OATH/ECB</a></b></td>\s*<td[^>]*>(\d+)</td>", html, re.I)
        cross_match = re.search(r"Cross Street\(s\):</b></td>\s*<td[^>]*>(.*?)</td>", html, re.I)
        remarks_match = re.search(r"DOB Building Remarks:</b></td>\s*<td[^>]*>(.*?)</td>", html, re.I)
        boiler_url_match = re.search(r'href="(BoilerComplianceQueryServlet[^"]+)"', html, re.I) or \
                           re.search(r'href="(/bisweb/BoilerComplianceQueryServlet[^"]+)"', html, re.I)

        boiler_url = None
        if boiler_url_match:
            href = boiler_url_match.group(1)
            boiler_url = href if href.startswith("http") else f"https://a810-bisweb.nyc.gov/bisweb/{href}"

        profile = {
            "bin": bin_match.group(1) if bin_match else None,
            "dobViolationCount": to_num(dob_viol_match.group(1)) if dob_viol_match else None,
            "ecbViolationCount": to_num(ecb_viol_match.group(1)) if ecb_viol_match else None,
            "dobComplaintCount": to_num(complaints_match.group(1)) if complaints_match else None,
            "crossStreets": _clean_html(cross_match.group(1)) if cross_match else None,
            "buildingRemarks": _clean_html(remarks_match.group(1)) if remarks_match else None,
            "boilerComplianceUrl": boiler_url,
        }

        return SourceResult(name="dobBISProfile", status="ok", data=profile, record_count=1,
                            duration_ms=int((time.monotonic() - start) * 1000))
    except Exception as e:
        logger.error("[DOB-BIS] Profile error: %s", e)
        return SourceResult(name="dobBISProfile", status="failed", data=None,
                            duration_ms=int((time.monotonic() - start) * 1000), error=str(e))


async def fetch_dob_bis_boiler_records(boiler_url: str) -> SourceResult:
    contract = get_non_socrata_contract("dobBISBoilers")
    full_url = boiler_url if boiler_url.startswith("http") else f"https://a810-bisweb.nyc.gov/bisweb/{boiler_url}"
    start = time.monotonic()

    try:
        html = await _fetch_dob_bis_html(full_url, contract.max_retries if contract else 3, contract.timeout_ms if contract else 20000)
        if not html:
            return SourceResult(name="dobBISBoilers", status="failed", data=[],
                                duration_ms=int((time.monotonic() - start) * 1000),
                                error="Failed to fetch DOB BIS boiler HTML")

        boilers: list[dict] = []
        row_regex = re.compile(
            r'<tr>\s*<td class="content"[^>]*>(.*?)</td>\s*<td class="content"[^>]*>(.*?)</td>\s*'
            r'<td class="content"[^>]*>(.*?)</td>\s*<td class="content"[^>]*>(.*?)</td>\s*'
            r'<td class="content"[^>]*>(.*?)</td>\s*<td class="content"[^>]*>(.*?)</td>\s*'
            r'<td class="content"[^>]*>(.*?)</td>\s*<td class="content"[^>]*>(.*?)</td>\s*</tr>',
            re.I | re.S,
        )
        for match in row_regex.finditer(html):
            viol, num_cell, md, ser, status, insp_date, recv_date, name = match.groups()
            num_match = re.search(r"<b>(\d+)</b>", num_cell) or re.search(r"(\d+)", num_cell)
            if not num_match:
                continue
            boilers.append({
                "boilerNumber": num_match.group(1),
                "violation": _clean_html(viol),
                "multipleDwelling": _clean_html(md),
                "serialNumber": _clean_html(ser),
                "status": _clean_html(status),
                "inspectionDate": _clean_html(insp_date) or None,
                "receivedDate": _clean_html(recv_date) or None,
                "insuranceCompany": _clean_html(name) or None,
            })

        return SourceResult(name="dobBISBoilers", status="ok", data=boilers,
                            record_count=len(boilers),
                            duration_ms=int((time.monotonic() - start) * 1000))
    except Exception as e:
        logger.error("[DOB-BIS] Boiler error: %s", e)
        return SourceResult(name="dobBISBoilers", status="failed", data=[],
                            duration_ms=int((time.monotonic() - start) * 1000), error=str(e))


async def fetch_dob_bis_elevator_records(bin_number: str) -> SourceResult:
    contract = get_non_socrata_contract("dobBISElevator")
    url = f"https://a810-bisweb.nyc.gov/bisweb/ElevatorRecordsByLocationServlet?requestid=0&allbin={bin_number}"
    start = time.monotonic()

    try:
        html = await _fetch_dob_bis_html(url, contract.max_retries if contract else 3, contract.timeout_ms if contract else 20000)
        if not html:
            return SourceResult(name="dobBISElevator", status="failed", data=None,
                                duration_ms=int((time.monotonic() - start) * 1000),
                                error="Failed to fetch DOB BIS elevator HTML")

        records: list[dict] = []
        total_inspections = total_violations = total_devices = 0

        # Parse from HTML comments
        for comment in re.findall(r"<!--([\s\S]*?)-->", html):
            rec_nums = re.findall(r"\[0:LlRecordNumber\]\{(\d+)\}", comment)
            house_nums = re.findall(r"\[1:HouseNumber\]\{([^}]*)\}", comment)
            streets = re.findall(r"\[2:StreetName\]\{([^}]*)\}", comment)
            dev_counts = re.findall(r"\[3:LlNumOfDevices\]\{(\d+)\}", comment)
            insp_counts = re.findall(r"\[4:InspectCountQq30\]\{(\d+)\}", comment)
            viol_counts = re.findall(r"\[5:ViolCountQq30\]\{(\d+)\}", comment)

            for i in range(len(rec_nums)):
                insp_count = int(insp_counts[i]) if i < len(insp_counts) else 0
                viol_count = int(viol_counts[i]) if i < len(viol_counts) else 0
                dev_count = int(dev_counts[i]) if i < len(dev_counts) else 0
                total_inspections += insp_count
                total_violations += viol_count
                total_devices += dev_count
                records.append({
                    "recordNumber": rec_nums[i],
                    "houseNumber": house_nums[i].strip() if i < len(house_nums) else "",
                    "streetName": streets[i].strip() if i < len(streets) else "",
                    "numDevices": dev_count,
                    "inspectionCount": insp_count,
                    "violationCount": viol_count,
                })

        data = {"totalInspections": total_inspections, "totalViolations": total_violations,
                "totalDevices": total_devices, "records": records}
        return SourceResult(name="dobBISElevator", status="ok", data=data,
                            record_count=len(records),
                            duration_ms=int((time.monotonic() - start) * 1000))
    except Exception as e:
        logger.error("[DOB-BIS] Elevator error: %s", e)
        return SourceResult(name="dobBISElevator", status="failed", data=None,
                            duration_ms=int((time.monotonic() - start) * 1000), error=str(e))


async def fetch_bin_from_address(house_number: str, street: str) -> str | None:
    try:
        street_upper = street.upper().strip()
        variants = [
            street_upper,
            re.sub(r"\bSTREET\b", "ST", re.sub(r"\bAVENUE\b", "AVE", street_upper)),
            re.sub(r"\bST\b", "STREET", re.sub(r"\bAVE\b", "AVENUE", street_upper)),
        ]
        contract = get_contract("dobJobs")
        if not contract:
            return None

        from app.pipeline.fetch_engine import resilient_fetch
        from app.pipeline.types import FetchOptions
        from urllib.parse import urlencode

        for variant in variants:
            params = urlencode({
                "$where": f"house__='{house_number}' AND street_name LIKE '{variant}%'",
                "$select": "bin__",
                "$limit": "1",
            })
            url = f"{contract.endpoint}?{params}"
            result = await resilient_fetch(FetchOptions(
                url=url, timeout_ms=contract.timeout_ms, max_retries=1, retry_on=[], label="bin-fallback",
            ))
            if result.ok and isinstance(result.data, list) and result.data:
                bin_val = result.data[0].get("bin__")
                if bin_val:
                    return str(bin_val)
        return None
    except Exception:
        return None
