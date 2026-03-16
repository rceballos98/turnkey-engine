"""3-phase fetch orchestrator with asyncio.gather + 180s hard timeout."""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime

from app.pipeline.constants import borough_to_id, borough_to_full_name_upper, bbl_to_parid, to_num
from app.pipeline.types import SourceResult, build_manifest

from app.pipeline.fetchers.socrata import fetch_socrata_source, fetch_socrata_one
from app.pipeline.fetchers.pluto import fetch_pluto, fetch_pluto_by_bbl
from app.pipeline.fetchers.acris import fetch_acris_legals, fetch_acris_details
from app.pipeline.fetchers.sales import fetch_nyc_sales, fetch_dof_financials
from app.pipeline.fetchers.dob_bis import (
    fetch_dob_bis_profile, fetch_dob_bis_boiler_records,
    fetch_dob_bis_elevator_records, fetch_bin_from_address,
)
from app.pipeline.fetchers.firecrawl import (
    fetch_listing_data, fetch_streeteasy_comps,
    enrich_dof_sales_via_streeteasy, fetch_same_building_listings,
)
from app.pipeline.fetchers.external import (
    fetch_fema_flood_zone, fetch_environmental_sites,
    fetch_zoning_districts, fetch_311_requests,
    parse_address_with_ai, ParsedAddress,
)

logger = logging.getLogger(__name__)

PIPELINE_TIMEOUT_MS = 180_000


async def orchestrate_fetch(address: str) -> dict:
    """Orchestrate all data fetching in 3 sequential phases. Returns raw_data dict."""
    pipeline_start = time.monotonic()
    all_source_results: list[SourceResult] = []
    aborted = False

    async def _with_timeout():
        nonlocal aborted
        await asyncio.sleep(PIPELINE_TIMEOUT_MS / 1000.0)
        aborted = True
        logger.warning("[PIPELINE] Hard timeout reached (%dms)", PIPELINE_TIMEOUT_MS)

    timeout_task = asyncio.create_task(_with_timeout())

    try:
        # ═══ PHASE 1: Foundation ═══
        logger.info("[PIPELINE] Phase 1: Foundation")

        parsed = await parse_address_with_ai(address)

        # PLUTO lookup
        pluto_result = await fetch_pluto(parsed.house_number, parsed.street, parsed.borough, parsed.zip)
        all_source_results.append(pluto_result)
        pluto = pluto_result.data if pluto_result.status == "ok" else None
        logger.info("[PIPELINE] PLUTO: %s", f"BBL={pluto.get('bbl')}" if pluto else "not found")

        # DOB BIS Profile + BIN resolution
        dob_bis_profile_result = None
        bin_number = ""

        if pluto:
            dob_bis_profile_result = await fetch_dob_bis_profile(parsed.borough, pluto.get("block", ""), pluto.get("lot", ""))
            all_source_results.append(dob_bis_profile_result)
            profile = dob_bis_profile_result.data

            if not bin_number and profile and profile.get("bin"):
                bin_number = profile["bin"]
                logger.info("[PIPELINE] BIN from DOB BIS Profile: %s", bin_number)
            if not bin_number and parsed.house_number:
                bin_number = await fetch_bin_from_address(parsed.house_number, parsed.street) or ""
                if bin_number:
                    logger.info("[PIPELINE] BIN from DOB Jobs: %s", bin_number)
        elif parsed.house_number:
            bin_number = await fetch_bin_from_address(parsed.house_number, parsed.street) or ""
            if bin_number:
                logger.info("[PIPELINE] BIN from DOB Jobs (no PLUTO): %s", bin_number)

        # Firecrawl listing search
        listing_result = await fetch_listing_data(
            parsed.house_number, parsed.street, parsed.unit,
            parsed.city, parsed.state, address,
        )
        all_source_results.append(listing_result)
        listing = listing_result.data or {}

        # Derived identifiers
        lat = to_num(pluto.get("latitude")) if pluto else None
        lon = to_num(pluto.get("longitude")) if pluto else None
        zip_code = parsed.zip or (pluto.get("zipcode", "") if pluto else "")
        bbl = str(pluto["bbl"]).split(".")[0] if pluto and pluto.get("bbl") else None
        block = pluto.get("block", "") if pluto else ""
        lot = pluto.get("lot", "") if pluto else ""
        boro_id = borough_to_id(parsed.borough)
        parid = bbl_to_parid(boro_id, block, lot) if bbl else ""
        listing_beds = listing.get("beds", 2)
        listing_building_type = listing.get("buildingType", "condo")

        # ═══ PHASE 2: Parallel batch ═══
        if not aborted:
            logger.info("[PIPELINE] Phase 2: Parallel batch")
            sources: list[tuple[str, object]] = []

            # BIN-dependent
            if bin_number:
                sources.append(("dobJobs", fetch_socrata_source("dobJobs", {"bin": bin_number})))
                sources.append(("dobEcb", fetch_socrata_source("dobEcb", {"bin": bin_number})))
                sources.append(("dobComplaints", fetch_socrata_source("dobComplaints", {"bin": bin_number})))
                sources.append(("boilerData", fetch_socrata_source("boilerData", {"bin": bin_number})))
                sources.append(("dobBISElevator", fetch_dob_bis_elevator_records(bin_number)))
                sources.append(("hpdRegistration", fetch_socrata_one("hpdRegistration", {"bin": bin_number})))
            else:
                for name in ["dobJobs", "dobEcb", "dobComplaints", "boilerData", "dobBISElevator", "hpdRegistration"]:
                    default_data = None if name == "dobBISElevator" else []
                    all_source_results.append(SourceResult(name=name, status="skipped", data=default_data, error="BIN not available"))

            # DOB BIS Boiler
            dob_profile = dob_bis_profile_result.data if dob_bis_profile_result else None
            if dob_profile and dob_profile.get("boilerComplianceUrl"):
                sources.append(("dobBISBoilers", fetch_dob_bis_boiler_records(dob_profile["boilerComplianceUrl"])))
            elif bin_number and pluto:
                padded_block = (pluto.get("block", "")).zfill(5)
                padded_lot = (pluto.get("lot", "")).zfill(5)
                direct_url = f"https://a810-bisweb.nyc.gov/bisweb/BoilerRecordsByLocationServlet?requestid=0&allkey={boro_id}{padded_block}{padded_lot}&allbin={bin_number}"
                sources.append(("dobBISBoilers", fetch_dob_bis_boiler_records(direct_url)))
            else:
                all_source_results.append(SourceResult(name="dobBISBoilers", status="skipped", data=[], error="No boiler URL or BIN"))

            # BBL-dependent
            if bbl:
                sources.append(("dobViolations", fetch_socrata_source("dobViolations", {"boro": boro_id, "block": block, "lot": lot})))
                sources.append(("hpdViolations", fetch_socrata_source("hpdViolations", {"boro": boro_id, "block": block, "lot": lot})))
                units_res = int(to_num(pluto.get("unitsres")) or 0) if pluto else 0
                sources.append(("acrisLegals", fetch_acris_legals(parsed.borough, block, lot, units_res, parsed.house_number, parsed.street)))
                sources.append(("elevatorData", fetch_socrata_source("elevatorData", {"bbl": bbl})))
                sources.append(("hpdComplaints", fetch_socrata_source("hpdComplaints", {"bbl": bbl})))
                sources.append(("hpdLitigation", fetch_socrata_source("hpdLitigation", {"bbl": bbl})))
                sources.append(("certOccupancy", fetch_socrata_source("certOccupancy", {"bbl": bbl})))
                sources.append(("dofExemptions", fetch_socrata_source("dofExemptions", {"parid": parid})))
                sources.append(("dofAbatements", fetch_socrata_source("dofAbatements", {"parid": parid})))
                sources.append(("taxLienSales", fetch_socrata_source("taxLienSales", {"boro": boro_id, "block": block, "lot": lot})))
                sources.append(("energyBenchmarking", fetch_socrata_one("energyBenchmarking", {"bbl": bbl})))
                sources.append(("landmarks", fetch_socrata_one("landmarks", {"bbl": bbl})))
                sources.append(("fdnyViolations", fetch_socrata_source("fdnyViolations", {"boroFullName": borough_to_full_name_upper(parsed.borough), "block": block, "lot": lot})))
                sources.append(("dobNowPermits", fetch_socrata_source("dobNowPermits", {"bbl": bbl})))
                sources.append(("zoningDistricts", fetch_zoning_districts(bbl)))
                sources.append(("dofFinancials", fetch_dof_financials(bbl)))
                sources.append(("sr311", fetch_311_requests(bbl, lat, lon)))

            # Geo-dependent
            if lat and lon:
                sources.append(("floodZone", fetch_fema_flood_zone(lat, lon)))
                sources.append(("envSites", fetch_environmental_sites(lat, lon)))
                sources.append(("nypdCrime", fetch_socrata_source("nypdCrime", {"lat": lat, "lon": lon, "radius": 500})))

            # Sales + comps
            if zip_code or block:
                sales_filter = "COOPS" if "co-op" in listing_building_type.upper() else "CONDO"
                sources.append(("nycSales", fetch_nyc_sales(parsed.borough, zip_code, block or None, 6, sales_filter)))
            if parsed.street and zip_code:
                sources.append(("streeteasyComps", fetch_streeteasy_comps(parsed.street, zip_code, listing_beds, listing_building_type, listing.get("price", 0))))
            if parsed.house_number and parsed.street:
                sources.append(("sameBuildingComps", fetch_same_building_listings(parsed.house_number, parsed.street, parsed.city)))

            logger.info("[PIPELINE] Launching %d parallel data fetches...", len(sources))
            parallel_start = time.monotonic()

            settled = await asyncio.gather(*(coro for _, coro in sources), return_exceptions=True)

            for i, result in enumerate(settled):
                name = sources[i][0]
                if isinstance(result, Exception):
                    all_source_results.append(SourceResult(name=name, status="failed", data=[], error=str(result)))
                else:
                    all_source_results.append(result)

            parallel_duration = int((time.monotonic() - parallel_start) * 1000)
            ok_count = sum(1 for r in settled if not isinstance(r, Exception) and r.status == "ok")
            fail_count = sum(1 for r in settled if isinstance(r, Exception) or (not isinstance(r, Exception) and r.status == "failed"))
            logger.info("[PIPELINE] phase2 | %d sources | %d ok / %d failed | %dms",
                        len(sources), ok_count, fail_count, parallel_duration)

        # ═══ PHASE 3: Chained fetches ═══
        if not aborted:
            logger.info("[PIPELINE] Phase 3: Chained fetches")

            # ACRIS details
            acris_legals_result = next((r for r in all_source_results if r.name == "acrisLegals"), None)
            acris_legals_data = acris_legals_result.data if acris_legals_result and acris_legals_result.status == "ok" and isinstance(acris_legals_result.data, list) else []
            if acris_legals_data:
                doc_ids = list({l.get("document_id") for l in acris_legals_data if l.get("document_id")})[:50]
                masters_result, parties_result = await fetch_acris_details(doc_ids)
                all_source_results.extend([masters_result, parties_result])
            else:
                all_source_results.append(SourceResult(name="acrisMasters", status="ok", data=[]))
                all_source_results.append(SourceResult(name="acrisParties", status="ok", data=[]))

            # HPD Reg Contacts
            hpd_reg_result = next((r for r in all_source_results if r.name == "hpdRegistration"), None)
            reg_id = hpd_reg_result.data.get("registrationid") if hpd_reg_result and hpd_reg_result.status == "ok" and isinstance(hpd_reg_result.data, dict) else None
            if reg_id:
                contacts_result = await fetch_socrata_source("hpdRegContacts", {"registrationId": reg_id})
                contacts_result.name = "hpdRegContacts"
                all_source_results.append(contacts_result)
            else:
                all_source_results.append(SourceResult(name="hpdRegContacts", status="ok", data=[]))

            # DOF Sales enrichment
            se_comps_result = next((r for r in all_source_results if r.name == "streeteasyComps"), None)
            nyc_sales_result = next((r for r in all_source_results if r.name == "nycSales"), None)
            if (se_comps_result and isinstance(se_comps_result.data, list) and len(se_comps_result.data) == 0
                    and nyc_sales_result and nyc_sales_result.data):
                sales_data = nyc_sales_result.data.get("sales", nyc_sales_result.data) if isinstance(nyc_sales_result.data, dict) else nyc_sales_result.data
                if isinstance(sales_data, list) and sales_data:
                    logger.info("[PIPELINE] StreetEasy 0 comps — enriching DOF sales via building pages...")
                    top_dof = sorted(
                        [s for s in sales_data if (to_num(s.get("sale_price")) or 0) > 100000],
                        key=lambda s: s.get("sale_date", ""), reverse=True,
                    )[:15]
                    enriched = await enrich_dof_sales_via_streeteasy(top_dof, listing_beds, parsed.city or "New York")
                    if enriched:
                        se_comps_result.data = enriched
                        se_comps_result.record_count = len(enriched)

        # ═══ ASSEMBLY ═══
        manifest = build_manifest(all_source_results)
        total_duration_ms = int((time.monotonic() - pipeline_start) * 1000)

        def get_data(name: str):
            r = next((s for s in all_source_results if s.name == name), None)
            if r is not None:
                return r.data
            return [] if name.endswith("s") or name in ["dobJobs", "dobEcb", "dobComplaints", "elevatorData", "boilerData", "nycSales"] else None

        nyc_sales_data = get_data("nycSales")
        nyc_sales = nyc_sales_data.get("sales", nyc_sales_data) if isinstance(nyc_sales_data, dict) else (nyc_sales_data or [])
        nyc_sales_date_window = nyc_sales_data.get("dateWindow", "6 months") if isinstance(nyc_sales_data, dict) else "6 months"

        raw_data = {
            "parsed": parsed.model_dump(),
            "pluto": pluto,
            "listing": listing,
            "dobBISProfile": dob_bis_profile_result.data if dob_bis_profile_result else None,
            "dobViolations": get_data("dobViolations") or [],
            "dobEcb": get_data("dobEcb") or [],
            "dobComplaints": get_data("dobComplaints") or [],
            "hpdViolations": get_data("hpdViolations") or [],
            "hpdComplaints": get_data("hpdComplaints") or [],
            "hpdLitigation": get_data("hpdLitigation") or [],
            "fdnyViolations": get_data("fdnyViolations") or [],
            "hpdRegistration": get_data("hpdRegistration"),
            "hpdRegContacts": get_data("hpdRegContacts") or [],
            "elevatorData": get_data("elevatorData") or [],
            "boilerData": get_data("boilerData") or [],
            "dobBISElevator": get_data("dobBISElevator"),
            "dobBISBoilers": get_data("dobBISBoilers") or [],
            "dobJobs": get_data("dobJobs") or [],
            "dobNowPermits": get_data("dobNowPermits") or [],
            "acrisLegals": get_data("acrisLegals") or [],
            "acrisMasters": get_data("acrisMasters") or [],
            "acrisParties": get_data("acrisParties") or [],
            "nycSales": nyc_sales,
            "streeteasyComps": get_data("streeteasyComps") or [],
            "sameBuildingComps": get_data("sameBuildingComps") or [],
            "nycSalesDateWindow": nyc_sales_date_window,
            "dofExemptions": get_data("dofExemptions") or [],
            "dofAbatements": get_data("dofAbatements") or [],
            "taxLienSales": get_data("taxLienSales") or [],
            "dofFinancials": get_data("dofFinancials"),
            "sr311": get_data("sr311") or [],
            "nypdCrime": get_data("nypdCrime") or [],
            "floodZone": get_data("floodZone"),
            "envSites": get_data("envSites") or [],
            "certOccupancy": get_data("certOccupancy") or [],
            "zoningDistricts": get_data("zoningDistricts"),
            "landmarks": get_data("landmarks"),
            "energyBenchmark": get_data("energyBenchmarking"),
            "_manifest": manifest.model_dump(),
            "_fetchedAt": datetime.utcnow().isoformat(),
        }

        logger.info("[PIPELINE] Complete: %dms | %d ok / %d failed / %d skipped",
                     total_duration_ms, manifest.succeeded, manifest.failed, manifest.skipped)
        return raw_data

    except Exception as err:
        logger.error("[PIPELINE] Unhandled error: %s", err)
        raise
    finally:
        timeout_task.cancel()
        try:
            await timeout_task
        except asyncio.CancelledError:
            pass
