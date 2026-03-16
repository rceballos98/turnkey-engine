"""Pure data transformation: raw API responses -> normalized TransformedData."""

from __future__ import annotations

import time

from app.pipeline.constants import to_num, safe_divide, to_date, borough_to_name
from app.pipeline.transform.acris import transform_acris_to_neighbor_units
from app.pipeline.transform.comps import build_unified_comps


def transform_raw_data(raw: dict, report_address: str) -> dict:
    parsed = raw.get("parsed") or {}
    pluto = raw.get("pluto")
    listing = raw.get("listing") or {}

    # Handle parsed as either dict or ParsedAddress model dump
    p_house = parsed.get("house_number") or parsed.get("houseNumber") or ""
    p_street = parsed.get("street") or ""
    p_unit = parsed.get("unit") or ""
    p_borough = parsed.get("borough") or "MN"
    p_zip = parsed.get("zip") or ""
    p_city = parsed.get("city") or ""

    address = report_address or f"{p_house} {p_street}".strip()
    unit = p_unit
    borough = p_borough
    zip_code = p_zip or (pluto.get("zipcode", "") if pluto else "")
    bbl = pluto.get("bbl", "") if pluto else ""
    bin_number = (raw.get("dobBISProfile") or {}).get("bin", "")
    year_built = to_num(pluto.get("yearbuilt") if pluto else None) or to_num(listing.get("yearBuilt")) or 0
    stories = to_num(pluto.get("numfloors") if pluto else None) or 0
    units_res = to_num(pluto.get("unitsres") if pluto else None) or 0
    bldg_class = (pluto.get("bldgclass", "") if pluto else "")
    bldg_class_desc = (pluto.get("bldgclassdesc") or pluto.get("landuse", "") if pluto else "")
    neighborhood = borough_to_name(borough) if (pluto and pluto.get("zipcode")) else (p_city or "")

    asking_price = to_num(listing.get("price")) or 0
    sqft = to_num(listing.get("sqft")) or 0
    beds = to_num(listing.get("beds")) or 0
    baths = to_num(listing.get("baths")) or 0
    price_per_sqft = safe_divide(asking_price, sqft) or 0
    days_on_market = to_num(listing.get("daysOnMarket")) or 0
    common_charges = to_num(listing.get("commonCharges")) or 0
    property_taxes = to_num(listing.get("propertyTaxes")) or 0

    # Violations
    dob_violations = raw.get("dobViolations") or []
    open_dob = [v for v in dob_violations if "*" not in (v.get("violation_category") or "")]
    dob_ecb = raw.get("dobEcb") or []
    open_ecb = [v for v in dob_ecb if (v.get("ecb_violation_status") or "").upper() in ("ACTIVE", "DEFAULT")]
    hpd_violations = raw.get("hpdViolations") or []
    open_hpd = [v for v in hpd_violations if (v.get("currentstatus") or "").upper() != "CLOSE"]
    hpd_complaints = raw.get("hpdComplaints") or []
    hpd_complaints_by_cat: dict[str, int] = {}
    for c in hpd_complaints:
        cat = c.get("major_category") or c.get("minor_category") or "Other"
        hpd_complaints_by_cat[cat] = hpd_complaints_by_cat.get(cat, 0) + 1
    hpd_litigation = raw.get("hpdLitigation") or []
    open_hpd_lit = [l for l in hpd_litigation if (l.get("casestatus") or "").upper() != "CLOSED"]
    fdny_violations = raw.get("fdnyViolations") or []
    open_fdny = [v for v in fdny_violations if (to_num(v.get("balance_due")) or 0) > 0]
    total_open = len(open_dob) + len(open_ecb) + len(open_hpd) + len(open_fdny)

    # HPD Registration
    hpd_reg = raw.get("hpdRegistration")
    hpd_registration = [hpd_reg] if isinstance(hpd_reg, dict) else (hpd_reg if isinstance(hpd_reg, list) else [])
    hpd_reg_contacts = raw.get("hpdRegContacts") or []
    reg_owner = ""
    mgmt_agent = ""
    reg_expiry = ""
    reg_expired = False
    if hpd_registration:
        reg = hpd_registration[0]
        reg_expiry = to_date(reg.get("registrationenddate")) or ""
        if reg_expiry:
            try:
                from datetime import datetime
                reg_expired = datetime.fromisoformat(reg_expiry).timestamp() * 1000 < time.time() * 1000
            except Exception:
                pass
    for c in hpd_reg_contacts:
        ctype = (c.get("type") or "").upper()
        if ctype in ("CORPORATEOWNER", "INDIVIDUALOWNER", "HEADOFFICER") and not reg_owner:
            reg_owner = " ".join(filter(None, [c.get("firstname"), c.get("lastname")])) or c.get("corporationname", "")
        if ctype in ("AGENT", "SITEMANAGER") and not mgmt_agent:
            mgmt_agent = " ".join(filter(None, [c.get("firstname"), c.get("lastname")])) or c.get("corporationname", "")

    # Tax
    dof_exemptions = raw.get("dofExemptions") or []
    dof_abatements = raw.get("dofAbatements") or []
    tax_lien_sales = raw.get("taxLienSales") or []
    has_421a = any("421" in (e.get("exemption_classification") or "") for e in dof_exemptions)
    has_j51 = any(k in (e.get("exemption_classification") or "").lower() for e in dof_exemptions for k in ["j-51", "j51"])
    exemption_expiry = to_date(dof_exemptions[0].get("exemption_end_date")) if dof_exemptions else ""

    # 311 / Crime
    sr311 = raw.get("sr311") or []
    sr311_by_type: dict[str, int] = {}
    one_year_ago_ms = time.time() * 1000 - 365 * 24 * 60 * 60 * 1000
    sr311_recent = []
    for s in sr311:
        stype = s.get("complaint_type") or "Other"
        sr311_by_type[stype] = sr311_by_type.get(stype, 0) + 1
        date_str = to_date(s.get("created_date"))
        if date_str:
            try:
                from datetime import datetime
                ts = datetime.fromisoformat(date_str).timestamp() * 1000
                if ts > one_year_ago_ms:
                    sr311_recent.append(s)
            except Exception:
                pass

    nypd_crime = raw.get("nypdCrime") or []
    crime_by_cat: dict[str, int] = {}
    for c in nypd_crime:
        cat = c.get("law_cat_cd") or c.get("ofns_desc") or "Other"
        crime_by_cat[cat] = crime_by_cat.get(cat, 0) + 1

    # Sales & Comps
    nyc_sales = raw.get("nycSales") or []
    valid_sales = [s for s in nyc_sales if (to_num(s.get("sale_price")) or 0) > 10]
    se_comps = raw.get("streeteasyComps") or []
    streeteasy_comps = []
    for c in se_comps:
        c2 = dict(c)
        c2["pricePerSqft"] = to_num(c.get("pricePerSqft")) or safe_divide(to_num(c.get("price")), to_num(c.get("sqft")))
        streeteasy_comps.append(c2)

    same_building_comps = raw.get("sameBuildingComps") or []

    comp_source = streeteasy_comps if len(streeteasy_comps) >= 3 else valid_sales
    comp_prices = sorted([p for p in (to_num(c.get("price")) or to_num(c.get("sale_price")) or 0 for c in comp_source) if p > 0])
    comp_ppsf = sorted([p for p in (to_num(c.get("pricePerSqft")) or 0 for c in streeteasy_comps) if p > 0])
    comp_stats = {
        "count": len(comp_prices),
        "median": comp_prices[len(comp_prices) // 2] if comp_prices else 0,
        "min": comp_prices[0] if comp_prices else 0,
        "max": comp_prices[-1] if comp_prices else 0,
        "avg": round(sum(comp_prices) / len(comp_prices)) if comp_prices else 0,
        "medianPpsf": comp_ppsf[len(comp_ppsf) // 2] if comp_ppsf else 0,
    }

    # Neighbor units
    neighbor_data = transform_acris_to_neighbor_units(
        raw.get("acrisLegals") or [], raw.get("acrisMasters") or [],
        raw.get("acrisParties") or [], pluto.get("lot", "") if pluto else "",
    )

    # Unified comps
    partial = {
        "streeteasyComps": streeteasy_comps, "sameBuildingComps": same_building_comps,
        "validSales": valid_sales, "address": address, "unit": unit,
        "askingPrice": asking_price, "sqft": sqft, "pricePerSqft": price_per_sqft,
        "pluto": pluto, "listing": listing,
    }
    unified_comps = build_unified_comps(partial)

    landmarks_raw = raw.get("landmarks")
    landmarks = [landmarks_raw] if isinstance(landmarks_raw, dict) else (landmarks_raw if isinstance(landmarks_raw, list) else [])
    energy_raw = raw.get("energyBenchmark")
    energy_benchmark = [energy_raw] if isinstance(energy_raw, dict) else (energy_raw if isinstance(energy_raw, list) else [])

    return {
        "address": address, "unit": unit, "borough": borough, "neighborhood": neighborhood,
        "zip": zip_code, "bbl": bbl, "bin": bin_number,
        "yearBuilt": year_built, "stories": stories, "unitsRes": units_res,
        "bldgClass": bldg_class, "bldgClassDesc": bldg_class_desc,
        "askingPrice": asking_price, "sqft": sqft, "beds": beds, "baths": baths,
        "pricePerSqft": price_per_sqft, "daysOnMarket": days_on_market,
        "commonCharges": common_charges, "propertyTaxes": property_taxes,
        "listing": listing,
        "dobViolations": dob_violations, "openDobViolations": open_dob,
        "dobEcb": dob_ecb, "openEcb": open_ecb,
        "hpdViolations": hpd_violations, "openHpdViolations": open_hpd,
        "hpdComplaints": hpd_complaints, "hpdComplaintsByCategory": hpd_complaints_by_cat,
        "hpdLitigation": hpd_litigation, "openHpdLitigation": open_hpd_lit,
        "fdnyViolations": fdny_violations, "openFdnyViolations": open_fdny,
        "totalOpenViolations": total_open,
        "hpdRegistration": hpd_registration, "hpdRegContacts": hpd_reg_contacts,
        "registrationOwner": reg_owner, "managementAgent": mgmt_agent,
        "registrationExpiry": reg_expiry, "registrationExpired": reg_expired,
        "dofExemptions": dof_exemptions, "dofAbatements": dof_abatements,
        "taxLienSales": tax_lien_sales, "has421a": has_421a, "hasJ51": has_j51,
        "exemptionExpiry": exemption_expiry or "",
        "sr311": sr311, "sr311ByType": sr311_by_type, "sr311Recent": sr311_recent,
        "nypdCrime": nypd_crime, "crimeByCategory": crime_by_cat,
        "elevatorData": raw.get("elevatorData") or [],
        "boilerData": raw.get("boilerData") or [],
        "dobBISProfile": raw.get("dobBISProfile"),
        "dobBISBoilers": raw.get("dobBISBoilers") or [],
        "dobBISElevator": raw.get("dobBISElevator"),
        "certOccupancy": raw.get("certOccupancy") or [],
        "zoningDistricts": raw.get("zoningDistricts"),
        "landmarks": landmarks,
        "energyBenchmark": energy_benchmark,
        "dobJobs": raw.get("dobJobs") or [],
        "dobNowPermits": raw.get("dobNowPermits") or [],
        "dobComplaints": raw.get("dobComplaints") or [],
        "floodZone": raw.get("floodZone"),
        "envSites": raw.get("envSites") or [],
        "nycSales": nyc_sales, "validSales": valid_sales,
        "compStats": comp_stats, "streeteasyComps": streeteasy_comps,
        "sameBuildingComps": same_building_comps,
        "nycSalesDateWindow": raw.get("nycSalesDateWindow", "2 years"),
        "neighborData": neighbor_data,
        "dofFinancials": raw.get("dofFinancials"),
        "pluto": pluto, "parsed": parsed,
        "unifiedComps": unified_comps,
    }
