"""
Port of supabase/functions/_shared/render-report.ts

Fills the HTML chassis template with TransformedData (dict) + AI narratives.
Pure function -- no I/O, no side effects.
"""

from __future__ import annotations

import re
import time
from datetime import datetime, timezone

from app.pipeline.render.helpers import (
    escape_html,
    fmt_date,
    fmt_monthly,
    fmt_num,
    fmt_p,
    fmt_pct,
    fmt_ppsf,
    fmt_short,
    viol_color,
)
from app.pipeline.constants import borough_to_name, to_num
from app.pipeline.types import AINarratives


# ---------------------------------------------------------------------------
# Data Building
# ---------------------------------------------------------------------------

def build_data(d: dict, ai: AINarratives) -> dict:
    """Map TransformedData dict + AINarratives to template variables."""
    data: dict = {}

    now_year = datetime.now(tz=timezone.utc).year
    now_ts = time.time() * 1000  # millis, like Date.now()

    # ── Cover ──
    data["cover_address_line1"] = d.get("address", "")
    unit = d.get("unit", "")
    data["cover_address_line2"] = f"Apartment {unit}" if unit else ""
    neighborhood = d.get("neighborhood", "") or borough_to_name(d.get("borough", ""))
    data["cover_location"] = f"{neighborhood}, {borough_to_name(d.get('borough', ''))} {d.get('zip', '')}"

    # ── Section 1: Executive Summary ──
    comp_stats = d.get("compStats") or {}
    median_ppsf = comp_stats.get("medianPpsf") or 0
    price_per_sqft = d.get("pricePerSqft", 0) or 0
    vs_median_pct: float | None = (
        ((price_per_sqft - median_ppsf) / median_ppsf) * 100
        if median_ppsf > 0
        else None
    )
    vs_median_color = ""
    if vs_median_pct is not None:
        vs_median_color = "amber" if vs_median_pct > 0 else "green"

    total_open_violations = d.get("totalOpenViolations", 0) or 0
    year_built = d.get("yearBuilt")
    days_on_market = d.get("daysOnMarket")

    data["exec_metrics"] = [
        {
            "value": fmt_short(d.get("askingPrice")),
            "label": "Asking Price",
            "context": fmt_ppsf(price_per_sqft),
            "color": "",
        },
        {
            "value": fmt_short(comp_stats.get("median")),
            "label": "Comp Median",
            "context": f"{comp_stats['count']} comps" if comp_stats.get("count") else "",
            "color": "",
        },
        {
            "value": fmt_pct(vs_median_pct) if vs_median_pct is not None else "N/A",
            "label": "vs. Median",
            "context": (
                ("Above median" if vs_median_pct > 0 else "Below median")
                if vs_median_pct is not None
                else ""
            ),
            "color": vs_median_color,
        },
        {
            "value": str(total_open_violations),
            "label": "Open Violations",
            "context": "Across all agencies" if total_open_violations > 0 else "No open violations",
            "color": viol_color(total_open_violations),
        },
        {
            "value": str(days_on_market) if days_on_market else "N/A",
            "label": "Days on Market",
            "context": "",
            "color": "",
        },
        {
            "value": str(year_built) if year_built else "N/A",
            "label": "Year Built",
            "context": f"{now_year - year_built} years old" if year_built else "",
            "color": "",
        },
    ]

    # ── Section 2: The Unit ──
    beds = d.get("beds", 0)
    baths = d.get("baths", 0)
    sqft = d.get("sqft", 0) or 0
    common_charges = d.get("commonCharges") or 0
    property_taxes = d.get("propertyTaxes") or 0
    listing = d.get("listing") or {}

    data["unit_asking_price"] = fmt_p(d.get("askingPrice"))
    data["unit_size"] = f"{fmt_num(sqft)} sqft" if sqft > 0 else "N/A"
    data["unit_layout"] = f"{beds} BD / {baths} BA"
    data["unit_ppsf"] = fmt_ppsf(price_per_sqft)
    data["unit_common_charges"] = fmt_monthly(d.get("commonCharges"))
    data["unit_property_taxes"] = fmt_monthly(d.get("propertyTaxes"))
    data["unit_total_carry"] = fmt_monthly(common_charges + property_taxes)
    data["unit_days_on_market"] = f"{days_on_market} days" if days_on_market else "N/A"
    features = listing.get("features") or []
    data["unit_features"] = ", ".join(features) if features else ""
    data["unit_description"] = listing.get("description") or ""

    # ── Section 3: What It's Worth ──
    data["comp_median_ppsf"] = fmt_ppsf(median_ppsf) if median_ppsf > 0 else "N/A"
    data["comp_vs_median"] = fmt_pct(vs_median_pct) if vs_median_pct is not None else "N/A"
    data["comp_vs_median_color"] = vs_median_color
    data["comp_vs_median_context"] = (
        ("Above comp median" if vs_median_pct > 0 else "Below comp median")
        if vs_median_pct is not None
        else ""
    )

    comp_min = comp_stats.get("min")
    comp_max = comp_stats.get("max")
    sqft_denom = sqft if sqft else 1
    if comp_min and comp_max:
        low = f"${round(comp_min / sqft_denom):,}"
        high = f"${round(comp_max / sqft_denom):,}"
        min_ppsf = f"{low}\u2013{high}"
    else:
        min_ppsf = "N/A"

    comp_count = comp_stats.get("count") or 0
    data["comp_range_ppsf"] = min_ppsf if comp_count > 0 else "N/A"
    data["comp_count"] = str(comp_count) if comp_count else "0"

    # Build lookup from streeteasy comps for extra fields (beds, status)
    se_comp_map: dict[str, dict] = {}
    for c in d.get("streeteasyComps") or []:
        addr = c.get("address")
        if addr:
            se_comp_map[addr] = c

    sb_comp_map: dict[str, dict] = {}
    for c in d.get("sameBuildingComps") or []:
        addr = c.get("address")
        if addr:
            sb_comp_map[addr] = c

    unified_comps = d.get("unifiedComps") or {}
    comps_list = unified_comps.get("comps") or []

    data["comps"] = []
    for c in comps_list:
        is_subject = c.get("isSubject", False)
        c_addr = c.get("address", "")
        se = se_comp_map.get(c_addr)
        sb = sb_comp_map.get(c_addr)

        if is_subject:
            comp_beds = str(beds)
        elif se and se.get("beds") is not None:
            comp_beds = str(se["beds"])
        elif sb and sb.get("beds") is not None:
            comp_beds = str(sb["beds"])
        else:
            comp_beds = "\u2014"

        if is_subject:
            status = "Subject"
        else:
            status = (se.get("status") if se else None) or "Sold"

        if is_subject:
            status_color = "amber"
        elif status in ("Sold", "Closed"):
            status_color = "green"
        else:
            status_color = "muted"

        c_ppsf = c.get("pricePerSqft")
        ppsf_display = fmt_ppsf(c_ppsf).replace("/sqft", "") if c_ppsf else "\u2014"

        data["comps"].append({
            "address": f"<strong>{escape_html(c_addr)}</strong>" if is_subject else escape_html(c_addr),
            "price": fmt_p(c.get("price")),
            "price_per_sqft": ppsf_display,
            "beds": comp_beds,
            "sqft": fmt_num(c.get("sqft")) if c.get("sqft") else "\u2014",
            "status": status,
            "status_color": status_color,
            "row_class": "highlight-row" if is_subject else "",
        })

    # ── Section 4: The Building ──
    pluto = d.get("pluto") or {}
    energy_list = d.get("energyBenchmark") or []
    energy = energy_list[0] if energy_list else None
    landmarks = d.get("landmarks") or []

    data["bldg_year_built"] = str(year_built) if year_built else "N/A"
    data["bldg_age"] = f"{now_year - year_built} years old" if year_built else ""
    stories = d.get("stories")
    data["bldg_floors"] = str(stories) if stories else "N/A"
    units_res = d.get("unitsRes")
    data["bldg_units"] = str(units_res) if units_res else "N/A"
    data["bldg_zoning"] = pluto.get("zonedist1") or pluto.get("zoning") or "N/A"

    if energy:
        data["bldg_energy_grade"] = (
            energy.get("energy_star_score")
            if energy.get("energy_star_score")
            else (energy.get("letter_grade") or "N/A")
        )
        lg = energy.get("letter_grade", "")
        if lg in ("A", "B"):
            data["bldg_energy_color"] = "green"
        elif lg in ("D", "F"):
            data["bldg_energy_color"] = "red"
        else:
            data["bldg_energy_color"] = ""
    else:
        data["bldg_energy_grade"] = "N/A"
        data["bldg_energy_color"] = ""

    data["bldg_landmark"] = "Yes" if landmarks else "No"
    data["bldg_landmark_color"] = "amber" if landmarks else "green"
    data["bldg_landmark_context"] = (
        (landmarks[0].get("lpc_name") or "Designated landmark") if landmarks else "Not landmarked"
    )
    bldg_class = d.get("bldgClass", "")
    bldg_class_desc = d.get("bldgClassDesc", "")
    data["bldg_class"] = f"{bldg_class} \u2014 {bldg_class_desc}" if bldg_class_desc else bldg_class
    lot_area = pluto.get("lotarea")
    data["bldg_lot_area"] = f"{fmt_num(to_num(lot_area))} sqft" if lot_area else "N/A"
    assess_tot = pluto.get("assesstot")
    data["bldg_assessed_value"] = fmt_p(to_num(assess_tot)) if assess_tot else "N/A"
    cert_occ = d.get("certOccupancy") or []
    data["bldg_cert_occupancy"] = f"On file ({len(cert_occ)})" if cert_occ else "Not found"

    # ── Section 5: Who Runs the Building ──
    reg_expired = d.get("registrationExpired", False)
    hpd_reg = d.get("hpdRegistration") or []
    data["reg_status"] = "Expired" if reg_expired else ("Active" if hpd_reg else "Unknown")
    data["reg_status_color"] = "red" if reg_expired else ("green" if hpd_reg else "")
    reg_expiry = d.get("registrationExpiry")
    data["reg_expiry_context"] = f"Expires {reg_expiry}" if reg_expiry else ""
    data["reg_owner"] = d.get("registrationOwner") or "Unknown"
    data["reg_agent"] = d.get("managementAgent") or "Unknown"

    # ACRIS ownership history from neighborData units
    neighbor_data = d.get("neighborData") or {}
    nd_units = (neighbor_data.get("units") or [])[:10]
    data["acris_history"] = []
    for u in nd_units:
        purchase_price = u.get("purchasePrice")
        if purchase_price:
            cleaned = re.sub(r"[^0-9]", "", str(purchase_price))
            amount = fmt_p(to_num(cleaned)) if cleaned else "\u2014"
        else:
            amount = "\u2014"
        data["acris_history"].append({
            "date": u.get("date") or u.get("recordedDate") or "\u2014",
            "type": u.get("docType") or u.get("type") or "Deed",
            "party": u.get("owner") or u.get("party") or "\u2014",
            "amount": amount,
        })

    # HPD registered contacts
    data["contacts"] = []
    for c in d.get("hpdRegContacts") or []:
        first = c.get("firstname", "")
        last = c.get("lastname", "")
        name_parts = [p for p in (first, last) if p]
        name = " ".join(name_parts) if name_parts else (c.get("corporationname") or "\u2014")
        data["contacts"].append({
            "role": c.get("type") or "\u2014",
            "name": name,
            "phone": c.get("businessphone") or c.get("businesshousenumber") or "\u2014",
        })

    # ── Section 6: Financial Health ──
    data["fin_monthly_carry"] = fmt_monthly(common_charges + property_taxes)
    data["fin_carry_context"] = "CC + Tax"

    has_421a = d.get("has421a", False)
    has_j51 = d.get("hasJ51", False)
    dof_exemptions = d.get("dofExemptions") or []

    if has_421a:
        exemption_type = "421-a"
    elif has_j51:
        exemption_type = "J-51"
    elif dof_exemptions:
        exemption_type = "Other"
    else:
        exemption_type = "None"

    exemption_expiry = d.get("exemptionExpiry")

    data["fin_exemption_type"] = exemption_type
    data["fin_exemption_color"] = "amber" if exemption_type != "None" else "green"
    if exemption_expiry:
        data["fin_exemption_context"] = f"Expires {exemption_expiry}"
    elif exemption_type != "None":
        data["fin_exemption_context"] = "Active"
    else:
        data["fin_exemption_context"] = "No exemptions"

    tax_lien_sales = d.get("taxLienSales") or []
    data["fin_liens_count"] = str(len(tax_lien_sales))
    data["fin_liens_color"] = "red" if tax_lien_sales else "green"

    dof_fin = d.get("dofFinancials") or {}
    data["fin_expense_per_sqft"] = f"${dof_fin['expensePerSqft']}" if dof_fin.get("expensePerSqft") else "N/A"
    data["fin_gross_income"] = fmt_p(dof_fin.get("estimatedGrossIncome")) if dof_fin.get("estimatedGrossIncome") else "N/A"
    data["fin_expenses"] = fmt_p(dof_fin.get("estimatedExpense")) if dof_fin.get("estimatedExpense") else "N/A"
    data["fin_noi"] = fmt_p(dof_fin.get("netOperatingIncome")) if dof_fin.get("netOperatingIncome") else "N/A"
    data["fin_market_value"] = fmt_p(dof_fin.get("fullMarketValue")) if dof_fin.get("fullMarketValue") else "N/A"

    # Exemptions + abatements table
    exemptions: list[dict] = []
    for e in dof_exemptions:
        cls = e.get("exemption_classification") or "Unknown"
        end_date_str = e.get("exemption_end_date")
        is_expiring = False
        if end_date_str:
            try:
                end_ts = datetime.fromisoformat(end_date_str.replace("Z", "+00:00")).timestamp() * 1000
                is_expiring = end_ts < now_ts + 365 * 86_400_000
            except (ValueError, TypeError):
                pass

        if "421" in cls:
            badge_color = "amber"
        elif "j-51" in cls.lower() or "j51" in cls.lower():
            badge_color = "amber"
        else:
            badge_color = "green"

        exemptions.append({
            "type": "Exemption",
            "program": cls,
            "badge_color": badge_color,
            "annual_benefit": fmt_p(to_num(e.get("exemption_amount"))) if e.get("exemption_amount") else "\u2014",
            "expires": fmt_date(end_date_str) or "\u2014",
            "expires_style": "color:var(--red); font-weight:600;" if is_expiring else "",
        })

    dof_abatements = d.get("dofAbatements") or []
    for a in dof_abatements:
        exemptions.append({
            "type": "Abatement",
            "program": a.get("abatement_classification") or a.get("program") or "Unknown",
            "badge_color": "green",
            "annual_benefit": fmt_p(to_num(a.get("abatement_amount"))) if a.get("abatement_amount") else "\u2014",
            "expires": fmt_date(a.get("abatement_end_date")) or "\u2014",
            "expires_style": "",
        })
    data["exemptions"] = exemptions

    # Tax warning
    if has_421a and exemption_expiry:
        try:
            expiry_date = datetime.fromisoformat(exemption_expiry.replace("Z", "+00:00"))
            years_left = max(0, round((expiry_date.timestamp() - time.time()) / (365.25 * 86400)))
            if years_left <= 5:
                data["tax_warning"] = (
                    f"\u26a0 421-a tax exemption expires {exemption_expiry} "
                    f"({years_left} years). Property taxes will increase significantly after expiration."
                )
        except (ValueError, TypeError):
            pass
    elif has_j51 and exemption_expiry:
        try:
            expiry_date = datetime.fromisoformat(exemption_expiry.replace("Z", "+00:00"))
            years_left = max(0, round((expiry_date.timestamp() - time.time()) / (365.25 * 86400)))
            if years_left <= 3:
                data["tax_warning"] = (
                    f"\u26a0 J-51 tax abatement expires {exemption_expiry} "
                    f"({years_left} years). Expect higher taxes after expiration."
                )
        except (ValueError, TypeError):
            pass

    # ── Section 7: Violations & Compliance ──
    open_dob = d.get("openDobViolations") or []
    dob_viols = d.get("dobViolations") or []
    open_ecb = d.get("openEcb") or []
    dob_ecb = d.get("dobEcb") or []
    open_hpd = d.get("openHpdViolations") or []
    hpd_viols = d.get("hpdViolations") or []
    open_fdny = d.get("openFdnyViolations") or []
    fdny_viols = d.get("fdnyViolations") or []

    data["agencies"] = [
        {"name": "DOB", "open": str(len(open_dob)), "total": str(len(dob_viols)), "color": viol_color(len(open_dob))},
        {"name": "ECB", "open": str(len(open_ecb)), "total": str(len(dob_ecb)), "color": viol_color(len(open_ecb))},
        {"name": "HPD", "open": str(len(open_hpd)), "total": str(len(hpd_viols)), "color": viol_color(len(open_hpd))},
        {"name": "FDNY", "open": str(len(open_fdny)), "total": str(len(fdny_viols)), "color": viol_color(len(open_fdny))},
    ]

    open_hpd_lit = d.get("openHpdLitigation") or []
    hpd_complaints = d.get("hpdComplaints") or []
    data["viol_litigation"] = str(len(open_hpd_lit))
    data["viol_litigation_color"] = viol_color(len(open_hpd_lit))
    data["viol_open_complaints"] = str(len(hpd_complaints))

    # Build violations list from all sources
    violations: list[dict] = []
    for v in open_dob[:10]:
        violations.append({
            "badge_text": "DOB",
            "badge_color": "red",
            "date": fmt_date(v.get("issue_date")) or "\u2014",
            "description": v.get("violation_type_description") or v.get("description") or "DOB Violation",
            "meta": f"Violation #{v['violation_number']}" if v.get("violation_number") else "",
        })
    for v in open_ecb[:5]:
        ecb_num = v.get("ecb_violation_number")
        meta = ""
        if ecb_num:
            meta = f"ECB #{ecb_num}"
            if v.get("balance_due"):
                meta += f" \u2014 Penalty: ${v['balance_due']}"
        violations.append({
            "badge_text": "ECB",
            "badge_color": "red",
            "date": fmt_date(v.get("ecb_violation_date") or v.get("issue_date")) or "\u2014",
            "description": v.get("violation_type") or v.get("violation_description") or "ECB Violation",
            "meta": meta,
        })
    for v in open_hpd[:10]:
        violations.append({
            "badge_text": "HPD",
            "badge_color": "amber",
            "date": fmt_date(v.get("inspectiondate") or v.get("approveddate")) or "\u2014",
            "description": v.get("novdescription") or v.get("violationstatus") or "HPD Violation",
            "meta": f"Violation #{v['violationid']}" if v.get("violationid") else "",
        })
    for v in open_fdny[:3]:
        violations.append({
            "badge_text": "FDNY",
            "badge_color": "red",
            "date": fmt_date(v.get("inspection_date")) or "\u2014",
            "description": v.get("violation_description") or v.get("code_description") or "FDNY Violation",
            "meta": f"Balance due: ${v['balance_due']}" if v.get("balance_due") else "",
        })
    data["violations_items"] = violations

    # ── Section 8: Building Systems ──
    bis_elevator = d.get("dobBISElevator") or {}
    elevators = bis_elevator.get("records") if isinstance(bis_elevator, dict) else None
    if elevators is None:
        elevators = d.get("elevatorData") or []
    boilers = d.get("dobBISBoilers") or []
    if not boilers:
        boilers = d.get("boilerData") or []

    data["sys_elevator_count"] = str(len(elevators)) if isinstance(elevators, list) else "0"
    data["sys_elevator_context"] = (
        "Registered devices" if isinstance(elevators, list) and elevators else "No elevator data"
    )
    data["sys_boiler_count"] = str(len(boilers)) if isinstance(boilers, list) else "0"
    data["sys_boiler_context"] = (
        "Registered units" if isinstance(boilers, list) and boilers else "No boiler data"
    )

    # Elevator inspection status
    elev_passed = True
    if isinstance(elevators, list) and elevators:
        for e in elevators:
            status_str = (
                e.get("lastper_insp_disp")
                or e.get("last_inspection_disposition")
                or e.get("status")
                or ""
            ).upper()
            if "FAIL" in status_str or "DEFICIENCY" in status_str or "UNSAFE" in status_str:
                elev_passed = False
                break

    has_elevators = isinstance(elevators, list) and len(elevators) > 0
    data["sys_elevator_status"] = ("Pass" if elev_passed else "Deficiency") if has_elevators else "N/A"
    data["sys_elevator_status_color"] = ("green" if elev_passed else "red") if has_elevators else ""
    if has_elevators:
        first_elev = elevators[0]
        insp_date = fmt_date(first_elev.get("lastper_insp_date") or first_elev.get("last_inspection_date")) or "Unknown"
        data["sys_elevator_status_context"] = f"Last inspection: {insp_date}"
    else:
        data["sys_elevator_status_context"] = ""

    # Boiler status
    boiler_passed = True
    if isinstance(boilers, list) and boilers:
        for b in boilers:
            status_str = (b.get("defect") or b.get("status") or "").upper()
            if "DEFECT" in status_str or "FAIL" in status_str:
                boiler_passed = False
                break

    has_boilers = isinstance(boilers, list) and len(boilers) > 0
    data["sys_boiler_status"] = ("Pass" if boiler_passed else "Fail") if has_boilers else "N/A"
    data["sys_boiler_status_color"] = ("green" if boiler_passed else "red") if has_boilers else ""

    # Systems table
    system_rows: list[dict] = []
    if isinstance(elevators, list):
        for e in elevators:
            disp = e.get("lastper_insp_disp") or e.get("last_inspection_disposition") or e.get("status") or "Unknown"
            disp_upper = disp.upper()
            passed = "FAIL" not in disp_upper and "DEFICIENCY" not in disp_upper and "UNSAFE" not in disp_upper
            system_rows.append({
                "system": "Elevator",
                "id": e.get("device_number") or e.get("deviceNumber") or "\u2014",
                "last_inspection": fmt_date(e.get("lastper_insp_date") or e.get("last_inspection_date")) or "\u2014",
                "result": "Pass" if passed else "Deficiency",
                "result_color": "green" if passed else "red",
                "notes": e.get("approved_manufacturer") or e.get("devicetype") or "",
            })
    if isinstance(boilers, list):
        for b in boilers:
            defect = b.get("defect") or b.get("status") or ""
            defect_upper = defect.upper()
            passed = "DEFECT" not in defect_upper and "FAIL" not in defect_upper
            system_rows.append({
                "system": "Boiler",
                "id": b.get("report_number") or b.get("boilerId") or "\u2014",
                "last_inspection": fmt_date(b.get("inspection_date") or b.get("inspectionDate")) or "\u2014",
                "result": "Pass" if passed else "Fail",
                "result_color": "green" if passed else "red",
                "notes": defect,
            })
    data["systems"] = system_rows

    # ── Section 9: Work & Permits ──
    five_years_ago = now_ts - 5 * 365 * 86_400_000
    dob_jobs = d.get("dobJobs") or []

    def _parse_ts(date_str: str | None) -> float | None:
        if not date_str:
            return None
        try:
            return datetime.fromisoformat(date_str.replace("Z", "+00:00")).timestamp() * 1000
        except (ValueError, TypeError):
            return None

    recent_jobs = []
    for j in dob_jobs:
        filed = j.get("pre__filing_date") or j.get("latest_action_date")
        ts = _parse_ts(filed)
        if ts is not None and ts > five_years_ago:
            recent_jobs.append(j)

    dob_now_permits = d.get("dobNowPermits") or []
    active_permits = []
    for p in dob_now_permits:
        st = (p.get("job_status") or p.get("filing_status") or "").upper()
        if "SIGNED" not in st and "COMPLETE" not in st:
            active_permits.append(p)

    def _is_active_job(j: dict) -> bool:
        st = (j.get("job_status_descrp") or j.get("job_status") or "").upper()
        return "PROGRESS" in st or "APPROVED" in st or "PARTIAL" in st

    active_jobs = [j for j in recent_jobs if _is_active_job(j)]

    data["permits_active_count"] = str(len(active_permits) + len(active_jobs))
    data["permits_jobs_count"] = str(len(recent_jobs))

    # Largest active job
    if active_jobs:
        largest = active_jobs[0]
        data["permits_largest_type"] = largest.get("job_type") or "N/A"
        data["permits_largest_desc"] = (largest.get("job_description") or "")[:60]
    else:
        data["permits_largest_type"] = "None"
        data["permits_largest_desc"] = "No active jobs"

    # Permits table
    permit_rows: list[dict] = []
    for j in recent_jobs[:10]:
        st = j.get("job_status_descrp") or j.get("job_status") or "Unknown"
        st_upper = st.upper()
        is_complete = "SIGN" in st_upper or "COMPLETE" in st_upper
        permit_rows.append({
            "filed": fmt_date(j.get("pre__filing_date") or j.get("latest_action_date")) or "\u2014",
            "type": j.get("job_type") or "\u2014",
            "description": (j.get("job_description") or "")[:80] or "\u2014",
            "status": st,
            "status_color": "green" if is_complete else "amber",
        })
    for p in dob_now_permits[:5]:
        st = p.get("filing_status") or p.get("job_status") or "Unknown"
        st_upper = st.upper()
        is_complete = "SIGNED" in st_upper or "COMPLETE" in st_upper
        permit_rows.append({
            "filed": fmt_date(p.get("issued_date") or p.get("filing_date")) or "\u2014",
            "type": p.get("job_type") or "DOBNow",
            "description": (p.get("job_description") or p.get("work_type") or "")[:80] or "\u2014",
            "status": st,
            "status_color": "green" if is_complete else "amber",
        })
    data["permits"] = permit_rows

    # ── Section 10: Neighborhood ──
    sr311_recent = d.get("sr311Recent") or []
    sr311_by_type: dict = d.get("sr311ByType") or {}
    nypd_crime = d.get("nypdCrime") or []
    crime_by_category: dict = d.get("crimeByCategory") or {}

    data["hood_311_count"] = str(len(sr311_recent))
    sr311_sorted = sorted(sr311_by_type.items(), key=lambda x: x[1], reverse=True)
    data["hood_311_top"] = f"Top: {sr311_sorted[0][0]}" if sr311_sorted else ""

    data["hood_crime_count"] = str(len(nypd_crime))
    crime_sorted = sorted(crime_by_category.items(), key=lambda x: x[1], reverse=True)
    data["hood_crime_top"] = f"Top: {crime_sorted[0][0]}" if crime_sorted else ""

    # Flood zone
    fz = d.get("floodZone")
    if fz:
        zone = fz.get("zone", "N/A")
        zone_label = fz.get("zoneLabel", "")
        data["hood_flood_zone"] = zone
        is_minimal = zone in ("X", "0.2 PCT ANNUAL CHANCE")
        data["hood_flood_color"] = "green" if is_minimal else "red"
        data["hood_flood_context"] = zone_label or ("Minimal flood hazard" if zone == "X" else "")
        data["hood_flood_fema"] = f"Zone {zone}" + (f" \u2014 {zone_label}" if zone_label else "")
        data["hood_flood_value_color"] = "green" if is_minimal else "red"
        data["hood_flood_insurance"] = "No" if is_minimal else "Yes"
    else:
        data["hood_flood_zone"] = "N/A"
        data["hood_flood_color"] = ""
        data["hood_flood_context"] = ""
        data["hood_flood_fema"] = "N/A"
        data["hood_flood_value_color"] = ""
        data["hood_flood_insurance"] = "No"

    env_sites = d.get("envSites") or []
    data["hood_env_count"] = str(len(env_sites))
    data["hood_env_color"] = "amber" if env_sites else "green"
    data["hood_env_context"] = f"{len(env_sites)} nearby" if env_sites else "None found"
    data["hood_env_none"] = "true" if not env_sites else ""
    data["hood_env_list"] = "true" if env_sites else ""

    # 311 recent complaints table
    data["complaints_recent"] = []
    for s in sr311_recent[:5]:
        st = s.get("status") or "Unknown"
        st_upper = st.upper()
        data["complaints_recent"].append({
            "date": fmt_date(s.get("created_date")) or "\u2014",
            "type": s.get("complaint_type") or "\u2014",
            "description": s.get("descriptor") or s.get("resolution_description") or "\u2014",
            "status": st,
            "status_color": "amber" if st_upper in ("OPEN", "PENDING") else "green",
        })
    data["hood_311_showing"] = (
        f"Showing 5 of {len(sr311_recent)} complaints in the last 12 months."
        if len(sr311_recent) > 5
        else "All recent complaints shown."
    )

    # 311 by type table
    data["complaints_by_type"] = []
    for category, count in sr311_sorted[:10]:
        recent = [s for s in sr311_recent if s.get("complaint_type") == category]
        most_recent = fmt_date(recent[0].get("created_date")) or "\u2014" if recent else "\u2014"
        data["complaints_by_type"].append({
            "category": category,
            "count": str(count),
            "most_recent": most_recent,
        })

    # Crime recent table
    data["crime_recent"] = []
    for c in nypd_crime[:5]:
        data["crime_recent"].append({
            "date": fmt_date(c.get("cmplnt_fr_dt") or c.get("rpt_dt")) or "\u2014",
            "type": c.get("law_cat_cd") or c.get("ofns_desc") or "\u2014",
            "description": c.get("pd_desc") or c.get("ofns_desc") or "\u2014",
        })
    data["hood_crime_showing"] = (
        f"Showing 5 of {len(nypd_crime)} incidents."
        if len(nypd_crime) > 5
        else "All incidents shown."
    )

    # Crime by type table
    data["crime_by_type"] = []
    for category, count in crime_sorted[:10]:
        recent = [c for c in nypd_crime if (c.get("law_cat_cd") or c.get("ofns_desc")) == category]
        most_recent = fmt_date(recent[0].get("cmplnt_fr_dt") or recent[0].get("rpt_dt")) or "\u2014" if recent else "\u2014"
        data["crime_by_type"].append({
            "category": category,
            "count": str(count),
            "most_recent": most_recent,
        })

    # Env sites table
    data["env_sites"] = [
        {
            "name": s.get("program_facility_name") or s.get("site_name") or "\u2014",
            "program": s.get("program_type") or s.get("program") or "\u2014",
            "status": s.get("site_class") or s.get("cleanup_status") or "\u2014",
        }
        for s in env_sites
    ]

    # ── Section 11: Pros & Cons ──
    pros: list[dict[str, str]] = []
    for pf in ai.positive_factors or []:
        points = pf.get("points")
        if points:
            for pt in points:
                pros.append({"text": pt})
        elif pf.get("title"):
            pros.append({"text": pf["title"]})
    data["pros"] = pros if pros else [{"text": "No notable positives identified."}]

    cons: list[dict[str, str]] = []
    for rf in ai.red_flags or []:
        points = rf.get("points")
        if points:
            for pt in points:
                cons.append({"text": pt})
        elif rf.get("title"):
            cons.append({"text": rf["title"]})
    data["cons"] = cons if cons else [{"text": "No notable concerns identified."}]

    return data


# ---------------------------------------------------------------------------
# Template Engine
# ---------------------------------------------------------------------------

def fill_template(template: str, data: dict) -> str:
    """Custom template engine with 4 passes + cleanup."""
    html = template

    # 1. Expand {{#array}}...{{/array}} blocks
    def _expand_array(match: re.Match) -> str:
        key = match.group(1)
        inner = match.group(2)
        arr = data.get(key)
        if not isinstance(arr, list) or len(arr) == 0:
            return ""
        parts: list[str] = []
        for item in arr:
            row = inner
            # Replace {{{.field}}} with raw item values (no escaping)
            row = re.sub(
                r"\{\{\{\.(\w+)\}\}\}",
                lambda m: str(item.get(m.group(1), "")) if item.get(m.group(1)) is not None else "",
                row,
            )
            # Replace {{.field}} with escaped item values
            row = re.sub(
                r"\{\{\.(\w+)\}\}",
                lambda m: escape_html(str(item.get(m.group(1), ""))) if item.get(m.group(1)) is not None else "",
                row,
            )
            # Expand conditionals inside array items: {{?.field}}...{{/field}}
            row = re.sub(
                r"\{\{\?\.(\w+)\}\}([\s\S]*?)\{\{/\1\}\}",
                lambda m: m.group(2) if item.get(m.group(1)) else "",
                row,
            )
            parts.append(row)
        return "".join(parts)

    html = re.sub(r"\{\{#(\w+)\}\}([\s\S]*?)\{\{/\1\}\}", _expand_array, html)

    # 2. Expand {{?field}}...{{/field}} conditionals
    def _expand_cond(match: re.Match) -> str:
        key = match.group(1)
        inner = match.group(2)
        val = data.get(key)
        if not val or (isinstance(val, str) and len(val) == 0):
            return ""
        return inner

    html = re.sub(r"\{\{\?(\w+)\}\}([\s\S]*?)\{\{/\1\}\}", _expand_cond, html)

    # 3. Replace {{{raw}}} -- no escaping
    def _raw(match: re.Match) -> str:
        val = data.get(match.group(1))
        return str(val) if val is not None else ""

    html = re.sub(r"\{\{\{(\w+)\}\}\}", _raw, html)

    # 4. Replace {{value}} -- with HTML escaping
    def _escaped(match: re.Match) -> str:
        val = data.get(match.group(1))
        if val is None:
            return ""
        if isinstance(val, (dict, list)):
            return ""
        return escape_html(str(val))

    html = re.sub(r"\{\{(\w+)\}\}", _escaped, html)

    # 5. Clean up any remaining {{...}}
    html = re.sub(r"\{\{[^}]*\}\}", "", html)

    return html


# ---------------------------------------------------------------------------
# Main Entry Point
# ---------------------------------------------------------------------------

def render_report(d: dict, ai: AINarratives, template: str) -> str:
    """Fill the HTML chassis template with data from TransformedData + AI narratives.

    Returns populated HTML string ready for PDF conversion.
    """
    data = build_data(d, ai)
    return fill_template(template, data)
