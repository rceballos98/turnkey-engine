"""Claude Sonnet -> 12 narrative fields for the report."""

from __future__ import annotations

import json
import logging

import anthropic

from app.config import settings
from app.pipeline.constants import to_num, to_date, fmt_p, fmt_date, borough_to_name
from app.pipeline.types import AINarratives

logger = logging.getLogger(__name__)


def fallback_narratives() -> AINarratives:
    return AINarratives()


def _parse_ai_response(raw_text: str) -> dict | None:
    try:
        return json.loads(raw_text)
    except json.JSONDecodeError:
        cleaned = raw_text.replace("```json\n", "").replace("```\n", "").replace("```", "").strip()
        try:
            return json.loads(cleaned)
        except json.JSONDecodeError:
            logger.error("[AI] JSON parse failed after cleanup")
            return None


def _build_prompt(d: dict) -> str:
    neighbor_summary = ""
    if d.get("neighborData", {}).get("units"):
        lines = []
        for u in d["neighborData"]["units"]:
            details = "; ".join(u.get("details", []))
            lines.append(f"  - {u['unit']}: Owner={u['owner']}, Price={u['purchasePrice']}" +
                         (f", {details}" if details else ""))
        neighbor_summary = "\n".join(lines)
    else:
        neighbor_summary = "  No neighbor unit data from ACRIS."

    open_dob_details = "\n".join(
        f"  - {v.get('violation_number', '?')}: {v.get('violation_type_description') or v.get('description', 'Unknown')} (issued {fmt_date(v.get('issue_date')) or '?'})"
        for v in d.get("openDobViolations", [])[:10]
    )

    open_ecb_details = "; ".join(
        f"{v.get('ecb_violation_number', '')}: {v.get('violation_type', '')} ({v.get('ecb_violation_status', '')}, penalty: ${v.get('balance_due') or v.get('penality_imposed') or 0})"
        for v in d.get("openEcb", [])[:5]
    )

    comps_summary = "\n".join(
        f"  - {c.get('address', '?')}: ${c.get('price', 0):,} ({c.get('status', 'closed')}), {c.get('beds', '?')}BR, {c.get('sqft', '?')} sqft"
        + (f", ${c['pricePerSqft']}/sqft" if c.get('pricePerSqft') else "")
        for c in d.get("streeteasyComps", [])
    ) or "  No comparable closed sales from StreetEasy."

    dof_sales_summary = "\n".join(
        f"  - {s.get('address', '?')}: ${(to_num(s.get('sale_price')) or 0):,.0f} ({fmt_date(s.get('sale_date')) or '?'})"
        for s in d.get("validSales", [])[:10]
    ) or "  No DOF closed sales."

    cs = d.get("compStats", {})
    comp_stats_text = f"  Stats: {cs.get('count', 0)} comps, median ${cs.get('median', 0):,}, range ${cs.get('min', 0):,}-${cs.get('max', 0):,}" + \
        (f", median ${cs['medianPpsf']}/sqft" if cs.get('medianPpsf') else "") if cs.get("count", 0) > 0 else ""

    same_bldg = "\n".join(
        f"  - {c.get('address', '?')}: ${c.get('price', 0):,} asking, {c.get('beds', '?')}BR, {c.get('sqft', '?')} sqft"
        for c in d.get("sameBuildingComps", [])
    ) or "  No same-building listings."

    sr311_sorted = sorted(d.get("sr311ByType", {}).items(), key=lambda x: x[1], reverse=True)[:10]
    sr311_summary = "\n".join(f"  - {t}: {c}" for t, c in sr311_sorted) or "  No 311 complaints."

    crime_sorted = sorted(d.get("crimeByCategory", {}).items(), key=lambda x: x[1], reverse=True)[:10]
    crime_summary = "\n".join(f"  - {t}: {c}" for t, c in crime_sorted) or "  No crime data."

    hpd_sorted = sorted(d.get("hpdComplaintsByCategory", {}).items(), key=lambda x: x[1], reverse=True)[:10]
    hpd_summary = "\n".join(f"  - {t}: {c}" for t, c in hpd_sorted) or "  No HPD complaints."

    exemption_summary = "\n".join(
        f"  - {e.get('exemption_classification', 'Unknown')} (ends {fmt_date(e.get('exemption_end_date')) or '?'})"
        for e in d.get("dofExemptions", [])[:5]
    ) or "  No exemptions found."

    comp_addresses = "\n".join(
        f"  - {c['address']}: ${c['price']:,}" +
        (f", {c['sqft']} sqft" if c.get("sqft") else "") +
        (f", ${c['pricePerSqft']}/sqft" if c.get("pricePerSqft") else "")
        for c in d.get("unifiedComps", {}).get("comps", []) if not c.get("isSubject")
    )[:15]

    dof = d.get("dofFinancials")
    dof_text = f"""- Report Year: {dof.get('reportYear')}
- Estimated Gross Income: ${(dof.get('estimatedGrossIncome') or 0):,}
- Estimated Expense: ${(dof.get('estimatedExpense') or 0):,}
- Expense/sqft: ${dof.get('expensePerSqft') or 'N/A'}
- Net Operating Income: ${(dof.get('netOperatingIncome') or 0):,}
- Full Market Value: ${(dof.get('fullMarketValue') or 0):,}""" if dof else "No DOF financial data available."

    pluto = d.get("pluto") or {}

    return f"""You are a senior real estate due diligence analyst specializing in NYC properties. Based on ALL the following data about a property at {d.get('address', '')}{' #' + d['unit'] if d.get('unit') else ''}, generate a comprehensive JSON object.

IMPORTANT: Respond ONLY with valid JSON. No markdown fences, no explanations outside the JSON.
IMPORTANT: Be factual and neutral. Do NOT provide verdicts, recommendations, offer strategies, or advisory opinions.

Required JSON fields:

1. "executiveSummary" - A vivid 3-4 sentence description of the property.
2. "keyFindings" - Array of 5-8 objects with "type" ("positive"|"negative"|"neutral") and "text".
3. "violationNarrative" - A thorough paragraph analyzing ALL violations.
4. "registrationNarrative" - A paragraph about ownership/management structure.
5. "taxNarrative" - A paragraph about tax exemptions/abatements.
6. "neighborhoodNarrative" - A paragraph about quality of life based on 311/crime data.
7. "redFlags" - Array of 3-8 objects with "title" and "points" (string[], 2-4 each).
8. "positiveFactors" - Array of 3-8 objects with "title" and "points" (string[], 2-3 each).
9. "comparableAnalysis" - Array of 2-4 string paragraphs analyzing comps vs the subject.
10. "neighborProfileNarrative" - A paragraph about ownership patterns from ACRIS.
11. "workPermitObservations" - Array of 3-5 string observations.
12. "compComments" - Array of objects with "address" and "comment".

=== DATA ===

LISTING: {d.get('listing', {}).get('description', 'No listing description.')}
Features: {', '.join(d.get('listing', {}).get('features', [])) or 'None'}

PROPERTY: {d.get('address', '')} Unit {d.get('unit', '')} | {d.get('neighborhood', '')}, {borough_to_name(d.get('borough', 'MN'))}
Year Built: {d.get('yearBuilt') or 'Unknown'} | Stories: {d.get('stories') or 'Unknown'} | Units: {d.get('unitsRes') or 'Unknown'}
Class: {d.get('bldgClass', '')} | BBL: {d.get('bbl', '')} | Lot: {pluto.get('lotarea', 'Unknown')} sqft

UNIT: Price: {fmt_p(d.get('askingPrice'))} | {d.get('sqft') or 'Unknown'} sqft | {d.get('beds')}BD/{d.get('baths')}BA | ${d.get('pricePerSqft') or 'N/A'}/sqft
DOM: {d.get('daysOnMarket') or 'Unknown'} | CC: ${d.get('commonCharges') or 'N/A'}/mo | Tax: ${d.get('propertyTaxes') or 'N/A'}/mo

VIOLATIONS:
- DOB: {len(d.get('dobViolations', []))} ({len(d.get('openDobViolations', []))} open)
- ECB: {len(d.get('dobEcb', []))} ({len(d.get('openEcb', []))} open)
- HPD: {len(d.get('hpdViolations', []))} ({len(d.get('openHpdViolations', []))} open)
- HPD Complaints: {len(d.get('hpdComplaints', []))} by category:
{hpd_summary}
- HPD Litigation: {len(d.get('hpdLitigation', []))} ({len(d.get('openHpdLitigation', []))} open)
- FDNY: {len(d.get('fdnyViolations', []))} ({len(d.get('openFdnyViolations', []))} open)
{('- Open ECB: ' + open_ecb_details) if d.get('openEcb') else ''}
{('- Open DOB:\n' + open_dob_details) if open_dob_details else ''}

HPD REG: Owner={d.get('registrationOwner') or 'Unknown'} | Agent={d.get('managementAgent') or 'Unknown'} | Expiry={d.get('registrationExpiry') or 'Unknown'}{'(EXPIRED)' if d.get('registrationExpired') else ''}

TAX: {exemption_summary}
421-a: {'YES' if d.get('has421a') else 'No'} | J-51: {'YES' if d.get('hasJ51') else 'No'} | Liens: {len(d.get('taxLienSales', []))}

DOF FINANCIALS: {dof_text}

PERMITS: {len(d.get('dobJobs', []))} jobs + {len(d.get('dobNowPermits', []))} DOBNow

311 ({len(d.get('sr311', []))} total, {len(d.get('sr311Recent', []))} recent):
{sr311_summary}

CRIME ({len(d.get('nypdCrime', []))} incidents):
{crime_summary}

ELEVATOR: {len((d.get('dobBISElevator') or {}).get('records', d.get('elevatorData', [])))} devices
BOILER: {len(d.get('dobBISBoilers') or d.get('boilerData', []))} records
FLOOD: {d.get('floodZone', {}).get('zone', 'N/A') if d.get('floodZone') else 'N/A'} - {d.get('floodZone', {}).get('zoneLabel', '') if d.get('floodZone') else ''}
ENV: {', '.join(s.get('program_facility_name', '') for s in d.get('envSites', [])) or 'None nearby'}

ACRIS NEIGHBORS:
{neighbor_summary}
{('CRITICAL: ' + '; '.join(d.get('neighborData', {}).get('criticalFindings', []))) if d.get('neighborData', {}).get('criticalFindings') else ''}

SAME BUILDING: {same_bldg}

COMPS:
{comp_addresses}
{comp_stats_text}

DOF SALES (last {d.get('nycSalesDateWindow', '2 years')}):
{dof_sales_summary}

Respond ONLY with valid JSON."""


async def generate_ai_narratives(d: dict) -> tuple[AINarratives, bool]:
    """Returns (narratives, failed). failed=True triggers fallback indicators."""
    if not settings.anthropic_api_key:
        logger.warning("[AI] No ANTHROPIC_API_KEY — using fallback narratives")
        return fallback_narratives(), True

    prompt = _build_prompt(d)
    logger.info("[AI] Sending prompt: %d chars", len(prompt))

    try:
        client = anthropic.AsyncAnthropic(api_key=settings.anthropic_api_key)
        response = await client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=6144,
            messages=[{"role": "user", "content": prompt}],
        )

        text = response.content[0].text if response.content else ""
        logger.info("[AI] Response: %d chars, input_tokens=%s, output_tokens=%s",
                     len(text), response.usage.input_tokens, response.usage.output_tokens)

        parsed = _parse_ai_response(text)
        if not parsed:
            return fallback_narratives(), True

        return AINarratives(
            executive_summary=parsed.get("executiveSummary", "Detailed analysis not available for this section."),
            key_findings=parsed.get("keyFindings", []),
            violation_narrative=parsed.get("violationNarrative", "Detailed analysis not available for this section."),
            registration_narrative=parsed.get("registrationNarrative", "Detailed analysis not available for this section."),
            tax_narrative=parsed.get("taxNarrative", "Detailed analysis not available for this section."),
            neighborhood_narrative=parsed.get("neighborhoodNarrative", "Detailed analysis not available for this section."),
            red_flags=parsed.get("redFlags", []),
            positive_factors=parsed.get("positiveFactors", []),
            comparable_analysis=parsed.get("comparableAnalysis", []),
            neighbor_profile_narrative=parsed.get("neighborProfileNarrative", "Detailed analysis not available for this section."),
            work_permit_observations=parsed.get("workPermitObservations", []),
            comp_comments=parsed.get("compComments", []),
        ), False
    except Exception as e:
        logger.error("[AI] Exception: %s", e)
        return fallback_narratives(), True
