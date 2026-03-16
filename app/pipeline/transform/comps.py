"""Merge StreetEasy + same-building + DOF sales into unified comps table."""

from __future__ import annotations

from app.pipeline.constants import to_num, safe_divide


def build_unified_comps(d: dict) -> dict:
    comps: list[dict] = []

    # Add StreetEasy comps
    for c in (d.get("streeteasyComps") or []):
        price = to_num(c.get("price")) or 0
        sqft_val = to_num(c.get("sqft"))
        comps.append({
            "address": c.get("address", "Unknown"),
            "price": price,
            "sqft": sqft_val,
            "pricePerSqft": to_num(c.get("pricePerSqft")) or safe_divide(price, sqft_val),
            "isSubject": False,
        })

    # Add same-building comps
    for c in (d.get("sameBuildingComps") or []):
        addr = c.get("address", "Same Building")
        if any(e["address"] == addr for e in comps):
            continue
        price = to_num(c.get("price")) or to_num(c.get("askingPrice")) or 0
        sqft_val = to_num(c.get("sqft"))
        comps.append({
            "address": addr,
            "price": price,
            "sqft": sqft_val,
            "pricePerSqft": to_num(c.get("pricePerSqft")) or safe_divide(price, sqft_val),
            "isSubject": False,
        })

    # Add DOF sales
    for s in (d.get("validSales") or []):
        sp = to_num(s.get("sale_price")) or 0
        if sp <= 10:
            continue
        addr = s.get("address") or f"{s.get('neighborhood', '')} {s.get('building_class_at_time_of_sale', '')}".strip()
        if any(e["address"] == addr for e in comps):
            continue
        comps.append({
            "address": addr,
            "price": sp,
            "sqft": None,
            "pricePerSqft": None,
            "isSubject": False,
        })

    # Add subject property
    subject_comp = {
        "address": f"{d.get('address', '')}{', ' + d['unit'] if d.get('unit') else ''}",
        "price": to_num(d.get("askingPrice")) or 0,
        "sqft": to_num(d.get("sqft")),
        "pricePerSqft": to_num(d.get("pricePerSqft")),
        "isSubject": True,
    }

    insufficient_comps = len(comps) == 0

    # Sort by $/sqft
    with_ppsf = sorted(
        [c for c in comps if c.get("pricePerSqft") and c["pricePerSqft"] > 0],
        key=lambda c: c["pricePerSqft"], reverse=True,
    )
    without_ppsf = [c for c in comps if not c.get("pricePerSqft") or c["pricePerSqft"] == 0]

    if subject_comp["pricePerSqft"] and subject_comp["pricePerSqft"] > 0:
        insert_idx = next(
            (i for i, c in enumerate(with_ppsf) if (c["pricePerSqft"] or 0) < (subject_comp["pricePerSqft"] or 0)),
            len(with_ppsf),
        )
        with_ppsf.insert(insert_idx, subject_comp)
        all_comps = with_ppsf + without_ppsf
    else:
        all_comps = with_ppsf + [subject_comp] + without_ppsf

    subject_index = next((i for i, c in enumerate(all_comps) if c["isSubject"]), -1)
    return {"comps": all_comps, "subjectIndex": subject_index, "insufficientComps": insufficient_comps}
