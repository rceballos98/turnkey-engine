"""ACRIS legals/masters/parties -> per-unit ownership profiles."""

from __future__ import annotations

from app.pipeline.constants import to_num, to_date


def transform_acris_to_neighbor_units(
    acris_legals: list, acris_masters: list, acris_parties: list, subject_lot: str,
) -> dict:
    if not acris_legals:
        return {"units": [], "criticalFindings": []}

    lot_map: dict[str, list] = {}
    for legal in acris_legals:
        lot = (legal.get("lot") or "").lstrip("0") or ""
        if not lot:
            continue
        lot_num = to_num(lot)
        if lot_num is None or lot_num < 1000:
            continue
        lot_map.setdefault(lot, []).append(legal)

    critical_findings: list[str] = []
    units: list[dict] = []
    normalized_subject = (subject_lot or "").lstrip("0") or ""

    sorted_lots = sorted(lot_map.keys(), key=lambda x: (x != normalized_subject, to_num(x) or 0))

    for lot in sorted_lots:
        legals = lot_map[lot]
        doc_ids = list({l.get("document_id") for l in legals if l.get("document_id")})

        docs = []
        for doc_id in doc_ids:
            master = next((m for m in acris_masters if m.get("document_id") == doc_id), None)
            parties = [p for p in acris_parties if p.get("document_id") == doc_id]
            docs.append({"docId": doc_id, "master": master, "parties": parties})

        deed_docs = sorted(
            [d for d in docs if d["master"] and "DEED" in (d["master"].get("doc_type") or "").upper()],
            key=lambda d: d["master"].get("recorded_datetime") or "", reverse=True,
        )

        latest_deed = deed_docs[0] if deed_docs else None

        mortgage_docs = sorted(
            [d for d in docs if d["master"] and any(t in (d["master"].get("doc_type") or "").upper() for t in ["MTGE", "MORTGAGE"])],
            key=lambda d: d["master"].get("recorded_datetime") or "", reverse=True,
        )

        lien_docs = [d for d in docs if d["master"] and any(t in (d["master"].get("doc_type") or "").upper() for t in ["LIEN", "LIS PENDENS"])]

        buyer = next((p for p in (latest_deed["parties"] if latest_deed else []) if p.get("party_type") == "2"), None)
        owner = buyer.get("name", "Unknown Owner") if buyer else "Unknown Owner"
        purchase_amount = to_num(latest_deed["master"].get("document_amt")) if latest_deed and latest_deed["master"] else None
        purchase_price = f"${purchase_amount:,.0f}" if purchase_amount else "Unknown"
        purchase_date = to_date(latest_deed["master"].get("recorded_datetime")) if latest_deed and latest_deed["master"] else ""

        details: list[str] = []
        if purchase_date:
            details.append(f"Purchased: {purchase_date}")
        if mortgage_docs:
            latest_mtg = mortgage_docs[0]
            lender = next((p for p in latest_mtg["parties"] if p.get("party_type") == "2"), None)
            mtg_amount = to_num(latest_mtg["master"].get("document_amt")) if latest_mtg["master"] else None
            mtg_str = f"${mtg_amount:,.0f}" if mtg_amount else ""
            mtg_date = to_date(latest_mtg["master"].get("recorded_datetime")) if latest_mtg["master"] else ""
            lender_name = f" ({lender['name']})" if lender else ""
            details.append(f"Mortgage: {mtg_str}{lender_name} {mtg_date}")
        if lien_docs:
            details.append(f"LIEN ALERT: {len(lien_docs)} lien(s) on record")
            critical_findings.append(f"Unit at lot {lot} has {len(lien_docs)} lien document(s) filed")

        is_subject = lot == normalized_subject
        acris_unit = legals[0].get("unit") or legals[0].get("property_type") or ""
        if acris_unit:
            unit_label = f"{acris_unit} (Lot {lot}){' — Subject' if is_subject else ''}"
        else:
            unit_label = f"Lot {lot}{' (Subject)' if is_subject else ''}"

        latest_mtg = mortgage_docs[0] if mortgage_docs else None
        units.append({
            "unit": unit_label,
            "condoLot": lot,
            "owner": owner,
            "purchasePrice": purchase_price,
            "purchaseDate": purchase_date or "",
            "occupancy": f"Since {purchase_date}" if purchase_date else "",
            "isSubjectUnit": is_subject,
            "details": details,
            "mortgageCount": len(mortgage_docs),
            "lienCount": len(lien_docs),
            "latestMortgage": {
                "amount": to_num(latest_mtg["master"].get("document_amt")) if latest_mtg and latest_mtg["master"] else None,
                "lender": next((p.get("name") for p in (latest_mtg["parties"] if latest_mtg else []) if p.get("party_type") == "2"), None),
                "date": to_date(latest_mtg["master"].get("recorded_datetime")) if latest_mtg and latest_mtg["master"] else None,
            } if latest_mtg else None,
        })

    return {"units": units, "criticalFindings": critical_findings}
