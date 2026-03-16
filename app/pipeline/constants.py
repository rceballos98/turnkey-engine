"""Borough maps, BBL helpers, and shared formatters."""

from __future__ import annotations

import re

# Normalize free-text borough input to 2-letter code
BOROUGH_MAP: dict[str, str] = {
    "manhattan": "MN", "mn": "MN", "new york": "MN", "new york county": "MN",
    "brooklyn": "BK", "bk": "BK", "kings": "BK", "kings county": "BK",
    "bronx": "BX", "bx": "BX", "bronx county": "BX",
    "queens": "QN", "qn": "QN", "queens county": "QN",
    "staten island": "SI", "si": "SI", "richmond": "SI", "richmond county": "SI",
}

_BORO_ID = {"MN": "1", "BX": "2", "BK": "3", "QN": "4", "SI": "5"}
_BORO_NAME = {"MN": "Manhattan", "BX": "Bronx", "BK": "Brooklyn", "QN": "Queens", "SI": "Staten Island"}
_BORO_UPPER = {"MN": "MANHATTAN", "BX": "BRONX", "BK": "BROOKLYN", "QN": "QUEENS", "SI": "STATEN ISLAND"}


def borough_to_id(code: str) -> str:
    return _BORO_ID.get(code, "1")


def borough_to_name(code: str) -> str:
    return _BORO_NAME.get(code, code or "Manhattan")


def borough_to_full_name_upper(code: str) -> str:
    return _BORO_UPPER.get(code, "MANHATTAN")


def bbl_to_parid(boro_id: str, block: str, lot: str) -> str:
    return f"{boro_id}{block.zfill(5)}{lot.zfill(4)}"


def bbl_to_dashed(bbl: str) -> str:
    clean = re.sub(r"\D", "", bbl.split(".")[0])
    if len(clean) != 10:
        return bbl
    return f"{clean[0]}-{clean[1:6]}-{clean[6:10]}"


# ── Formatters ──

def fmt_p(n: int | float | None) -> str:
    if n is None or n == 0:
        return "N/A"
    return f"${n:,.0f}"


def fmt_num(n: int | float | None) -> str:
    if n is None:
        return "N/A"
    return f"{n:,.0f}"


def fmt_date(s: str | None) -> str | None:
    if not s:
        return None
    return s[:10]


def escape_html(s: str | None) -> str:
    if not s:
        return ""
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")


# ── Safe type coercion ──

def to_num(v) -> int | float | None:
    if v is None or v == "":
        return None
    try:
        n = float(v)
        return n if n == n and n != float("inf") and n != float("-inf") else None  # noqa: PLR0124
    except (ValueError, TypeError):
        return None


def safe_divide(a: int | float | None, b: int | float | None) -> int | None:
    if a is None or b is None or b == 0:
        return None
    return round(a / b)


def to_date(v) -> str | None:
    if not v or not isinstance(v, str):
        return None
    try:
        from datetime import datetime
        d = datetime.fromisoformat(v.replace("Z", "+00:00"))
        return d.strftime("%Y-%m-%d")
    except (ValueError, TypeError):
        if len(v) >= 10 and v[4] == "-" and v[7] == "-":
            return v[:10]
        return None
